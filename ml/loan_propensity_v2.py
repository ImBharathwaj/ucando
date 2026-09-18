import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from sqlalchemy import create_engine

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# CONFIGURATION

DB_HOST = os.getenv("UCANDO_DB_HOST", "localhost")
DB_PORT = os.getenv("UCANDO_DB_PORT", "5432")
DB_NAME = os.getenv("UCANDO_DB_NAME", "ucando")
DB_USER = os.getenv("UCANDO_DB_USER", "postgres")
DB_PASSWORD = os.getenv("UCANDO_DB_PASSWORD", "root")

FEATURE_TABLE = "customer_features"
GRAPH_FEATURE_TABLE = "customer_graph_features"
LOAN_TABLE = "loans"

OUTPUT_TABLE = "customer_loan_propensity_v2"

MODEL_NAME = "loan_propensity_logistic_regression_graph"
MODEL_VERSION = "v2"

RANDOM_STATE = 42

# DATABASE

def create_db_engine():

    connection_url = (
        f"postgresql+psycopg2://"
        f"{DB_USER}:{DB_PASSWORD}@"
        f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    return create_engine(connection_url)

# LOAD DATA

def load_table(engine, table_name):

    query = f"""
        SELECT *
        FROM dbt.{table_name}
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        raise ValueError(
            f"dbt.{table_name} contains no records."
        )

    print(
        f"Loaded {len(df)} records from "
        f"dbt.{table_name}."
    )

    return df


def load_loans(engine):

    query = """
        SELECT *
        FROM loans
    """

    df = pd.read_sql(query, engine)

    print(
        f"Loaded {len(df)} loan records from loans."
    )

    return df

# BUILD DATASET

def build_dataset(
    customer_features,
    graph_features,
    loans,
):

    # Create target

    loan_customers = set(
        loans["customer_id"]
        .dropna()
        .astype(str)
    )

    df = customer_features.copy()

    df["customer_id"] = (
        df["customer_id"]
        .astype(str)
    )

    df["loan_target"] = (
        df["customer_id"]
        .isin(loan_customers)
        .astype(int)
    )

    # Join graph features

    graph_features = graph_features.copy()

    graph_features["customer_id"] = (
        graph_features["customer_id"]
        .astype(str)
    )

    graph_columns = [
        "account_count",
        "graph_transaction_count",
        "connected_device_count",
        "graph_insurance_count",
        "connected_campaign_count",
        "graph_relationship_count",
    ]

    graph_columns = [
        column
        for column in graph_columns
        if column in graph_features.columns
    ]

    graph_subset = graph_features[
        ["customer_id"] + graph_columns
    ]

    df = df.merge(
        graph_subset,
        on="customer_id",
        how="left",
    )

    # Customers without graph data receive zero.
    for column in graph_columns:

        df[column] = (
            pd.to_numeric(
                df[column],
                errors="coerce",
            )
            .fillna(0)
        )

    print(
        f"\nAdded {len(graph_columns)} "
        f"graph features."
    )

    print(
        "Graph features:",
        ", ".join(graph_columns)
    )

    return df



# FEATURE SELECTION


def prepare_features(df):

    feature_columns = [

        # ----------------------------------------------------
        # Transaction behavior
        # ----------------------------------------------------

        "total_transactions",
        "total_transaction_amount",
        "avg_transaction_amount",

        "transaction_count_30d",
        "transaction_amount_30d",

        "transaction_count_90d",
        "transaction_amount_90d",

        "distinct_merchants",
        "distinct_transaction_categories",
        "distinct_transaction_channels",

        # ----------------------------------------------------
        # Digital behavior
        # ----------------------------------------------------

        "behavior_event_count",
        "distinct_sessions",
        "distinct_devices",
        "distinct_pages",
        "distinct_products",

        # ----------------------------------------------------
        # Credit
        # ----------------------------------------------------

        "latest_credit_score",
        "previous_credit_score",
        "credit_score_change",

        "latest_enquiry_count",
        "credit_event_count",

        # ----------------------------------------------------
        # Marketing
        # ----------------------------------------------------

        "marketing_event_count",
        "distinct_campaigns",
        "distinct_marketing_channels",

        "campaigns_sent",
        "campaigns_delivered",
        "campaigns_opened",
        "campaigns_clicked",
        "campaigns_converted",

        "campaigns_bounced",
        "campaigns_unsubscribed",

        "leads_created",
        "leads_accepted",
        "leads_rejected",

        # ----------------------------------------------------
        # Marketing response
        # ----------------------------------------------------

        "marketing_open_rate",
        "marketing_click_rate",
        "marketing_conversion_rate",
        "lead_acceptance_rate",

        # ----------------------------------------------------
        # Graph features
        # ----------------------------------------------------

        "account_count",
        "graph_transaction_count",
        "connected_device_count",
        "graph_insurance_count",
        "connected_campaign_count",
        "graph_relationship_count",
    ]

    missing_columns = [
        column
        for column in feature_columns
        if column not in df.columns
    ]

    if missing_columns:

        raise ValueError(
            "Missing feature columns:\n"
            + "\n".join(missing_columns)
        )

    X = df[feature_columns].copy()

    X = X.apply(
        pd.to_numeric,
        errors="coerce",
    )

    X = X.replace(
        [np.inf, -np.inf],
        np.nan,
    )

    y = df["loan_target"]

    return X, y, feature_columns



# MODEL


def build_model():

    numeric_pipeline = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(
                    strategy="median"
                ),
            ),
            (
                "scaler",
                StandardScaler(),
            ),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                numeric_pipeline,
                slice(0, None),
            )
        ]
    )

    classifier = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        random_state=RANDOM_STATE,
    )

    pipeline = Pipeline(
        steps=[
            (
                "preprocessor",
                preprocessor,
            ),
            (
                "model",
                classifier,
            ),
        ]
    )

    return pipeline



# TRAIN


def train_model(X, y):

    print("\nTarget distribution:")
    print(
        y.value_counts()
        .sort_index()
    )

    X_train, X_test, y_train, y_test = (
        train_test_split(
            X,
            y,
            test_size=0.20,
            random_state=RANDOM_STATE,
            stratify=y,
        )
    )

    model = build_model()

    model.fit(
        X_train,
        y_train,
    )

    predictions = model.predict(
        X_test
    )

    probabilities = model.predict_proba(
        X_test
    )[:, 1]

    accuracy = accuracy_score(
        y_test,
        predictions,
    )

    auc = roc_auc_score(
        y_test,
        probabilities,
    )

    print("\nModel evaluation")
    print("----------------")
    print(
        f"Accuracy : {accuracy:.4f}"
    )
    print(
        f"ROC AUC  : {auc:.4f}"
    )

    print("\nClassification report:")

    print(
        classification_report(
            y_test,
            predictions,
            zero_division=0,
        )
    )

    return model



# PREDICTIONS


def generate_predictions(
    model,
    X,
    df,
):

    probabilities = model.predict_proba(
        X
    )[:, 1]

    predictions = (
        probabilities >= 0.50
    ).astype(int)

    result = pd.DataFrame()

    result["customer_id"] = (
        df["customer_id"]
        .astype(str)
    )

    result["propensity_score"] = (
        probabilities
    )

    result["prediction"] = (
        predictions
    )

    result["model_name"] = (
        MODEL_NAME
    )

    result["model_version"] = (
        MODEL_VERSION
    )

    result["prediction_timestamp"] = (
        datetime.now(timezone.utc)
    )

    return result



# WRITE RESULTS


def write_predictions(
    engine,
    predictions,
):

    predictions.to_sql(
        OUTPUT_TABLE,
        engine,
        schema="public",
        if_exists="replace",
        index=False,
        method="multi",
    )

    print(
        f"\nWrote {len(predictions)} predictions "
        f"to public.{OUTPUT_TABLE}."
    )



# MAIN


def main():

    print("=" * 60)
    print("UCanDo Loan Propensity Model V2")
    print("Graph-Enriched")
    print("=" * 60)

    engine = create_db_engine()

    
    # Load
    

    customer_features = load_table(
        engine,
        FEATURE_TABLE,
    )

    graph_features = load_table(
        engine,
        GRAPH_FEATURE_TABLE,
    )

    loans = load_loans(
        engine,
    )

    
    # Build dataset
    

    dataset = build_dataset(
        customer_features,
        graph_features,
        loans,
    )

    
    # Prepare features
    

    X, y, feature_columns = (
        prepare_features(
            dataset
        )
    )

    print(
        f"\nUsing {len(feature_columns)} features."
    )

    
    # Train
    

    model = train_model(
        X,
        y,
    )

    
    # Generate predictions
    

    predictions = generate_predictions(
        model,
        X,
        dataset,
    )

    print(
        "\nPrediction distribution:"
    )

    print(
        predictions["prediction"]
        .value_counts()
        .sort_index()
    )

    print(
        "\nTop propensity customers:"
    )

    print(
        predictions
        .sort_values(
            "propensity_score",
            ascending=False,
        )
        .head(10)
        .to_string(index=False)
    )

    # Persist

    write_predictions(
        engine,
        predictions,
    )

    print(
        "\nLoan propensity V2 completed."
    )


if __name__ == "__main__":
    main()