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
    roc_auc_score
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Configuration

DB_HOST = os.getenv("UCANDO_DB_HOST", "localhost")
DB_PORT = os.getenv("UCANDO_DB_PORT", "5432")
DB_NAME = os.getenv("UCANDO_DB_NAME", "ucando")
DB_USER = os.getenv("UCANDO_DB_USER", "postgres")
DB_PASSWORD = os.getenv("UCANDO_DB_PASSWORD", "root")

FEATURE_TABLE = "dbt.customer_features"
LOAN_TABLE = "loans"
OUTPUT_TABLE = "customer_loan_propensity"

MODEL_NAME = "loan_propensity_logistic_regression"
MODEL_VERSION = "v1"

RANDOM_STATE = 42

# Database

def create_db_engine():
    connection_url = (
        f"postgresql+psycopg2://"
        f"{DB_USER}:{DB_PASSWORD}@"
        f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    return create_engine(connection_url)

# Load Data
def load_features(engine):
    query = f"""
        SELECT * 
        FROM {FEATURE_TABLE}
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        raise ValueError("customer_feature contains no records")
    
    print(
        f"Loaded {len(df)} customers from "
        f"{FEATURE_TABLE}."
    )

    return df

def load_loans(engine):
    query = f"""
            SELECT * 
            FROM {LOAN_TABLE}
            """

    df = pd.read_sql(query, engine)

    if df.empty:
        raise ValueError("Loan table contains no records")
    
    print(
        f"Loaded {len(df)} loan records from "
        f"{LOAN_TABLE}."
    )

    return df

# Create Target
def create_target(features, loans):
    if "customer_id" not in loans.columns:
        raise ValueError("Loans table does not contain customer_id.")

    # Any customer appearing in the loan table considered a positive example.
    loan_customers = (
        loans["customer_id"]
        .dropna()
        .astype(str)
        .unique()
    )

    result = features.copy()

    result["customer_id"] = (
        result["customer_id"]
        .astype(str)
    )

    result["loan_target"] = (
        result["customer_id"]
        .isin(loan_customers)
        .astype(int)
    )

    print("\nTarget Distribution: ")
    print(
        result["loan_target"]
        .value_counts()
        .sort_index()
    )

    return result

# Feature Selection

def prepare_training_features(df):
    feature_columns = [
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
        "behavior_event_count",
        "distinct_sessions",
        "distinct_devices",
        "distinct_pages",
        "distinct_products",
        "latest_credit_score",
        "previous_credit_score",
        "credit_score_change",
        "latest_enquiry_count",
        "credit_event_count",
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
        "marketing_open_rate",
        "marketing_click_rate",
        "marketing_conversion_rate",
        "lead_acceptance_rate",
    ]

    missing_columns = [
        column 
        for column in feature_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError("Missing feature columns: \n" + "\n".join(missing_columns))
    
    X = df[feature_columns].copy()

    X = X.apply(
        pd.to_numeric,
        errors = "coerce"
    )

    X = X.replace(
        [np.inf, -np.inf],
        np.nan
    )

    y = df["loan_target"]

    return X, y, feature_columns

# Build Model

def build_model():

    numeric_pipeline = Pipeline(
        steps = [
            (
                "imputer",
                SimpleImputer(strategy="median")
            ),
            (
                "scalar",
                StandardScaler()
            )
        ]
    )

    preprocessor = ColumnTransformer(
        transformers = [
            (
                "numeric",
                numeric_pipeline,
                slice(0, None)
            )
        ]
    )

    model = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        random_state=RANDOM_STATE
    )

    pipeline = Pipeline(
        steps=[
            (
                "preprocessor",
                preprocessor
            ),
            (
                "model",
                model
            )
        ]
    )

    return pipeline

# Train Model

def train_model(X, y):

    if y.nunique() < 2:
        raise ValueError(
            "Training data contains only one target class. "
            "Both positive and negative examples are required."
        )
    
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=RANDOM_STATE,
        stratify=y
    )

    pipeline = build_model()

    pipeline.fit(
        X_train,
        y_train
    )

    predictions = pipeline.predict(X_test)

    probabilities = pipeline.predict_proba(X_test)[:, 1]

    accuracy = accuracy_score(y_test,predictions)

    auc = roc_auc_score(y_test, probabilities)

    print("\nModel evalutation")
    print('-'*25)
    print(f"Accuracy : {accuracy:.4f}")
    print(f"ROC AUC  : {auc:.4f}")

    print("\nClassification report:")
    print(
        classification_report(
            y_test,
            predictions,
            zero_division=0
        )
    )

    return pipeline

# Generate propensity score
def generate_propensity(pipeline, df, X):

    probabilities = pipeline.predict_proba(X)[:, 1]

    predictions = (
        probabilities >= 0.50
    ).astype(int)

    result = pd.DataFrame()

    result["customer_id"] = (
        df["customer_id"]
        .astype(str)
    )

    result["propensity_score"] = probabilities

    result["prediction"] = predictions

    result["model_name"] = MODEL_NAME

    result["model_version"] = MODEL_VERSION

    result["prediction_timestamp"] = (
        datetime.now(timezone.utc)
    )

    return result

# Write results
def write_predictions(
    engine,
    predictions,
):

    predictions.to_sql(
        OUTPUT_TABLE,
        engine,
        if_exists="replace",
        index=False,
        method="multi",
    )

    print(
        f"\nWrote "
        f"{len(predictions)} predictions "
        f"to {OUTPUT_TABLE}."
    )

# Main
def main():

    print("=" * 60)
    print("UCanDo Loan Propensity Model")
    print("=" * 60)

    engine = create_db_engine()

    # Load

    features = load_features(
        engine
    )

    loans = load_loans(
        engine
    )

    # Target

    dataset = create_target(
        features,
        loans
    )

    # Training features

    X, y, feature_columns = (
        prepare_training_features(
            dataset
        )
    )

    print(
        f"\nUsing {len(feature_columns)} "
        f"features."
    )

    # Train

    pipeline = train_model(
        X,
        y
    )

    # Predictions

    predictions = generate_propensity(
        pipeline,
        dataset,
        X,
    )

    # Distribution

    print("\nPrediction distribution:")

    print(
        predictions["prediction"]
        .value_counts()
        .sort_index()
    )

    print("\nSample predictions:")

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
        "\nLoan propensity model completed."
    )


if __name__ == "__main__":
    main()