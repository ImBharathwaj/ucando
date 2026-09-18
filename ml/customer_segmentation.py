import os

import numpy as np
import pandas as pd

from sqlalchemy import create_engine

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Configuration
DB_HOST = os.getenv("UCANDO_DB_HOST", "localhost")
DB_PORT = os.getenv("UCANDO_DB_PORT", "5432")
DB_NAME = os.getenv("UCANDO_DB_NAME", "ucando")
DB_USER = os.getenv("UCANDO_DB_USER", "postgres")
DB_PASSWORD = os.getenv("UCANDO_DB_PASSWORD", "root")

N_CLUSTER = 5
RANDOM_STATE = 42

FEATURE_TABLE = "dbt.customer_features"
OUTPUT_TABLE = "customer_segments"

# Database

def create_db_engine():
    connection_url = (
        f"postgresql+psycopg2://"
        f"{DB_USER}:{DB_PASSWORD}@"
        f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    return create_engine(connection_url)

# Load Features

def load_customer_features(engine):
    query = f"""
        SELECT * 
        FROM {FEATURE_TABLE}
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        raise ValueError("customer_features contains no records")
    
    print(f"Loaded {len(df)} customers.")
    return df

# Feature Selection
def prepare_features(df):
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
        "loan_count",
        "active_loan_count",
        "total_loan_amount",
        "insurance_policy_count",
        "active_policy_count",
        "total_premium_amount",
        "latest_credit_score",
        "credit_score_change",
        "latest_enquiry_count",
        "credit_event_count",
        "marketing_event_count",
        "distinct_campaigns",
        "campaigns_sent",
        "campaigns_opened",
        "campaigns_clicked",
        "campaigns_converted",
        "leads_created",
        "leads_accepted",
        "marketing_open_rate",
        "marketing_click_rate",
        "marketing_conversion_rate",
        "lead_acceptance_rate"
    ]

    missing_columns = [
        column
        for column in feature_columns
        if column not in df.columns
    ]

    if missing_columns: raise ValueError(
        "Missing columns in customer_features: " + ", ".join(missing_columns)
    )

    features = df[feature_columns].copy()

    # Convert everything to numeric
    features = features.replace(
        [np.inf, -np.inf],
        np.nan
    )

    # Missing feature values become zero
    features = features.fillna(0)

    return features, feature_columns

# Scale Features

def scale_features(features):
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)

    return scaled, scaler

# Train Model

def train_model(scaled_features):

    model = KMeans(
        n_clusters=N_CLUSTER,
        random_state=RANDOM_STATE,
        n_init = 10
    )

    labels = model.fit_predict(
        scaled_features
    )

    return model, labels

# Build Segment Output

def build_segment_output(
    df, 
    labels,
    # model
    ):
    
    result = pd.DataFrame()
    result["customer_id"] = df["customer_id"]

    # Human-facing segment identifier.
    result["segment_id"] = labels

    result["segment_name"] = (
        "segment_" + result["segment_id"].astype(str)
    )

    result["model_name"] = "customer_kmeans"
    result["model_version"] = "v1"

    # result["cluster_distance"] = (
    #     model.transform(
    #         df["_scaled_features"].tolist()
    #     ).min(axis=1)
    # )

    return result


# Create Segment Profile

def create_segment_profile(df, labels, feature_columns):
    profile=df[feature_columns].copy()

    profile["segment_id"] = labels

    segment_profile = (
        profile
            .groupby("segment_id")
            .mean(numeric_only=True)
            .reset_index()
    )

    return segment_profile

# Write results

def write_results(engine, result):
    
    result.to_sql(
        OUTPUT_TABLE,
        engine,
        if_exists="replace",
        index=False
    )

    print(
        f"Wrote {len(result)} customer segment assignments "
        f"to {OUTPUT_TABLE}"
    )

# Main
def main():
    print("=" * 60)
    print("UCanDo Customer Segmentation")
    print("=" * 60)

    engine = create_db_engine()

    # Load
    df = load_customer_features(engine)

    # Prepare
    features, feature_columns = prepare_features(df)

    # Scale
    scaled_features, scaler = scale_features(features)

    # Train
    model, labels = train_model(scaled_features)

    # Attach scaled features temporarily so that we can
    # calculate distance from cluster centers.
    df["_scaled_features"] = list(scaled_features)

    # Output
    result = build_segment_output(df, labels)

    # Segment profile
    segment_profile = create_segment_profile(df, labels, feature_columns)

    print("\nSegment distribute: ")
    print(
        result["segment_id"]
        .value_counts()
        .sort_index()
    )

    print("\nSegment profile: ")
    print(segment_profile.to_string(index=False))

    # Persist
    write_results(
        engine,
        result
    )

    print("\nSegmentation completed")

if __name__ == "__main__":
    main()