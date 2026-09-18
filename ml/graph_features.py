import os
from datetime import datetime, timezone

import pandas as pd
from neo4j import GraphDatabase
from sqlalchemy import create_engine


NEO4J_URI = os.getenv("UCANDO_NEO4J_URI","bolt://localhost:7687")
NEO4J_USER = os.getenv("UCANDO_NEO4J_USER","neo4j")
NEO4J_PASSWORD = os.getenv("UCANDO_NEO4J_PASSWORD","strongroot")
NEO4J_DATABASE = os.getenv("UCANDO_NEO4J_DATABASE","neo4j")
POSTGRES_HOST = os.getenv("UCANDO_DB_HOST","localhost")
POSTGRES_PORT = os.getenv("UCANDO_DB_PORT","5432")
POSTGRES_DB = os.getenv("UCANDO_DB_NAME","ucando")
POSTGRES_USER = os.getenv("UCANDO_DB_USER","postgres")
POSTGRES_PASSWORD = os.getenv("UCANDO_DB_PASSWORD","root")
OUTPUT_SCHEMA = "public"
OUTPUT_TABLE = "customer_graph_features"

# Neo4j
def create_neo4j_driver():

    return GraphDatabase.driver(
        NEO4J_URI,
        auth=(
            NEO4J_USER,
            NEO4J_PASSWORD
        )
    )

# Postgres
def create_postgres_engine():
    connection_url = (
        f"postgresql+psycopg2://"
        f"{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/"
        f"{POSTGRES_DB}"
    )

    return create_engine(connection_url)

# Customer Graph Features

def extract_customer_features(driver):
    query = """
        MATCH (c:Customer)
        OPTIONAL MATCH (c)-[:OWNS]->(a:Account)
        OPTIONAL MATCH (c)-[:MADE]->(t:Transaction)
        OPTIONAL MATCH (c)-[:AT]->(m:Merchant)
        OPTIONAL MATCH (c)-[:USED]->(d:Device)
        OPTIONAL MATCH (c)-[:APPLIED_FOR]->(l:Loan)
        OPTIONAL MATCH (c)-[:HAS_POLICY]->(i:InsurancePolicy)
        OPTIONAL MATCH (c)-[:INTERACTED_WITH]->(camp:Campaign)
        
        WITH c,
            count(DISTINCT a) AS account_count,
            count(DISTINCT t) AS graph_transaction_count,
            count(DISTINCT m) AS graph_merchant_count,
            count(DISTINCT d) AS connected_device_count,
            count(DISTINCT l) AS graph_loan_count,
            count(DISTINCT i) AS graph_insurance_count,
            count(DISTINCT camp) AS connected_campaign_count
        RETURN
            c.customer_id as customer_id,
            account_count,
            graph_transaction_count,
            graph_merchant_count,
            connected_device_count,
            graph_loan_count,
            graph_insurance_count,
            connected_campaign_count
    """

    with driver.session(database=NEO4J_DATABASE) as session:
        result = session.run(query)
        records = [
            record.data()
            for record in result
        ]

        if not records:
            print("No Customer nodes found in Neo4j")

            return pd.DataFrame()
        
        df = pd.DataFrame(records)

        return df

# Shared Customer Connections
def extract_connected_customers(driver):

    query = """
    MATCH (c1:Customer)-[:USED]->(d:Device)<-[:USED]-(c2:Customer)
    WHERE c1.customer_id <> c2.customer_id
    RETURN
        c1.customer_id AS customer_id,
        count(DISTINCT c2) AS connected_customer_count
    """

    with driver.session(
        database=NEO4J_DATABASE
    ) as session:

        result = session.run(query)

        records = [
            record.data()
            for record in result
        ]

    if not records:

        return pd.DataFrame(
            columns=["customer_id","connected_customer_count"]
        )

    return pd.DataFrame(records)

# Build Final Feature Set
def build_features(
    graph_features,
    connected_customers
):

    if graph_features.empty:

        return pd.DataFrame()

    df = graph_features.merge(
        connected_customers,
        on="customer_id",
        how="left"
    )

    numeric_columns = [
        "account_count",
        "graph_transaction_count",
        "graph_merchant_count",
        "connected_device_count",
        "graph_loan_count",
        "graph_insurance_count",
        "connected_campaign_count",
        "connected_customer_count",
    ]

    for column in numeric_columns:
        if column not in df.columns:
            df[column] = 0
        df[column] = (
            pd.to_numeric(
                df[column],
                errors="coerce"
            )
            .fillna(0)
            .astype(int)
        )

    df["merchant_diversity_ratio"] = (
        df["graph_merchant_count"]
        /
        df["graph_transaction_count"]
        .replace(0, 1)
    )

    df["device_customer_ratio"] = (
        df["connected_customer_count"]
        /
        df["connected_device_count"]
        .replace(0, 1)
    )

    df["graph_relationship_count"] = (
        df["account_count"]
        + df["graph_transaction_count"]
        + df["graph_merchant_count"]
        + df["connected_device_count"]
        + df["graph_loan_count"]
        + df["graph_insurance_count"]
        + df["connected_campaign_count"]
        + df["connected_customer_count"]
    )

    df["graph_feature_timestamp"] = (
        datetime.now(timezone.utc)
    )

    return df

# Write to PostgreSQL
def write_features(engine, df):
    if df.empty:
        print("No graph features to write.")
        return
    print(
        f"\nWriting {len(df)} rows to "
        f"{OUTPUT_SCHEMA}.{OUTPUT_TABLE}..."
    )

    with engine.begin() as connection:
        df.to_sql(
            OUTPUT_TABLE,
            connection,
            schema=OUTPUT_SCHEMA,
            if_exists="replace",
            index=False,
            method="multi",
        )
        count = connection.exec_driver_sql(
            f"""
            SELECT COUNT(*)
            FROM "{OUTPUT_SCHEMA}"."{OUTPUT_TABLE}"
            """
        ).scalar()

        print(f"Rows visible inside transaction: {count}")

    print(
        f"PostgreSQL write committed: "
        f"{OUTPUT_SCHEMA}.{OUTPUT_TABLE}"
    )

    with engine.connect() as connection:
        count = connection.exec_driver_sql(
            f"""
            SELECT COUNT(*)
            FROM "{OUTPUT_SCHEMA}"."{OUTPUT_TABLE}"
            """
        ).scalar()

        print(f"Rows visible after commit: {count}")

# Main

def main():

    print("=" * 60)
    print("UCanDo Graph Feature Extraction")
    print("=" * 60)

    driver = create_neo4j_driver()

    postgres_engine = (create_postgres_engine())

    try:
        driver.verify_connectivity()
        print("Neo4j connectivity verified.")

        print("\nExtracting customer graph features...")
        graph_features = (extract_customer_features(driver))

        print(
            f"Found {len(graph_features)} "
            f"customers in graph."
        )
        print(
            "Extracting customer-to-customer "
            "connections..."
        )

        connected_customers = (extract_connected_customers(driver))

        features = build_features(
            graph_features,
            connected_customers
        )
        if not features.empty:
            print("\nGraph feature sample:")
            print(features.head(10).to_string(index=False))

        write_features(
            postgres_engine,
            features
        )

        print("\nGraph feature extraction completed.")

    finally:
        driver.close()
        postgres_engine.dispose()

if __name__ == "__main__":
    main()