import os
import yaml
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

# ── CONFIG ────────────────────────────────────────────────────────────
DAG_ID = "chatbot_conversations_ingest"
DATASET_NAME = "chatbot-conversations"
CSV_PATH = f"/opt/airflow/extraction/{DATASET_NAME}/sample_data/3K Conversations Dataset for ChatBot.csv"
SCHEMA_PATH = f"/opt/airflow/extraction/{DATASET_NAME}/config/schema_expected.yaml"
TABLE_NAME = "public.chatbot_conversations"

# ── DEFAULT ARGS ──────────────────────────────────────────────────────
default_args = {
    "owner": "aadya-anil-kumar",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# ── HELPERS ───────────────────────────────────────────────────────────
def get_pg_engine():
    """Build SQLAlchemy engine from environment variables."""
    host = os.environ.get("EXT_PG_HOST", "postgres")
    port = os.environ.get("EXT_PG_PORT", "5432")
    db = os.environ.get("EXT_PG_DB", "airflow")
    user = os.environ.get("EXT_PG_USER", "airflow")
    password = os.environ.get("EXT_PG_PASSWORD", "airflow")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

# ── TASK 1: FILE CHECK ────────────────────────────────────────────────
def file_check():
    """Verify CSV file exists before proceeding."""
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"CSV file not found at: {CSV_PATH}. "
            "Please ensure the file is in extraction/chatbot-conversations/sample_data/"
        )
    print(f"✅ File found: {CSV_PATH}")

# ── TASK 2: VALIDATE SCHEMA ───────────────────────────────────────────
def validate_schema():
    """Compare CSV columns and types against schema_expected.yaml."""
    # Load schema contract
    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_columns = [col["name"] for col in schema["columns"]]

    # Load CSV
    df = pd.read_csv(CSV_PATH)

    # Rename unnamed index column to id if present
    if "Unnamed: 0" in df.columns:
        df = df.rename(columns={"Unnamed: 0": "id"})

    actual_columns = df.columns.tolist()

    # Check columns match
    missing = set(expected_columns) - set(actual_columns)
    extra = set(actual_columns) - set(expected_columns)

    if missing:
        raise ValueError(f"❌ Schema mismatch — missing columns: {missing}")
    if extra:
        raise ValueError(f"❌ Schema mismatch — unexpected columns: {extra}")

    # Check nullability
    for col in schema["columns"]:
        if not col["nullable"] and df[col["name"]].isnull().any():
            raise ValueError(
                f"❌ Column '{col['name']}' has nulls but is defined as NOT NULL"
            )

    print(f"✅ Schema validation passed. Columns: {actual_columns}")

# ── TASK 3: TRANSFORM ─────────────────────────────────────────────────
def transform():
    """Clean and normalize the CSV data."""
    df = pd.read_csv(CSV_PATH)

    # Rename unnamed index column to id
    if "Unnamed: 0" in df.columns:
        df = df.rename(columns={"Unnamed: 0": "id"})

    # Strip whitespace from string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Drop rows where question or answer is null
    before = len(df)
    df = df.dropna(subset=["question", "answer"])
    after = len(df)
    if before != after:
        print(f"⚠️ Dropped {before - after} rows with null values")

    # Save cleaned data to temp file
    cleaned_path = CSV_PATH.replace(".csv", "_cleaned.csv")
    df.to_csv(cleaned_path, index=False)
    print(f"✅ Transform complete. {after} rows saved to {cleaned_path}")

# ── TASK 4: LOAD TO POSTGRES ──────────────────────────────────────────
def load_to_postgres():
    """Load cleaned CSV into PostgreSQL table."""
    cleaned_path = CSV_PATH.replace(".csv", "_cleaned.csv")

    # Load cleaned data
    df = pd.read_csv(cleaned_path)

    # Connect to PostgreSQL
    engine = get_pg_engine()

    # Create table if not exists
    with engine.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS public.chatbot_conversations (
                id        INTEGER NOT NULL,
                question  TEXT    NOT NULL,
                answer    TEXT    NOT NULL,
                PRIMARY KEY (id)
            );
        """)
        conn.execute("COMMIT")

    # Load data — skip rows already in table (respect existing data)
    existing = pd.read_sql("SELECT id FROM public.chatbot_conversations", engine)
    existing_ids = set(existing["id"].tolist())

    new_rows = df[~df["id"].isin(existing_ids)]
    skipped = len(df) - len(new_rows)

    if skipped > 0:
        print(f"⚠️ Skipped {skipped} already existing rows")

    if len(new_rows) == 0:
        print("✅ No new rows to insert")
        return

    new_rows.to_sql(
        "chatbot_conversations",
        engine,
        schema="public",
        if_exists="append",
        index=False
    )
    print(f"✅ Loaded {len(new_rows)} new rows into {TABLE_NAME}")

# ── DAG DEFINITION ────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Ingest chatbot conversations CSV into PostgreSQL",
    schedule_interval="@daily",
    catchup=False,
    tags=["chatbot", "csv", "postgres", "intern"],
) as dag:

    t1 = PythonOperator(
        task_id="file_check",
        python_callable=file_check,
    )

    t2 = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    t3 = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t4 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    # Task dependencies
    t1 >> t2 >> t3 >> t4
