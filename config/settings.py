import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Project
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_PATH)


def get_env(name: str, default: str | None = None) -> str:
    """Ambil environment variable dan pastikan nilainya tersedia."""

    # 1. Ambil environment variable berdasarkan nama
    value = os.getenv(name, default)

    # 2. Pastikan environment variable memiliki nilai.
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def CITIBIKE_LOG(name: str) -> logging.Logger:
    """Standard logger untuk seluruh Citi Bike pipeline."""

    # 1. Konfigurasi format logging pipeline.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    return logging.getLogger(name)

# GCP Configuration
PROJECT_ID = get_env("PROJECT_ID")
LOCATION_ID = get_env("LOCATION_ID")
BUCKET_ID = get_env("BUCKET_ID")
GOOGLE_APPLICATION_CREDENTIALS = (PROJECT_ROOT / get_env("GOOGLE_APPLICATION_CREDENTIALS"))

# BiqQuery
FINPRO_DBT_DATASET = get_env("FINPRO_DBT_DATASET")

RAW_LAYER_BQ = f"{FINPRO_DBT_DATASET}_raw"
STAG_LAYER_BQ = f"{FINPRO_DBT_DATASET}_staging"
INTER_LAYER_BQ = f"{FINPRO_DBT_DATASET}_intermediate"
MART_LAYER_BQ = f"{FINPRO_DBT_DATASET}_mart"
AUDIT_LAYER_BQ = f"{FINPRO_DBT_DATASET}_audit"

BQ_RAW_TABLE = "raw_citibike_trips"
BQ_AUDIT_TABLE = get_env("BQ_AUDIT_TABLE")

# Airflow
AIRFLOW_TIMEZONE = get_env("AIRFLOW_TIMEZONE")
AIRFLOW_DAG_ID = get_env("AIRFLOW_DAG_ID")

# GBFS Streaming
GBFS_TOPIC_ID = os.getenv("GBFS_TOPIC_ID")
GBFS_SUBSCRIPTION_ID = os.getenv("GBFS_SUBSCRIPTION_ID")
BQ_GBFS_TABLE = os.getenv("BQ_GBFS_TABLE","station_status",)
BQ_GBFS_DLQ_TABLE = os.getenv("BQ_GBFS_DLQ_TABLE","station_status_dlq",)
GBFS_STATION_STATUS_URL = os.getenv("GBFS_STATION_STATUS_URL")

GBFS_POLL_INTERVAL_SECONDS = int(
os.getenv(
        "GBFS_POLL_INTERVAL_SECONDS",
        "60",
    )
)

GBFS_HTTP_TIMEOUT_SECONDS = int(
    os.getenv(
        "GBFS_HTTP_TIMEOUT_SECONDS",
        "20",
    )
)

# Dataflow Citibike
DATAFLOW_ID = get_env("DATAFLOW_ID")
DATAFLOW_MACHINE_TYPE = get_env("DATAFLOW_MACHINE_TYPE")
DATAFLOW_TEMP_PREFIX="dataflow/temp"
DATAFLOW_STAGING_PREFIX="dataflow/staging"
DATAFLOW_MAX_NUM_WORKERS=int(get_env("DATAFLOW_MAX_NUM_WORKERS", "2"))

# Citi Bike Batch
HTTP_TIMEOUT = 60
DATA_SOURCE = "citibike"
RAW_PREFIX = "raw/citibike"
DATA_LOCAL_PATH = PROJECT_ROOT / "data/raw"
CITIBIKE_BASE_URL = "https://s3.amazonaws.com/tripdata"

SOURCE_FILES = {
    "202601": "JC-202601-citibike-tripdata.zip",
    "202602": "JC-202602-citibike-tripdata.csv.zip",
    "202603": "JC-202603-citibike-tripdata.csv.zip",
    "202604": "JC-202604-citibike-tripdata.zip",
    "202605": "JC-202605-citibike-tripdata.csv.zip",
    "202606": "JC-202606-citibike-tripdata.csv.zip",
    "202607": "JC-202607-citibike-tripdata.csv.zip",
}