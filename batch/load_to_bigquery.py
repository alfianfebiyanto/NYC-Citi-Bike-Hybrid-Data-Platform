from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from config.settings import (
    BQ_RAW_TABLE,
    BUCKET_ID,
    CITIBIKE_LOG,
    LOCATION_ID,
    PROJECT_ID,
    RAW_LAYER_BQ,
    RAW_PREFIX,
)

logger = CITIBIKE_LOG("CitibikeBQLoad")

def ensure_dataset_exists(client: bigquery.Client) -> bigquery.Dataset:
    """Gunakan RAW dataset yang tersedia atau buat jika belum ada."""

    dataset_id = f"{PROJECT_ID}.{RAW_LAYER_BQ}"

    # 1. Checking Dataset tersedia
    try:
        return client.get_dataset(dataset_id)

    except NotFound:
        logger.info(f"Dataset not found. Creating: {dataset_id}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = LOCATION_ID
        dataset = client.create_dataset(dataset)
        logger.info(f"Dataset created: {dataset_id}")

        return dataset


def load_to_bigquery() -> None:
    """Load seluruh GCS daily partitions ke BigQuery RAW."""

    client = bigquery.Client(project=PROJECT_ID)

    # 1. Inisialisasi BigQuery client.
    ensure_dataset_exists(client)

    table_id = f"{PROJECT_ID}.{RAW_LAYER_BQ}.{BQ_RAW_TABLE}"
    source_prefix = f"gs://{BUCKET_ID}/{RAW_PREFIX}/"
    source_uri = f"{source_prefix}*"

    # 2. Konfigurasi Hive partition untuk membaca year/month/day dari GCS path.
    hive_options = bigquery.HivePartitioningOptions()
    hive_options.mode = "AUTO"
    hive_options.source_uri_prefix = source_prefix

    # 3. Konfigurasi load RAW mengikuti schema source Citi Bike.
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        hive_partitioning=hive_options,
    )

    logger.info("BigQuery load started")
    logger.info(f"Source: {source_uri}")
    logger.info(f"Destination: {table_id}")
    logger.info("-------------------------")

    # 4. Load seluruh GCS partitions
    load_job = client.load_table_from_uri(
        source_uri,
        table_id,
        job_config=job_config,
        location=LOCATION_ID,
    )

    load_job.result()

    # 6. Validasi jumlah row setelah proses load selesai.
    table = client.get_table(table_id)

    logger.info(f"Rows loaded: {table.num_rows:,}")
    logger.info("BigQuery load completed")

if __name__ == "__main__":
    load_to_bigquery()