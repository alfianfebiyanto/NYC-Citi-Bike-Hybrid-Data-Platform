import json
import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone
from config.settings import CITIBIKE_LOG
from google.cloud import bigquery, storage

load_dotenv()

logger = CITIBIKE_LOG("CitibikeStationInfo")

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION_ID = os.getenv("LOCATION_ID")
BUCKET_ID = os.getenv("BUCKET_ID")
RAW_DATASET = os.getenv("FINPRO_DBT_RAW")

STATION_INFORMATION_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
GCS_PREFIX = "raw/station_information"
TABLE_NAME = "station_information"


def extract_station_information():
    """Ambil snapshot station information terbaru dari GBFS API."""

    # 1. Ambil snapshot station terbaru dari GBFS API.
    response = requests.get(STATION_INFORMATION_URL, timeout=20)
    response.raise_for_status()
    stations = response.json()["data"]["stations"]

    # 2. Tambahkan waktu ingestion untuk setiap record station.
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "station_id": station.get("station_id"),
            "name": station.get("name"),
            "short_name": station.get("short_name"),
            "lat": station.get("lat"),
            "lon": station.get("lon"),
            "region_id": station.get("region_id"),
            "capacity": station.get("capacity"),
            "station_type": station.get("station_type"),
            "rental_methods": station.get("rental_methods", []),
            "_ingested_at": ingested_at,
        }
        for station in stations
    ]

    # 4. Pastikan payload memiliki data station untuk diproses.
    if not rows:
        raise ValueError("Station information payload is empty")
    
    logger.info("Station information extracted | rows=%s", len(rows))
    return rows


def upload_to_gcs(rows):
    """Upload snapshot station information ke GCS dalam format NDJSON."""

    # 1. Inisialisasi GCS client dan bucket tujuan.
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_ID)

    # 2. Tentukan partition berdasarkan tanggal ingestion.
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    blob_name = f"{GCS_PREFIX}/dt={date}/station_information.ndjson"
    ndjson = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)

    # 3. Upload snapshot station information ke GCS.
    bucket.blob(blob_name).upload_from_string(ndjson, content_type="application/x-ndjson")
    gcs_uri = f"gs://{BUCKET_ID}/{blob_name}"

    logger.info("GCS upload completed | %s", gcs_uri)
    return gcs_uri


def load_to_bigquery(gcs_uri):
    """Load snapshot station information dari GCS ke BigQuery RAW."""

    # 1. Inisialisasi BigQuery client dan tabel tujuan.
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION_ID)
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{TABLE_NAME}"

    # 2. Definisikan schema RAW station information.
    schema = [
        bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("short_name", "STRING"),
        bigquery.SchemaField("lat", "FLOAT"),
        bigquery.SchemaField("lon", "FLOAT"),
        bigquery.SchemaField("region_id", "STRING"),
        bigquery.SchemaField("capacity", "INTEGER"),
        bigquery.SchemaField("station_type", "STRING"),
        bigquery.SchemaField("rental_methods", "STRING", mode="REPEATED"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    # 3. Gunakan WRITE_TRUNCATE agar RAW selalu menyimpan snapshot terbaru
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # 4. Load snapshot dari GCS ke BigQuery RAW.
    client.load_table_from_uri(gcs_uri, table_id, job_config=job_config).result()
    rows = client.get_table(table_id).num_rows
    logger.info("BigQuery load completed | table=%s | rows=%s", table_id, rows)

def main():
    """Jalankan ingestion station information dari GBFS hingga BigQuery RAW."""

    logger.info("Station information ingestion started")

    # Alur ingestion: GBFS API -> GCS -> BigQuery RAW
    rows = extract_station_information()
    gcs_uri = upload_to_gcs(rows)
    load_to_bigquery(gcs_uri)

    logger.info("Station information ingestion completed")

if __name__ == "__main__":
    main()