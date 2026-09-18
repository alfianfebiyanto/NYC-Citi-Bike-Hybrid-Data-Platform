from __future__ import annotations
import json
import uuid
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
    WorkerOptions,
)

from config.settings import (
    PROJECT_ID, LOCATION_ID, RAW_LAYER_BQ,
    GBFS_SUBSCRIPTION_ID, BQ_GBFS_TABLE, BQ_GBFS_DLQ_TABLE,
    BUCKET_ID, DATAFLOW_TEMP_PREFIX, DATAFLOW_STAGING_PREFIX,
    DATAFLOW_MAX_NUM_WORKERS,
)

# BigQuery Schema
GBFS_BQ_SCHEMA = {
    "fields": [
        {"name": "station_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "num_bikes_available", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "num_ebikes_available", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "num_docks_available", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "num_bikes_disabled", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "num_docks_disabled", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "is_installed", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "is_renting", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "is_returning", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "last_reported", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "snapshot_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

GBFS_DLQ_SCHEMA = {
    "fields": [
        {"name": "error_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "raw_payload", "type": "STRING", "mode": "REQUIRED"},
        {"name": "error_reason", "type": "STRING", "mode": "REQUIRED"},
        {"name": "source", "type": "STRING", "mode": "REQUIRED"},
        {"name": "detected_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# GBFS Validation
class ParseAndValidateGBFS(beam.DoFn):
    """Parse dan validasi event GBFS sebelum ditulis ke BigQuery."""

    DLQ_TAG = "dlq"

    INTEGER_FIELDS = [
        "num_bikes_available",
        "num_ebikes_available",
        "num_docks_available",
        "num_bikes_disabled",
        "num_docks_disabled",
    ]

    BOOLEAN_FIELDS = ["is_installed", "is_renting", "is_returning"]

    def process(self, element):
        """Proses satu event GBFS dan pisahkan valid atau DLQ."""

        raw_payload = None

        try:
            # 1. Decode Pub/Sub message dan parse JSON payload.
            raw_payload = element.decode("utf-8")
            data = json.loads(raw_payload)

            # 2. Pastikan payload menggunakan JSON object.
            if not isinstance(data, dict):
                raise ValueError("Payload harus berupa JSON object.")

            # 3. Validasi station_id sebagai identitas station.
            station_id = data.get("station_id")
            if not isinstance(station_id, str) or not station_id.strip():
                raise ValueError("station_id tidak valid.")

            # 4. Validasi seluruh field integer dan nilai availability.
            integers = {}
            for field in self.INTEGER_FIELDS:
                value = data.get(field)

                if value is not None:
                    if isinstance(value, bool) or not isinstance(value, int):
                        raise ValueError(f"{field} harus integer.")
                    if value < 0:
                        raise ValueError(f"{field} tidak boleh negatif.")

                integers[field] = value

            # 5. Validasi dan normalisasi seluruh field boolean.
            booleans = {}
            for field in self.BOOLEAN_FIELDS:
                value = data.get(field)

                if value is not None and value not in (0, 1, False, True):
                    raise ValueError(f"{field} harus 0 atau 1.")

                booleans[field] = None if value is None else bool(value)

            snapshot_raw = data.get("snapshot_timestamp")
            if not snapshot_raw:
                raise ValueError("snapshot_timestamp tidak tersedia.")

            try:
                snapshot_dt = datetime.fromisoformat(
                    str(snapshot_raw).replace("Z", "+00:00")
                )
            except ValueError:
                raise ValueError("snapshot_timestamp tidak valid.")

            if snapshot_dt.tzinfo is None:
                snapshot_dt = snapshot_dt.replace(tzinfo=timezone.utc)

            last_reported = None
            last_raw = data.get("last_reported")

            if isinstance(last_raw, int) and not isinstance(last_raw, bool) and last_raw > 1_000_000_000:
                try:
                    last_reported = datetime.fromtimestamp(
                        last_raw, tz=timezone.utc
                    ).isoformat()
                except (ValueError, OSError, OverflowError):
                    pass

            yield {
                "station_id": station_id,
                **integers,
                **booleans,
                "last_reported": last_reported,
                "snapshot_timestamp": snapshot_dt.astimezone(timezone.utc).isoformat(),
                "_ingested_at": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as exc:
            yield pvalue.TaggedOutput(
                self.DLQ_TAG,
                {
                    "error_id": str(uuid.uuid4()),
                    "raw_payload": raw_payload or repr(element),
                    "error_reason": str(exc),
                    "source": "gbfs_station_status",
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                },
            )

# Dataflow Configuration
def build_options():
    """Siapkan konfigurasi Dataflow streaming pipeline."""

    # 1. Inisialisasi pipeline options.
    options = PipelineOptions()

    # 2. Konfigurasi Dataflow sebagai streaming runner.
    standard = options.view_as(StandardOptions)
    standard.runner = "DataflowRunner"
    standard.streaming = True

    # 3. Konfigurasi project, region, dan Dataflow job.
    cloud = options.view_as(GoogleCloudOptions)
    cloud.project = PROJECT_ID
    cloud.region = LOCATION_ID
    job_suffix = uuid.uuid4().hex[:6]
    cloud.job_name = f"finpro-alfian-gbfs-{job_suffix}"

    cloud.temp_location = f"gs://{BUCKET_ID}/{DATAFLOW_TEMP_PREFIX}"
    cloud.staging_location = f"gs://{BUCKET_ID}/{DATAFLOW_STAGING_PREFIX}"

    options.view_as(WorkerOptions).max_num_workers = int(DATAFLOW_MAX_NUM_WORKERS)
    options.view_as(SetupOptions).save_main_session = True

    return options

# Dataflow Pipeline
def run():
    """Jalankan GBFS streaming pipeline dari Pub/Sub ke BigQuery."""

    # 1. Tentukan Pub/Sub subscription sebagai streaming source.
    subscription = f"projects/{PROJECT_ID}/subscriptions/{GBFS_SUBSCRIPTION_ID}"
    valid_table = f"{PROJECT_ID}:{RAW_LAYER_BQ}.{BQ_GBFS_TABLE}"
    dlq_table = f"{PROJECT_ID}:{RAW_LAYER_BQ}.{BQ_GBFS_DLQ_TABLE}"

    print("=" * 55)
    print("GBFS DATAFLOW PIPELINE")
    print(f"Subscription : {subscription}")
    print(f"RAW Table    : {valid_table}")
    print(f"DLQ Table    : {dlq_table}")
    print("=" * 55)

    # 2. Baca event dari Pub/Sub dan validasi setiap event GBFS.
    with beam.Pipeline(options=build_options()) as pipeline:
        results = (
            pipeline
            | "Read PubSub" >> beam.io.ReadFromPubSub(subscription=subscription)
            | "Validate GBFS" >> beam.ParDo(ParseAndValidateGBFS()).with_outputs(
                ParseAndValidateGBFS.DLQ_TAG,
                main="valid",
            )
        )

        # 3. Tulis event valid ke BigQuery RAW.
        results.valid | "Write RAW" >> beam.io.WriteToBigQuery(
            valid_table,
            schema=GBFS_BQ_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        # 4. Tulis event invalid ke BigQuery DLQ.
        results.dlq | "Write DLQ" >> beam.io.WriteToBigQuery(
            dlq_table,
            schema=GBFS_DLQ_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

if __name__ == "__main__":
    run()