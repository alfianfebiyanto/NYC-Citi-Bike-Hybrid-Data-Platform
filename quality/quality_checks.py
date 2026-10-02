import uuid
from datetime import datetime, timezone

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from config.settings import (
    PROJECT_ID, LOCATION_ID, CITIBIKE_LOG,
    AUDIT_LAYER_BQ, BQ_AUDIT_TABLE,
    STAG_LAYER_BQ, INTER_LAYER_BQ,
    MART_LAYER_BQ, RAW_LAYER_BQ,
)

from config.alerts import send_slack


logger = CITIBIKE_LOG("CitibikeQualityCheck")
client = bigquery.Client(project=PROJECT_ID)


def run_query(query):
    """Jalankan query BigQuery dan ambil satu row hasil."""
    return next(client.query(query).result())


def ensure_audit():
    """Pastikan dataset dan table audit tersedia."""

    dataset_id = f"{PROJECT_ID}.{AUDIT_LAYER_BQ}"
    table_id = f"{dataset_id}.{BQ_AUDIT_TABLE}"

    # 1. Checking Dataset
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = LOCATION_ID
        client.create_dataset(dataset)
        logger.info(f"Audit dataset created: {dataset_id}")

    # 2. Checking Audit Table
    try:
        client.get_table(table_id)
    except NotFound:
        schema = [
            bigquery.SchemaField("audit_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("dag_id", "STRING"),
            bigquery.SchemaField("layer", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("check_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("actual_value", "STRING"),
            bigquery.SchemaField("expected_value", "STRING"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("message", "STRING"),
            bigquery.SchemaField("checked_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        client.create_table(bigquery.Table(table_id, schema=schema))
        logger.info(f"Audit table created: {table_id}")

    return table_id


def audit_check(layer, table_name, check_name, actual, expected, passed,
                run_id=None, dag_id=None):
    """Simpan hasil PASS/FAIL ke audit table."""

    status = "PASS" if passed else "FAIL"

    row = {
        "audit_id": str(uuid.uuid4()),
        "run_id": run_id,
        "dag_id": dag_id,
        "layer": layer,
        "table_name": table_name,
        "check_name": check_name,
        "actual_value": str(actual),
        "expected_value": str(expected),
        "status": status,
        "message": f"{check_name}: {actual}",
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }

    errors = client.insert_rows_json(ensure_audit(), [row])

    if errors:
        raise RuntimeError(f"Failed to write audit: {errors}")

    logger.info(f"{status} | {layer} | {table_name} | {check_name} | {actual}")
    return passed


def finish_gate(layer, results):
    """Fail pipeline jika ada quality check yang gagal."""

    if not all(results):
        raise RuntimeError(f"{layer} quality gate FAILED")

    logger.info(f"{layer} quality gate PASSED")

# +++ BATCH +++

# Staging Quality
def check_staging(run_id=None, dag_id=None):
    """Validasi kualitas teknis batch pada STAGING layer."""

    # 1. Tentukan table yang akan divalidasi
    trips_id = f"{PROJECT_ID}.{STAG_LAYER_BQ}.stg_citibike_trips"
    info_id = f"{PROJECT_ID}.{STAG_LAYER_BQ}.stg_station_information"

    # 2. Validasi row count dan ride_id pada staging trips
    trips = run_query(f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(ride_id IS NULL) AS null_id,
            COUNT(*) - COUNT(DISTINCT ride_id) AS duplicate_id
        FROM `{trips_id}`
    """)

    # 3. Validasi identitas station information
    info = run_query(f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(station_id IS NULL OR short_name IS NULL) AS invalid_id,
            COUNT(*) - COUNT(DISTINCT station_id) AS duplicate_id,
            COUNT(*) - COUNT(DISTINCT short_name) AS duplicate_short_name
        FROM `{info_id}`
    """)

    # 4. Definisikan quality check STAGING
    checks = [
        ("stg_citibike_trips", "row_count", trips.total, "> 0", trips.total > 0),
        ("stg_citibike_trips", "ride_id_not_null", trips.null_id, 0, trips.null_id == 0),
        ("stg_citibike_trips", "ride_id_unique", trips.duplicate_id, 0, trips.duplicate_id == 0),

        ("stg_station_information", "row_count", info.total, "> 0", info.total > 0),
        ("stg_station_information", "station_id_valid", info.invalid_id, 0, info.invalid_id == 0),
        ("stg_station_information", "station_id_unique", info.duplicate_id, 0, info.duplicate_id == 0),
        ("stg_station_information", "short_name_unique", info.duplicate_short_name, 0, info.duplicate_short_name == 0),
    ]

    # 5. Simpan hasil quality check
    results = [
        audit_check("STAGING", table, name, actual, expected, passed, run_id, dag_id)
        for table, name, actual, expected, passed in checks
    ]

    finish_gate("STAGING", results)

# Intermediate QualityS
def check_intermediate(run_id=None, dag_id=None):
    """Validasi business rules dan reconciliation batch pada INTERMEDIATE layer."""

    # 1. Tentukan table yang dibutuhkan
    staging_id = f"{PROJECT_ID}.{STAG_LAYER_BQ}.stg_citibike_trips"
    clean_id = f"{PROJECT_ID}.{INTER_LAYER_BQ}.int_citibike_trips_clean"
    invalid_id = f"{PROJECT_ID}.{INTER_LAYER_BQ}.int_citibike_trips_invalid"
    info_id = f"{PROJECT_ID}.{INTER_LAYER_BQ}.int_station_information_clean"

    # 2. Validasi clean trips dan reconciliation dengan staging
    trips = run_query(f"""
        SELECT
            (SELECT COUNT(*) FROM `{staging_id}`) AS staging_count,
            (SELECT COUNT(*) FROM `{clean_id}`) AS clean_count,
            (SELECT COUNT(*) FROM `{invalid_id}`) AS invalid_count,
            (SELECT COUNTIF(ride_id IS NULL) FROM `{clean_id}`) AS null_id,
            (SELECT COUNT(*) - COUNT(DISTINCT ride_id) FROM `{clean_id}`) AS duplicate_id,
            (
                SELECT COUNT(*) FROM `{clean_id}`
                WHERE ended_at <= started_at
                    OR member_casual NOT IN ('member', 'casual')
                    OR rideable_type NOT IN ('classic_bike', 'electric_bike')
                    OR start_station_id IS NULL OR end_station_id IS NULL
                    OR start_lat NOT BETWEEN -90 AND 90 OR end_lat NOT BETWEEN -90 AND 90
                    OR start_lng NOT BETWEEN -180 AND 180 OR end_lng NOT BETWEEN -180 AND 180
            ) AS invalid_rules
    """)

    # 3. Validasi station information
    info = run_query(f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(station_id IS NULL OR short_name IS NULL OR station_name IS NULL) AS invalid_id,
            COUNT(*) - COUNT(DISTINCT station_id) AS duplicate_id,
            COUNT(*) - COUNT(DISTINCT short_name) AS duplicate_short_name,
            COUNTIF(
                latitude NOT BETWEEN -90 AND 90
                OR longitude NOT BETWEEN -180 AND 180
                OR capacity < 0
            ) AS invalid_rules
        FROM `{info_id}`
    """)

    # 4. Definisikan quality check INTERMEDIATE
    reconciled = trips.staging_count == trips.clean_count + trips.invalid_count

    checks = [
        ("int_citibike_trips_clean", "ride_id_not_null", trips.null_id, 0, trips.null_id == 0),
        ("int_citibike_trips_clean", "ride_id_unique", trips.duplicate_id, 0, trips.duplicate_id == 0),
        ("int_citibike_trips_clean", "business_rules", trips.invalid_rules, 0, trips.invalid_rules == 0),
        (
            "int_citibike_trips_clean", "clean_invalid_reconciliation",
            f"{trips.staging_count}={trips.clean_count}+{trips.invalid_count}",
            "staging=clean+invalid", reconciled,
        ),

        ("int_station_information_clean", "row_count", info.total, "> 0", info.total > 0),
        ("int_station_information_clean", "station_identity_valid", info.invalid_id, 0, info.invalid_id == 0),
        ("int_station_information_clean", "station_id_unique", info.duplicate_id, 0, info.duplicate_id == 0),
        ("int_station_information_clean", "short_name_unique", info.duplicate_short_name, 0, info.duplicate_short_name == 0),
        ("int_station_information_clean", "business_rules", info.invalid_rules, 0, info.invalid_rules == 0),
    ]

    # 5. Simpan hasil quality check
    results = [
        audit_check("INTERMEDIATE", table, name, actual, expected, passed, run_id, dag_id)
        for table, name, actual, expected, passed in checks
    ]

    finish_gate("INTERMEDIATE", results)

# Warehouse Quality
def check_dimension(table_name, key_column, run_id=None, dag_id=None):
    """Validasi primary key dimension table."""

    table_id = f"{PROJECT_ID}.{MART_LAYER_BQ}.{table_name}"

    # 1. Periksa null dan duplicate primary key
    row = run_query(f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF({key_column} IS NULL) AS null_key,
            COUNT(*) - COUNT(DISTINCT {key_column}) AS duplicate_key
        FROM `{table_id}`
    """)

    # 2. Simpan hasil quality check
    return [
        audit_check("WAREHOUSE", table_name, "row_count",
                    row.total, "> 0", row.total > 0, run_id, dag_id),

        audit_check("WAREHOUSE", table_name, f"{key_column}_not_null",
                    row.null_key, 0, row.null_key == 0, run_id, dag_id),

        audit_check("WAREHOUSE", table_name, f"{key_column}_unique",
                    row.duplicate_key, 0, row.duplicate_key == 0, run_id, dag_id),
    ]


def check_mart(run_id=None, dag_id=None):
    """Validasi fact, dimension, dan reconciliation batch pada warehouse."""

    # 1. Tentukan fact dan source table
    fact_id = f"{PROJECT_ID}.{MART_LAYER_BQ}.fact_trips"
    clean_id = f"{PROJECT_ID}.{INTER_LAYER_BQ}.int_citibike_trips_clean"

    # 2. Validasi grain, foreign key dan reconciliation fact trips
    trips = run_query(f"""
        SELECT
            (SELECT COUNT(*) FROM `{clean_id}`) AS clean_count,
            (SELECT COUNT(*) FROM `{fact_id}`) AS fact_count,
            (SELECT COUNTIF(ride_id IS NULL) FROM `{fact_id}`) AS null_id,
            (SELECT COUNT(*) - COUNT(DISTINCT ride_id) FROM `{fact_id}`) AS duplicate_id,

            (
                SELECT COUNT(*) FROM `{fact_id}`
                WHERE date_key IS NULL
                    OR start_station_key IS NULL
                    OR end_station_key IS NULL
                    OR rider_type_key IS NULL
                    OR bike_type_key IS NULL
            ) AS null_fk,

            (
                SELECT COUNT(*) FROM `{fact_id}` f
                LEFT JOIN `{PROJECT_ID}.{MART_LAYER_BQ}.dim_date` d
                    ON f.date_key = d.date_key
                LEFT JOIN `{PROJECT_ID}.{MART_LAYER_BQ}.dim_station` ss
                    ON f.start_station_key = ss.station_key
                LEFT JOIN `{PROJECT_ID}.{MART_LAYER_BQ}.dim_station` es
                    ON f.end_station_key = es.station_key
                LEFT JOIN `{PROJECT_ID}.{MART_LAYER_BQ}.dim_rider_type` r
                    ON f.rider_type_key = r.rider_type_key
                LEFT JOIN `{PROJECT_ID}.{MART_LAYER_BQ}.dim_bike_type` b
                    ON f.bike_type_key = b.bike_type_key
                WHERE d.date_key IS NULL
                    OR ss.station_key IS NULL
                    OR es.station_key IS NULL
                    OR r.rider_type_key IS NULL
                    OR b.bike_type_key IS NULL
            ) AS invalid_fk
    """)

    # 3. Definisikan quality check fact
    checks = [
        ("fact_trips", "row_count", trips.fact_count, "> 0", trips.fact_count > 0),
        ("fact_trips", "ride_id_not_null", trips.null_id, 0, trips.null_id == 0),
        ("fact_trips", "ride_id_unique", trips.duplicate_id, 0, trips.duplicate_id == 0),
        ("fact_trips", "foreign_keys_not_null", trips.null_fk, 0, trips.null_fk == 0),
        ("fact_trips", "foreign_keys_valid", trips.invalid_fk, 0, trips.invalid_fk == 0),
        ("fact_trips", "clean_fact_reconciliation", trips.fact_count, trips.clean_count, trips.fact_count == trips.clean_count),
    ]

    # 4. Simpan hasil quality check fact
    results = [
        audit_check("WAREHOUSE", table, name, actual, expected, passed, run_id, dag_id)
        for table, name, actual, expected, passed in checks
    ]

    # 5. Validasi dimension
    results += check_dimension("dim_date", "date_key", run_id, dag_id)
    results += check_dimension("dim_station", "station_key", run_id, dag_id)
    results += check_dimension("dim_rider_type", "rider_type_key", run_id, dag_id)
    results += check_dimension("dim_bike_type", "bike_type_key", run_id, dag_id)

    finish_gate("WAREHOUSE", results)

# Analytics Quality
def check_analytics(run_id=None, dag_id=None):
    """Validasi reconciliation analytics batch sebelum digunakan dashboard."""

    # 1. Tentukan fact dan analytics mart
    fact_id = f"{PROJECT_ID}.{MART_LAYER_BQ}.fact_trips"
    daily_id = f"{PROJECT_ID}.{MART_LAYER_BQ}.mart_daily_rides"
    hourly_id = f"{PROJECT_ID}.{MART_LAYER_BQ}.mart_hourly_rides"
    station_id = f"{PROJECT_ID}.{MART_LAYER_BQ}.mart_station_performance"

    # 2. Bandingkan analytics dengan fact
    row = run_query(f"""
        SELECT
            (SELECT COUNT(*) FROM `{fact_id}`) AS fact_count,
            (SELECT SUM(total_rides) FROM `{daily_id}`) AS daily_count,
            (SELECT SUM(total_rides) FROM `{hourly_id}`) AS hourly_count,
            (SELECT SUM(total_departures) FROM `{station_id}`) AS departures,
            (SELECT SUM(total_arrivals) FROM `{station_id}`) AS arrivals
    """)

    # 3. Definisikan reconciliation analytics
    checks = [
        ("mart_daily_rides", "fact_reconciliation", row.daily_count, row.fact_count, row.daily_count == row.fact_count),
        ("mart_hourly_rides", "fact_reconciliation", row.hourly_count, row.fact_count, row.hourly_count == row.fact_count),
        ("mart_station_performance", "departure_reconciliation", row.departures, row.fact_count, row.departures == row.fact_count),
        ("mart_station_performance", "arrival_reconciliation", row.arrivals, row.fact_count, row.arrivals == row.fact_count),
    ]

    # 4. Simpan hasil quality check
    results = [
        audit_check("ANALYTICS", table, name, actual, expected, passed, run_id, dag_id)
        for table, name, actual, expected, passed in checks
    ]

    finish_gate("ANALYTICS", results)

# +++ STREAMING +++

FRESHNESS = 5
THRESHOLD_DLQ = 100

# Raw Streaming Quality
def check_gbfs_streaming():
    """Validasi kualitas dan freshness RAW GBFS streaming."""

    table_id = f"{PROJECT_ID}.{RAW_LAYER_BQ}.station_status"

    # 1. Periksa data RAW GBFS
    row = run_query(f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(station_id IS NULL OR TRIM(station_id) = '') AS invalid_station,
            COUNTIF(
                num_bikes_available < 0
                OR num_ebikes_available < 0
                OR num_docks_available < 0
            ) AS invalid_availability,
            TIMESTAMP_DIFF(
                CURRENT_TIMESTAMP(),
                MAX(snapshot_timestamp),
                MINUTE
            ) AS freshness
        FROM `{table_id}`
    """)

    # 2. Quality Check RAW GBFS
    if row.total == 0:
        raise ValueError("GBFS data kosong")

    if row.invalid_station > 0:
        raise ValueError("GBFS station tidak valid")

    if row.invalid_availability > 0:
        raise ValueError("GBFS availability tidak valid")

    if row.freshness is None or row.freshness > FRESHNESS:
        raise ValueError(f"GBFS data stale | freshness={row.freshness}m")

    logger.info(f"GBFS PASS | rows={row.total} | freshness={row.freshness}m")

# Streaming Staging Quality
def check_streaming_staging(run_id=None, dag_id=None):
    """Validasi station status pada STAGING streaming."""

    # 1. Tentukan table
    status_id = f"{PROJECT_ID}.{STAG_LAYER_BQ}.stg_station_status"

    # 2. Validasi station status
    status = run_query(f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(station_id IS NULL OR snapshot_timestamp IS NULL) AS invalid_id,
            COUNTIF(
                num_bikes_available < 0
                OR num_ebikes_available < 0
                OR num_docks_available < 0
            ) AS invalid_rules
        FROM `{status_id}`
    """)

    # 3. Definisikan quality check
    checks = [
        ("stg_station_status", "row_count", status.total, "> 0", status.total > 0),
        ("stg_station_status", "identity_valid", status.invalid_id, 0, status.invalid_id == 0),
        ("stg_station_status", "availability_valid", status.invalid_rules, 0, status.invalid_rules == 0),
    ]

    # 4. Simpan hasil quality check
    results = [
        audit_check("STREAMING_STAGING", table, name, actual, expected, passed, run_id, dag_id)
        for table, name, actual, expected, passed in checks
    ]

    finish_gate("STREAMING_STAGING", results)

# Streaming Intermediate Quality
def check_streaming_intermediate(run_id=None, dag_id=None):
    """Validasi station status pada INTERMEDIATE streaming."""

    # 1. Tentukan table
    status_id = f"{PROJECT_ID}.{INTER_LAYER_BQ}.int_station_status"

    # 2. Validasi station status
    status = run_query(f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(station_id IS NULL OR snapshot_timestamp IS NULL) AS invalid_id,
            COUNTIF(
                num_bikes_available < 0
                OR num_ebikes_available < 0
                OR num_docks_available < 0
                OR available_capacity < 0
            ) AS invalid_rules
        FROM `{status_id}`
    """)

    # 3. Definisikan quality check
    checks = [
        ("int_station_status", "row_count", status.total, "> 0", status.total > 0),
        ("int_station_status", "identity_valid", status.invalid_id, 0, status.invalid_id == 0),
        ("int_station_status", "business_rules", status.invalid_rules, 0, status.invalid_rules == 0),
    ]

    # 4. Simpan hasil quality check
    results = [
        audit_check("STREAMING_INTERMEDIATE", table, name, actual, expected, passed, run_id, dag_id)
        for table, name, actual, expected, passed in checks
    ]

    finish_gate("STREAMING_INTERMEDIATE", results)

# Streaming Warehouse Quality
def check_streaming_warehouse(run_id=None, dag_id=None):
    """Validasi grain dan availability fact_station_status."""

    # 1. Tentukan table
    status_id = f"{PROJECT_ID}.{MART_LAYER_BQ}.fact_station_status"

    # 2. Validasi grain dan availability
    station = run_query(f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(station_key IS NULL OR snapshot_timestamp IS NULL) AS invalid_key,
            COUNT(*) - COUNT(
                DISTINCT CONCAT(
                    CAST(station_key AS STRING),
                    '|',
                    CAST(snapshot_timestamp AS STRING)
                )
            ) AS duplicate_event,
            COUNTIF(
                num_bikes_available < 0
                OR num_ebikes_available < 0
                OR num_docks_available < 0
            ) AS invalid_availability
        FROM `{status_id}`
    """)

    # 3. Definisikan quality check
    checks = [
        ("fact_station_status", "row_count", station.total, "> 0", station.total > 0),
        ("fact_station_status", "key_valid", station.invalid_key, 0, station.invalid_key == 0),
        ("fact_station_status", "station_snapshot_unique", station.duplicate_event, 0, station.duplicate_event == 0),
        ("fact_station_status", "availability_valid", station.invalid_availability, 0, station.invalid_availability == 0),
    ]

    # 4. Simpan hasil quality check
    results = [
        audit_check("STREAMING_WAREHOUSE", table, name, actual, expected, passed, run_id, dag_id)
        for table, name, actual, expected, passed in checks
    ]

    finish_gate("STREAMING_WAREHOUSE", results)

# Streaming Analytics Quality
def check_streaming_analytics(run_id=None, dag_id=None):
    """Validasi grain mart_station_live."""

    # 1. Tentukan table
    live_id = f"{PROJECT_ID}.{MART_LAYER_BQ}.mart_station_live"

    # 2. Pastikan satu station hanya satu row
    row = run_query(f"""
        SELECT
            COUNT(*) AS live_count,
            COUNT(DISTINCT station_key) AS live_unique
        FROM `{live_id}`
    """)

    # 3. Definisikan quality check
    checks = [
        ("mart_station_live", "station_unique", row.live_count, row.live_unique, row.live_count == row.live_unique),
    ]

    # 4. Simpan hasil quality check
    results = [
        audit_check("STREAMING_ANALYTICS", table, name, actual, expected, passed, run_id, dag_id)
        for table, name, actual, expected, passed in checks
    ]

    finish_gate("STREAMING_ANALYTICS", results)

# Streaming DLQ Monitoring
def check_gbfs_dlq():
    """Monitor volume data GBFS yang masuk ke DLQ."""

    table_id = f"{PROJECT_ID}.{RAW_LAYER_BQ}.station_status_dlq"

    # 1. Hitung jumlah DLQ 5 menit terakhir
    row = run_query(f"""
        SELECT COUNT(*) AS total
        FROM `{table_id}`
        WHERE detected_at >= TIMESTAMP_SUB(
            CURRENT_TIMESTAMP(),
            INTERVAL 5 MINUTE
        )
    """)

    # 2. Kirim warning jika DLQ melewati threshold
    if row.total > THRESHOLD_DLQ:
        logger.warning(f"GBFS DLQ WARNING | rows={row.total}")

        send_slack(
            "*⚠️ CITI BIKE STREAMING WARNING ⚠️*\n\n"
            "*Check*     : `GBFS DLQ`\n"
            f"*DLQ Rows*  : `{row.total}`\n"
            f"*Threshold* : `{THRESHOLD_DLQ}`\n"
            "*Status*    : `Pipeline tetap berjalan`\n\n"
            "*ACTION REQUIRED: Segera lakukan pengecekan lebih lanjut pada data DLQ.*"
        )

    # 3. Catat PASS jika masih dalam threshold
    else:
        logger.info(f"GBFS DLQ PASS | rows={row.total}")