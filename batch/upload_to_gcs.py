from google.cloud import storage
from google.api_core.exceptions import NotFound

from config.settings import (
    BUCKET_ID,
    DATA_LOCAL_PATH,
    LOCATION_ID,
    PROJECT_ID,
    RAW_PREFIX,
    CITIBIKE_LOG,
)

logger = CITIBIKE_LOG("CitibikeGCSUpload")

def ensure_bucket_exists(client: storage.Client) -> storage.Bucket:
    """Gunakan bucket yang tersedia atau buat jika belum ada."""

    # 1. Gunakan GCS bucket jika sudah tersedia.
    try:
        return client.get_bucket(BUCKET_ID)

    except NotFound:
        logger.info(f"Bucket not found. Creating: gs://{BUCKET_ID}")

        bucket = client.create_bucket(
            BUCKET_ID,
            location=LOCATION_ID,
        )
        logger.info(f"Bucket created: gs://{BUCKET_ID}")

        return bucket

def get_local_files() -> list:
    """Ambil seluruh CSV daily partition dari local RAW."""

    # 1. Pastikan directory local RAW tersedia
    if not DATA_LOCAL_PATH.exists():
        raise FileNotFoundError(
            f"Local RAW directory not found: {DATA_LOCAL_PATH}"
        )

    # 2. Ambil seluruh CSV dari daily partition
    files = sorted(DATA_LOCAL_PATH.rglob("*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No CSV files found: {DATA_LOCAL_PATH}"
        )

    return files


def upload_file(bucket: storage.Bucket, file_path) -> None:
    """Upload satu local RAW file ke GCS dengan path partition yang sama."""

    # 1. Ambil relative path agar struktur Hive partition tetap dipertahankan.
    relative_path = file_path.relative_to(DATA_LOCAL_PATH)
    blob_path = f"{RAW_PREFIX}/{relative_path.as_posix()}"

    # 2. Upload file ke deterministic object path di GCS.
    blob = bucket.blob(blob_path)

    blob.upload_from_filename(
        str(file_path),
        content_type="text/csv",
    )

def upload_to_gcs() -> None:
    """Upload seluruh daily partition ke GCS secara bertahap per bulan."""

    # 1. Ambil seluruh daily partition dari local RAW.
    files = get_local_files()
    bucket = ensure_bucket_exists(storage.Client(project=PROJECT_ID))

    logger.info("GCS upload started")
    logger.info(f"Bucket: gs://{BUCKET_ID}")
    logger.info("Uploading....")

    current_month = None
    total = uploaded = failed = 0
    failed_count = 0

    # 2. Upload tetap dilakukan per daily partition
    for file_path in files:
        relative_path = file_path.relative_to(DATA_LOCAL_PATH)

        year, month, day = [
            part.split("=")[1]
            for part in relative_path.parts[:3]
        ]

        month_key = f"{year}-{month}"
        date_key = f"{year}-{month}-{day}"

        # 3. Upload setiap daily partition ke GCS.
        if current_month and month_key != current_month:
            message = f"{current_month} | Uploaded {uploaded}/{total} files"

            if failed:
                message += f" | Failed: {failed}"

            logger.info(message)

            # 4. Reset counter untuk bulan berikutnya
            total = uploaded = failed = 0

        current_month = month_key
        total += 1

        try:
            upload_file(bucket, file_path)
            uploaded += 1

        except Exception as error:
            failed += 1
            failed_count += 1

            logger.error(
                f"{date_key} | Upload failed | "
                f"{relative_path.as_posix()} | {error}"
            )

    # 5. Print summary bulan terakhir
    if current_month:
        message = f"{current_month} | Uploaded {uploaded}/{total} files"

        if failed:
            message += f" | Failed: {failed}"

        logger.info(message)

    logger.info("")

    # 6. Fail task agar Airflow dapat melakukan retry
    if failed_count:
        logger.error(f"GCS upload completed | Failed: {failed_count}")
        raise RuntimeError(
            f"GCS upload failed for {failed_count} file(s)"
        )

    logger.info("GCS upload completed")

if __name__ == "__main__":
    upload_to_gcs()