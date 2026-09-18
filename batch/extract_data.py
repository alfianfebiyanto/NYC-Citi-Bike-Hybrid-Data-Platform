import shutil
import zipfile
import requests
import pandas as pd
from pathlib import Path

from config.settings import (
    CITIBIKE_BASE_URL,
    DATA_LOCAL_PATH,
    HTTP_TIMEOUT,
    SOURCE_FILES,
    CITIBIKE_LOG
)

SOURCE_LOCAL_PATH = Path("data/source")
USE_CHUNKING = False
CSV_CHUNK_SIZE = 250_000
DOWNLOAD_CHUNK_SIZE = 1024 * 1024

logger = CITIBIKE_LOG("CitibikeExtract")


def download_zip(url: str, zip_path: Path) -> float:
    """Download ZIP Citi Bike dan simpan ke local source."""

    # 1. Gunakan ZIP local jika source sudah pernah didownload
    if zip_path.exists():
        file_size_mb = zip_path.stat().st_size / 1_048_576
        return file_size_mb

    # 2. Download secara streaming agar file tidak dimuat seluruhnya ke RAM
    with requests.get(url, stream=True, timeout=HTTP_TIMEOUT) as response:
        response.raise_for_status()

        with zip_path.open("wb") as file:
            for data in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if data:
                    file.write(data)

    # 3. Ambil ukuran source untuk kebutuhan logging
    file_size_mb = zip_path.stat().st_size / 1_048_576

    return file_size_mb

def get_csv_name(zip_path: Path) -> str:
    """Cari CSV trip utama di dalam ZIP source."""

    # 1. Buka ZIP dan ambil hanya file CSV yang valid
    with zipfile.ZipFile(zip_path) as zip_file:
        csv_files = [
            name
            for name in zip_file.namelist()
            if name.lower().endswith(".csv")
            and "__MACOSX" not in name
            and not Path(name).name.startswith("._")
        ]

    # 2. Pastikan ZIP memiliki CSV yang bisa diproses
    if not csv_files:
        raise FileNotFoundError(
            f"CSV tidak ditemukan di dalam {zip_path.name}."
        )

    # 3. Pilih CSV trip utama jika ZIP memiliki lebih dari satu CSV
    main_csv = [
        name
        for name in csv_files
        if "citibike-tripdata" in name.lower()
    ]

    if main_csv:
        return main_csv[0]

    return csv_files[0]

def read_source_data(zip_path: Path,csv_name: str,year: int,month: int,) -> pd.DataFrame:
    """Baca CSV dari ZIP sesuai di source."""

    # Baca CSV langsung dari ZIP 
    with zipfile.ZipFile(zip_path) as zip_file:
        with zip_file.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file)

    return filter_period(df, year, month)

def filter_period(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Ambil hanya data dengan started_at valid sesuai periode source."""

    # 1. Pastikan kolom utama tersedia
    if "started_at" not in df.columns:
        raise ValueError("Column 'started_at' tidak ditemukan.")

    # 2. Ubah started_at menjadi timestamp
    df["started_at"] = pd.to_datetime(
        df["started_at"],
        errors="coerce",
    )

    # 3. Ambil hanya data sesuai tahun dan bulan source
    valid_mask = (
        df["started_at"].notna()
        & (df["started_at"].dt.year == year)
        & (df["started_at"].dt.month == month)
    )

    return df[valid_mask].copy()

def clear_month_partition(year: int, month: int) -> None:
    """Hapus output periode lama sebelum membuat ulang data bulan tersebut."""

    month_path = DATA_LOCAL_PATH / f"year={year}" / f"month={month:02d}"
    # 1. Bersihkan hasil sebelumnya agar rerun tidak menghasilkan duplicate
    if month_path.exists():
        shutil.rmtree(month_path)


def split_daily(df: pd.DataFrame) -> int:
    """Split data berdasarkan tanggal perjalanan dan simpan ke local RAW."""

    if df.empty:
        return 0

    # 1. Gunakan started_at untuk menentukan tanggal partition
    df["trip_date"] = df["started_at"].dt.date

    daily_groups = df.groupby("trip_date")

    # 2. Setiap tanggal menghasilkan satu daily partition
    for trip_date, daily_df in daily_groups:
        output_dir = (
            DATA_LOCAL_PATH
            / f"year={trip_date.year}"
            / f"month={trip_date.month:02d}"
            / f"day={trip_date.day:02d}"
        )

        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "citibike_trips.csv"
        daily_df.drop(columns="trip_date").to_csv(
            output_file,
            index=False,
        )

    return daily_groups.ngroups

def process_source(year_month: str, filename: str) -> tuple[float, int, int]:
    """Process satu source bulanan Citi Bike dari download sampai daily partition."""

    year = int(year_month[:4])
    month = int(year_month[4:])

    url = f"{CITIBIKE_BASE_URL}/{filename}"
    zip_path = SOURCE_LOCAL_PATH / filename

    # 1. Download atau gunakan ZIP yang sudah tersedia di local
    file_size_mb = download_zip(url, zip_path)

    # 2. Cari CSV trip utama di dalam ZIP
    csv_name = get_csv_name(zip_path)

    # 3. Baca source dan ambil hanya data sesuai periode
    df = read_source_data(zip_path, csv_name, year, month)

    # 4. Bersihkan output lama agar rerun tetap aman
    clear_month_partition(year, month)

    # 5. Split data valid menjadi daily partition
    partitions = split_daily(df)

    return file_size_mb, len(df), partitions


def main() -> None:
    """Jalankan batch extraction seluruh historical source Citi Bike."""

    SOURCE_LOCAL_PATH.mkdir(parents=True, exist_ok=True)
    DATA_LOCAL_PATH.mkdir(parents=True, exist_ok=True)

    logger.info("CITIBIKE BATCH EXTRACTION STARTED")

    total_valid_rows = 0
    total_partitions = 0

    for index, (year_month, filename) in enumerate(SOURCE_FILES.items(), start=1):
        year = int(year_month[:4])
        month = int(year_month[4:])
        month_name = pd.Timestamp(year=year, month=month, day=1).strftime("%B %Y")

        logger.info(f"[{index}/{len(SOURCE_FILES)}] {month_name}")

        file_size_mb, valid_rows, partitions = process_source(year_month, filename)

        logger.info(f"      Source     : {filename}")
        logger.info(f"      Size       : {file_size_mb:.1f} MB")
        logger.info(f"      Valid Rows : {valid_rows:,}")
        logger.info(f"      Partitions : {partitions} days")
        logger.info("")

        total_valid_rows += valid_rows
        total_partitions += partitions

    logger.info("Batch extraction completed")
    logger.info(f"      Sources    : {len(SOURCE_FILES)}")
    logger.info(f"      Valid Rows : {total_valid_rows:,}")
    logger.info(f"      Partitions : {total_partitions} days")

if __name__ == "__main__":
    main()