# NYC Citi Bike Hybrid Data Platform

## Project Overview

Citi Bike merupakan layanan bike-sharing di New York City yang
menghasilkan data perjalanan dari aktivitas penggunanya. Data tersebut
mencakup informasi seperti waktu perjalanan, stasiun asal dan tujuan,
tipe sepeda, serta tipe pengguna.

**Business problem utama adalah bagaimana mengubah data perjalanan
historis dan data terbaru Citi Bike menjadi informasi yang konsisten
untuk memahami pola demand dan aktivitas layanan.** Tanpa proses data
yang terintegrasi, analisis terhadap tren perjalanan, pola penggunaan,
dan aktivitas stasiun menjadi terpisah serta sulit disajikan secara
konsisten.

Project **NYC Citi Bike Hybrid Data Platform** dibangun untuk menjawab
masalah tersebut dengan mengintegrasikan pemrosesan data historis dan
data terbaru dalam satu platform data, sehingga menghasilkan data yang
terstruktur, reliable, dan siap digunakan untuk kebutuhan analitik serta
dashboard.

------------------------------------------------------------------------

## Project Requirements & Assumptions

-   Data historis menggunakan **Citi Bike Trip History periode
    Januari--Juli 2026** dan diproses melalui batch pipeline.
-   Batch pipeline dijalankan secara **harian** dan mendukung proses
    backfill untuk data historis.
-   Data terbaru diproses melalui **streaming pipeline** yang berjalan
    secara terpisah dari batch pipeline.
-   Data diproses melalui beberapa layer untuk memisahkan raw data,
    cleaning, validation, hingga data siap analisis.
-   Pipeline mendukung **safe rerun dan idempotency** untuk mengurangi
    risiko duplikasi data.
-   Data quality check diterapkan pada tahapan penting pipeline dan
    record invalid dipertahankan untuk audit serta troubleshooting.
-   Streaming menerapkan **data freshness check maksimum 5 menit**. Data
    di dalam threshold dianggap fresh, sedangkan data yang melewati
    threshold dianggap stale dan dimonitor.
-   Hasil akhir menyediakan analytical dataset sebagai sumber dashboard.

------------------------------------------------------------------------

## Project Structure

``` text
finpro-citibike-alfian/
├── batch/
│   ├── extract_data.py
│   ├── ingest_station_info.py
│   ├── upload_to_gcs.py
│   └── load_to_bigquery.py
├── streaming/
│   ├── publisher_gbfs.py
│   └── dataflow_gbfs.py
├── dags/
│   ├── citibike_pipeline.py
│   ├── citibike_gbfs_monitoring.py
│   └── citibike_realtime_refresh.py
├── quality/
│   └── quality_checks.py
├── config/
│   ├── settings.py
│   └── alerts.py
├── dbt_citibike/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   │       ├── warehouse/
│   │       └── analytics/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
├── data/
│   ├── source/
│   ├── raw/
│   └── replay/
├── credentials/
├── docs/
├── Dockerfile
├── Dockerfile.streaming
├── docker-compose.yaml
├── requirements.txt
├── requirements-streaming.txt
└── .env
```

### Main Components

-   **`batch/`** --- historical batch ingestion.
-   **`streaming/`** --- GBFS publisher dan Dataflow streaming.
-   **`dags/`** --- Airflow orchestration, GBFS monitoring, dan realtime
    refresh.
-   **`quality/`** --- reusable data quality checks.
-   **`config/`** --- centralized configuration dan alerting.
-   **`dbt_citibike/`** --- staging, intermediate, dimensional
    warehouse, dan analytics marts.
-   **`data/`** --- source, raw daily partitions, dan replay data.
-   **`docs/`** --- technical documentation.

------------------------------------------------------------------------

## Architecture

Project menggunakan **hybrid data architecture** yang menggabungkan
batch processing untuk data historis Citi Bike dan streaming processing
untuk data GBFS terbaru.

![NYC Citi Bike Hybrid Data Platform
Architecture](docs/arsitektur_project.png)

### Batch Pipeline

``` text
Citi Bike Trip History
        │
        ▼
Python Extraction
        │
        ▼
Daily Partition
        │
        ▼
Google Cloud Storage
        │
        ▼
BigQuery RAW
        │
        ▼
dbt Staging
        │
        ▼
dbt Intermediate
   ┌────┴─────┐
   │          │
 Valid     Invalid
   │
   ▼
Dimensional Warehouse
   │
   ▼
Analytics Marts
   │
   ▼
Looker Studio
```

### Streaming Pipeline

``` text
Citi Bike GBFS
      │
      ▼
GBFS Publisher
      │
      ▼
Google Pub/Sub
      │
      ▼
Google Dataflow
   ┌──┴───┐
   │      │
 Valid   Invalid
   │      │
   ▼      ▼
 RAW     DLQ
   │
   ▼
dbt Staging
   │
   ▼
dbt Intermediate
   │
   ▼
Station Status Fact
   │
   ▼
mart_station_live
   │
   ▼
Looker Studio
```
------------------------------------------------------------------------

## How to Start the Project

### 1. Environment Setup

``` bash
git clone <repository-url>
cd finpro-citibike-alfian
cp .env.example .env
```

Simpan Google Cloud service account pada:

``` text
credentials/purwadhika-key.json
```

### 2. Run Batch Pipeline

``` bash
docker compose build
docker compose up -d
```

Airflow UI: `http://localhost:8085`

Aktifkan dan trigger DAG:

``` text
citibike_batch_pipeline
```

### 3. Run Streaming Pipeline

Urutan streaming: **Dataflow → GBFS Publisher → Realtime Refresh**

#### Step 1 --- Start Dataflow

``` bash
docker compose --profile streaming run --rm finpro-alfian-streaming \
  python -m streaming.dataflow_gbfs
```

#### Step 2 --- Start GBFS Publisher

``` bash
docker compose --profile streaming run --rm finpro-alfian-streaming \
  python -m streaming.publisher_gbfs
```

Optional DLQ test:

``` bash
docker compose --profile streaming run --rm finpro-alfian-streaming \
  python -m streaming.publisher_gbfs --test-dlq 101
```

#### Step 3 --- Enable Realtime Refresh

``` bash
docker compose up -d
```

Airflow UI: `http://localhost:8085`

Aktifkan DAG:

``` text
citibike_realtime_refresh
```

------------------------------------------------------------------------

### Layered Transformation

```text
RAW → STAGING → INTERMEDIATE → WAREHOUSE → ANALYTICS MART
```

| Layer | Responsibility |
| --- | --- |
| RAW | Menyimpan data hasil ingestion dengan perubahan seminimal mungkin |
| STAGING | Menstandarkan nama kolom, tipe data, timestamp, dan nilai kategorikal |
| INTERMEDIATE | Menerapkan business rules, enrichment, deduplication, serta memisahkan valid dan rejected records |
| WAREHOUSE | Menyediakan star schema melalui fact dan dimension tables |
| ANALYTICS MART | Menyediakan tabel siap pakai untuk kebutuhan dashboard dan analisis bisnis |

Pemisahan tanggung jawab ini membuat transformasi lebih modular, mudah diuji, dan mudah ditelusuri ketika terjadi masalah.

### Materialization and Idempotency

| Layer | Strategy | Rationale |
| --- | --- | --- |
| Staging | Incremental `MERGE` | Memproses data baru tanpa membangun ulang seluruh tabel |
| Intermediate | Incremental dan partitioned table | Mendukung transformasi business rule secara efisien berdasarkan tanggal perjalanan |
| Warehouse & Marts | Table | Mengoptimalkan analytical query dan performa dashboard |

`ride_id` digunakan sebagai unique key pada data perjalanan. Kombinasi incremental processing, partitioning, dan `MERGE` mendukung **safe rerun**, mengurangi duplikasi, dan memungkinkan historical backfill.

## Data Quality & Reliability

Data quality diterapkan pada batch ingestion, transformation, dan streaming pipeline agar hanya data yang valid diteruskan ke analytical layer.

### Data Quality Checks

| Area | Validation | Handling |
| --- | --- | --- |
| Batch ingestion | Schema, required fields, timestamp, dan koordinat | Invalid records dipisahkan |
| Staging | Data type dan standardization | Data dinormalisasi sebelum transformasi lanjutan |
| Intermediate | Business rules, trip duration, dan duplicate records | Valid dan rejected records dipisahkan |
| dbt | `not_null`, `unique`, dan `accepted_values` | Model dianggap gagal jika test tidak terpenuhi |
| Streaming | Required fields, data type, dan non-negative values | Invalid events dikirim ke dead-letter queue (DLQ) |
| Freshness | Station status maksimal 5 menit | Data stale dideteksi dan dilaporkan |

### Invalid Data Handling

```text
Batch
├── Valid   → Warehouse → Analytics Mart
└── Invalid → Rejected Dataset

Streaming
├── Valid   → BigQuery RAW
└── Invalid → Dead-Letter Queue
```

Invalid data tidak langsung dihapus. Record tersebut tetap disimpan untuk kebutuhan **audit**, **debugging**, dan **troubleshooting**.

### Reliability Mechanisms

| Mechanism | Purpose |
| --- | --- |
| Layered validation | Mencegah invalid data masuk ke analytical layer |
| Rejected data retention | Menyediakan bukti dan konteks untuk troubleshooting |
| dbt tests | Memvalidasi integritas dan konsistensi hasil transformasi |
| Safe rerun and backfill | Mendukung recovery serta historical reprocessing |
| Freshness check | Mendeteksi streaming data yang tidak lagi diperbarui |
| Airflow task dependencies | Menghentikan downstream process ketika task kritis gagal |

## Monitoring & Alerting

Apache Airflow digunakan untuk memonitor workflow batch dan pekerjaan terjadwal. Kegagalan pipeline atau data quality dikirimkan melalui Slack Webhook agar masalah dapat diketahui tanpa pemeriksaan manual.

| Monitoring Area | Condition | Response |
| --- | --- | --- |
| Batch pipeline | Airflow task atau DAG gagal | Mengirim Slack alert |
| Data quality | Quality check gagal | Menghentikan pipeline dan mengirim Slack alert |
| Streaming pipeline | Monitoring GBFS pipeline gagal | Mengirim Slack alert |
| Data freshness | Station status tidak diperbarui lebih dari 5 menit | Menandai data sebagai stale dan mengirim Slack alert |
| Analytics refresh | Refresh serving table gagal | Mengirim Slack alert |

## Dashboard & Results

Hasil akhir pipeline divisualisasikan melalui **Looker Studio**. Dashboard menggabungkan historical trip analytics dan kondisi stasiun terbaru dari GBFS streaming data.

### Citi Bike Analytics Dashboard

![Citi Bike Analytics Dashboard](docs/images/citibike_dashboard.png)

### Dashboard Components

| Component | Information |
| --- | --- |
| Total Rides | Total perjalanan pada periode yang dipilih |
| Avg Daily Rides | Rata-rata jumlah perjalanan per hari |
| Avg Duration | Rata-rata durasi perjalanan |
| Member Rides | Total perjalanan pengguna member |
| Casual Rides | Total perjalanan pengguna casual |
| Daily Rides Trend | Tren volume perjalanan harian |
| Rides by Hour | Distribusi perjalanan berdasarkan jam |
| Top 5 Stations | Lima stasiun dengan aktivitas perjalanan tertinggi |
| Live Station Status | Jumlah stasiun, bike, e-bike, dock, dan waktu pembaruan terbaru |
| Global Filters | Date Range, Rider Type, dan Bike Type |

### Key Findings

Berdasarkan data historis **Januari–Juli 2026**:

- Total perjalanan mencapai **521,588 rides**.
- Rata-rata durasi perjalanan adalah **13.14 menit**.
- Penggunaan didominasi oleh **member** dengan **399,148 rides**.
- Pengguna **casual** mencatat **122,440 rides**.
- Aktivitas tertinggi terjadi pada sore hari, terutama sekitar pukul **17:00–18:00**.
- **Grove St PATH** menjadi stasiun dengan aktivitas perjalanan tertinggi pada data yang ditampilkan.

Dashboard menyediakan filter berdasarkan rentang tanggal, tipe pengguna, dan tipe sepeda sehingga pengguna dapat mengeksplorasi pola perjalanan secara interaktif.