# NYC Citi Bike --- Hybrid Data Platform

## Master Execution Guideline & Checklist

**Project name:** `finpro_citibike_alfian`\
**Status:** Master guideline / source of truth\
**Purpose:** Menjaga pengerjaan Final Project tetap konsisten dari awal
sampai akhir dan mencegah perubahan scope/desain tanpa alasan yang
jelas.

------------------------------------------------------------------------

# 0. PROJECT GOVERNANCE

## 0.1 Golden Rules

-   [ ] Dokumen ini menjadi **guideline utama** selama pengerjaan
    project.
-   [ ] Jangan menambah teknologi baru hanya karena terlihat menarik.
-   [ ] Jangan mengganti arsitektur yang sudah `LOCKED` tanpa alasan
    teknis atau hasil profiling.
-   [ ] Jangan lanjut ke fase berikutnya jika core requirement fase
    sebelumnya belum selesai.
-   [ ] Setiap perubahan desain penting harus dicatat di
    `docs/decisions.md`.
-   [ ] Bedakan keputusan menjadi:
    -   `LOCKED` --- sudah disepakati dan tidak diubah tanpa alasan
        kuat.
    -   `PROVISIONAL` --- masih menunggu validasi data/implementasi.
    -   `REVISE` --- keputusan lama terbukti perlu diubah.
-   [ ] Prioritaskan pipeline yang **berfungsi, bisa dijelaskan, dan
    reproducible** daripada menambah banyak tools.
-   [ ] Batch dan streaming harus bertemu pada business model yang sama.
-   [ ] Pipeline batch harus aman saat dijalankan ulang / idempotent.
-   [ ] Data quality harus berdasarkan profiling + business rules, bukan
    asumsi.

## 0.2 Change Control

Sebelum mengubah desain yang sudah disepakati, jawab:

-   [ ] Apa masalah pada desain sekarang?
-   [ ] Apakah perubahan benar-benar dibutuhkan?
-   [ ] Apakah ada bukti dari profiling/error/requirement?
-   [ ] Apa dampaknya terhadap batch?
-   [ ] Apa dampaknya terhadap streaming?
-   [ ] Apa dampaknya terhadap dbt/model?
-   [ ] Apa dampaknya terhadap Airflow?
-   [ ] Apa dampaknya terhadap dashboard?
-   [ ] Apakah keputusan sudah dicatat di `docs/decisions.md`?

Jika jawabannya hanya **"lebih keren"**, **"ingin coba tool lain"**,
atau **"project lain pakai ini"**, jangan ubah desain.

------------------------------------------------------------------------

# 1. LOCKED PROJECT BASELINE

## Project

-   [x] Project: **NYC Citi Bike --- Hybrid Batch & Streaming Data
    Platform**
-   [x] Repository/project naming: `finpro_citibike_alfian`
-   [x] Data source: Citi Bike trip data
-   [x] Historical batch: **Jan--Jul 2026**
-   [x] Streaming simulation: **Aug 2026**
-   [x] Streaming strategy: **trip replay**
-   [x] Batch schedule: **daily**
-   [x] Main grain: **1 row = 1 trip / `ride_id`**

## Technology

-   [x] Python
-   [x] Docker / Docker Compose
-   [x] Apache Airflow
-   [x] Google Cloud Storage
-   [x] BigQuery
-   [x] dbt
-   [x] Pub/Sub
-   [x] Looker Studio
-   [x] Git / GitHub

## Transformation Architecture

``` text
RAW
 ↓
STAGING
 ↓
INTERMEDIATE
 ├── VALID
 └── REJECTED
 ↓
MART
 ↓
DASHBOARD
```

-   [x] Tidak menggunakan istilah Bronze/Silver/Gold sebagai nama layer
    utama.
-   [x] Gunakan terminology `RAW → STAGING → INTERMEDIATE → MART` secara
    konsisten.

## Main Star Schema

``` text
             dim_date
                 │
                 │
dim_station ─ fact_trips
```

-   [x] `fact_trips`
-   [x] `dim_date`
-   [x] `dim_station`
-   [ ] Final columns menunggu profiling/final modeling.

------------------------------------------------------------------------

# 2. NAMING CONVENTION

Base naming:

``` text
finpro_citibike_alfian
```

Gunakan underscore jika service mendukungnya.

## GCP

-   [x] GCP Project ID: `jcdeah-009`
-   [x] Region: `asia-southeast2`
-   [x] Credential file: `credentials/purwadhika-key.json`
-   [x] GCS exception: bucket menggunakan hyphen karena underscore tidak
    diperbolehkan.

## GCS

-   [x] Bucket: `finpro-citibike-alfian-datalake`
-   [x] RAW prefix: `raw/citibike`

## BigQuery

-   [x] RAW: `finpro_citibike_alfian_raw`
-   [x] STAGING: `finpro_citibike_alfian_staging`
-   [x] INTERMEDIATE: `finpro_citibike_alfian_intermediate`
-   [x] MART: `finpro_citibike_alfian_mart`
-   [x] AUDIT: `finpro_citibike_alfian_audit`

## Pub/Sub

-   [x] Topic: `finpro_citibike_alfian_topic`
-   [x] Subscription: `finpro_citibike_alfian_sub`

## Airflow

-   [x] Timezone: `Asia/Jakarta`
-   [x] DAG naming: `finpro_citibike_alfian_daily_pipeline`

------------------------------------------------------------------------

# 3. TARGET MVP REPOSITORY

``` text
finpro_citibike_alfian/
│
├── .env
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yml
│
├── config/
│   ├── __init__.py
│   └── settings.py
│
├── credentials/
│   └── .gitkeep
│
├── data/
│   ├── raw/
│   │   ├── batch/
│   │   └── streaming/
│   └── processed/
│       ├── batch/
│       └── streaming/
│
├── batch/
│   ├── __init__.py
│   ├── extract.py
│   ├── split_daily.py
│   ├── upload_gcs.py
│   └── load_bigquery.py
│
├── streaming/
│   ├── __init__.py
│   ├── producer.py
│   ├── processor.py
│   └── schemas.py
│
├── dags/
│   └── citibike_daily_pipeline.py
│
├── dbt_citibike/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _staging__sources.yml
│   │   │   ├── _staging__models.yml
│   │   │   ├── stg_batch_trips.sql
│   │   │   ├── stg_stream_trips.sql
│   │   │   └── stg_trips.sql
│   │   ├── intermediate/
│   │   │   ├── _intermediate__models.yml
│   │   │   ├── int_trips_valid.sql
│   │   │   └── int_trips_rejected.sql
│   │   └── marts/
│   │       ├── _marts__models.yml
│   │       ├── dim_date.sql
│   │       ├── dim_station.sql
│   │       ├── fact_trips.sql
│   │       ├── mart_daily_trip_summary.sql
│   │       └── mart_station_performance.sql
│   ├── macros/
│   └── tests/
│
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    ├── data_quality.md
    ├── decisions.md
    └── qna.md
```

**Deferred:** `quality/`, `alerts/`, `scripts/`, dan root automated
`tests/` belum dibuat pada tahap MVP. Tambahkan hanya ketika core
pipeline sudah stabil.

------------------------------------------------------------------------

# PHASE 0 --- PROJECT DESIGN

**Goal:** Mengunci arah project sebelum implementasi.

## Checklist

-   [x] Tentukan dataset Citi Bike.
-   [x] Tentukan business goal.
-   [x] Tentukan stakeholder orientation: operations/management +
    kebutuhan presentasi Final Project.
-   [x] Tentukan historical period Jan--Jul 2026.
-   [x] Tentukan Aug 2026 sebagai streaming replay.
-   [x] Pilih replay trip dibanding GBFS.
-   [x] Tentukan batch berjalan daily.
-   [x] Tentukan GCS sebagai datalake.
-   [x] Tentukan BigQuery sebagai warehouse.
-   [x] Tentukan dbt sebagai transformation.
-   [x] Tentukan Airflow sebagai batch orchestrator.
-   [x] Tentukan Pub/Sub sebagai streaming messaging.
-   [x] Tentukan star schema.
-   [x] Tentukan `ride_id` sebagai business identifier utama.
-   [x] Tentukan adanya valid + rejected path.
-   [x] Tentukan data quality/checkpoint setiap layer sebagai target
    final.

## Business Questions

-   [x] Kapan demand perjalanan paling tinggi?
-   [x] Station mana yang paling sibuk?
-   [x] Route station mana yang paling sering digunakan?
-   [x] Bagaimana pola member vs casual?
-   [x] Bagaimana weekday vs weekend?
-   [x] Bagaimana electric vs classic bike?

### Exit Criteria

-   [x] Problem statement jelas.
-   [x] Scope batch dan streaming jelas.
-   [x] Stack utama jelas.
-   [x] Business questions jelas.
-   [x] Tidak ada teknologi utama yang perlu dipilih ulang.

**STATUS: LOCKED**

------------------------------------------------------------------------

# PHASE 1 --- DATASET PROFILING

**Goal:** Memahami data sebelum membuat DQ rules dan final model.

## 1.1 Source

-   [ ] Download dataset Jan 2026.
-   [ ] Download dataset Feb 2026.
-   [ ] Download dataset Mar 2026.
-   [ ] Download dataset Apr 2026.
-   [ ] Download dataset May 2026.
-   [ ] Download dataset Jun 2026.
-   [ ] Download dataset Jul 2026.
-   [ ] Download/prepare Aug 2026 untuk replay.
-   [ ] Simpan source batch pada `data/raw/batch/`.
-   [ ] Simpan source replay pada `data/raw/streaming/`.
-   [ ] Pastikan raw source tidak dimodifikasi.

## 1.2 Profiling

-   [ ] Hitung row count.
-   [ ] Cek seluruh nama kolom.
-   [ ] Cek data type.
-   [ ] Cek date range.
-   [ ] Cek NULL per kolom.
-   [ ] Cek duplicate row.
-   [ ] Cek duplicate `ride_id`.
-   [ ] Cek uniqueness `ride_id`.
-   [ ] Cek nilai `rideable_type`.
-   [ ] Cek nilai `member_casual`.
-   [ ] Cek missing start station.
-   [ ] Cek missing end station.
-   [ ] Cek missing coordinates.
-   [ ] Cek `ended_at <= started_at`.
-   [ ] Cek distribusi trip duration.
-   [ ] Cek kemungkinan extreme duration/outlier.
-   [ ] Cek station ID/name consistency.
-   [ ] Dokumentasikan hasil profiling.

## 1.3 Data Contract & Rules

-   [ ] Lock grain `1 row = 1 ride_id`.
-   [ ] Tentukan required columns.
-   [ ] Tentukan nullable columns yang legitimate.
-   [ ] Tentukan accepted values.
-   [ ] Tentukan duplicate handling.
-   [ ] Tentukan invalid timestamp rules.
-   [ ] Tentukan duration rules.
-   [ ] Tentukan station rules.
-   [ ] Tentukan coordinate rules.
-   [ ] Tentukan rejected reasons.
-   [ ] Catat rules di `docs/data_quality.md`.

### Exit Criteria

-   [ ] Dataset dipahami.
-   [ ] DQ rules berasal dari profiling.
-   [ ] Grain tervalidasi.
-   [ ] Valid vs rejected criteria jelas.
-   [ ] Tidak ada rule penting yang hanya berdasarkan asumsi.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 2 --- PROJECT FOUNDATION

**Goal:** Menyiapkan repository dan environment yang bersih.

## Checklist

-   [ ] Buat root `finpro_citibike_alfian/`.
-   [ ] Buat struktur folder MVP.
-   [ ] Buat Python virtual environment.
-   [ ] Activate environment.
-   [ ] Buat `requirements.txt`.
-   [ ] Buat `.gitignore`.
-   [ ] Buat `.env`.
-   [ ] Buat `.env.example`.
-   [ ] Buat `config/settings.py`.
-   [ ] Pastikan configuration tidak hardcoded di berbagai script.
-   [ ] Simpan `purwadhika-key.json` di `credentials/`.
-   [ ] Pastikan `.env` di-ignore Git.
-   [ ] Pastikan `credentials/*.json` di-ignore Git.
-   [ ] Initialize Git.
-   [ ] Test import settings.
-   [ ] Test Google authentication.
-   [ ] Test BigQuery connection.
-   [ ] Test GCS connection.
-   [ ] Test Pub/Sub permission/connection.

### Exit Criteria

-   [ ] Python environment berjalan.
-   [ ] Configuration terpusat.
-   [ ] Credential bekerja.
-   [ ] GCP dapat diakses dari Python.
-   [ ] Secret tidak masuk Git.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 3 --- GCP INFRASTRUCTURE

**Goal:** Menyiapkan resource cloud yang diperlukan.

## GCS

-   [ ] Create/validate `finpro-citibike-alfian-datalake`.
-   [ ] Set region sesuai project.
-   [ ] Buat/validasi logical RAW prefix.
-   [ ] Test upload object.
-   [ ] Test read object.

Target:

``` text
raw/citibike/trips/year=YYYY/month=MM/day=DD/
```

## BigQuery

-   [ ] Create `finpro_citibike_alfian_raw`.
-   [ ] Create `finpro_citibike_alfian_staging`.
-   [ ] Create `finpro_citibike_alfian_intermediate`.
-   [ ] Create `finpro_citibike_alfian_mart`.
-   [ ] Create `finpro_citibike_alfian_audit`.
-   [ ] Pastikan location konsisten.
-   [ ] Test `SELECT 1`.

## Pub/Sub

-   [ ] Create `finpro_citibike_alfian_topic`.
-   [ ] Create `finpro_citibike_alfian_sub`.
-   [ ] Hubungkan subscription ke topic.
-   [ ] Test publish.
-   [ ] Test consume.

### Exit Criteria

-   [ ] GCS ready.
-   [ ] BigQuery ready.
-   [ ] Pub/Sub ready.
-   [ ] Permission service account cukup.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 4 --- BATCH PIPELINE

**Goal:** Membuat jalur batch bekerja manual sebelum Airflow.

## 4.1 Extract

File: `batch/extract.py`

-   [ ] Read source file.
-   [ ] Validate source path.
-   [ ] Validate expected columns.
-   [ ] Filter/identify requested processing date.
-   [ ] Tidak mengubah raw source.

## 4.2 Daily Processing

File: `batch/split_daily.py`

-   [ ] Monthly source dapat diproses berdasarkan tanggal.
-   [ ] Gunakan `started_at` sebagai dasar business/process date jika
    sudah tervalidasi.
-   [ ] Output hanya berisi requested date.
-   [ ] Output naming konsisten.
-   [ ] Test minimal 3 tanggal berbeda.

## 4.3 Upload GCS

File: `batch/upload_gcs.py`

-   [ ] Upload daily output.
-   [ ] Partition path `year/month/day`.
-   [ ] Validate object exists.
-   [ ] Validate object size \> 0.
-   [ ] Rerun tidak membuat random duplicate object.

## 4.4 BigQuery RAW

File: `batch/load_bigquery.py`

-   [ ] Buat/validasi RAW batch table.
-   [ ] Load data GCS → BigQuery.
-   [ ] Preserve source fields.
-   [ ] Tambahkan metadata ingestion jika diperlukan.
-   [ ] Validate row count.
-   [ ] Validate processing date.

## 4.5 Idempotency

-   [ ] Jalankan satu tanggal pertama kali.
-   [ ] Catat row count.
-   [ ] Jalankan tanggal sama kedua kali.
-   [ ] Pastikan tidak terjadi duplicate analytical source.
-   [ ] Cek duplicate `ride_id`.
-   [ ] Dokumentasikan idempotency strategy.

## 4.6 Reconciliation

-   [ ] Compare local daily rows.
-   [ ] Compare GCS daily rows.
-   [ ] Compare BigQuery RAW rows.
-   [ ] Investigate mismatch jika ada.

### Exit Criteria

-   [ ] Local → GCS berhasil.
-   [ ] GCS → BigQuery RAW berhasil.
-   [ ] Satu processing date dapat diproses secara independen.
-   [ ] Rerun aman.
-   [ ] Row count dapat direkonsiliasi.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 5 --- DBT TRANSFORMATION

**Goal:** Mengubah RAW menjadi trustworthy analytical model.

## 5.1 dbt Setup

-   [ ] Configure `dbt_project.yml`.
-   [ ] Configure `profiles.yml`.
-   [ ] Gunakan environment variables.
-   [ ] Jalankan `dbt debug`.
-   [ ] Pastikan connection `OK`.

## 5.2 Source

-   [ ] Buat `_staging__sources.yml`.
-   [ ] Define RAW batch source.
-   [ ] Define RAW stream source.
-   [ ] Tambahkan source tests yang relevan.

## 5.3 Staging

Models:

-   [ ] `stg_batch_trips.sql`
-   [ ] `stg_stream_trips.sql`
-   [ ] `stg_trips.sql`
-   [ ] `_staging__models.yml`

Responsibilities:

-   [ ] Rename.
-   [ ] Casting.
-   [ ] Trim.
-   [ ] Standardization.
-   [ ] Source identification.
-   [ ] Technical cleanup.
-   [ ] Hindari heavy business filtering.

## 5.4 Intermediate

Models:

-   [ ] `int_trips_valid.sql`
-   [ ] `int_trips_rejected.sql`
-   [ ] `_intermediate__models.yml`

Logic:

-   [ ] Implement business validation.
-   [ ] Implement `rejection_reason`.
-   [ ] Calculate `duration_minutes`.
-   [ ] Calculate `trip_date`.
-   [ ] Calculate `trip_hour`.
-   [ ] Calculate weekday/weekend.
-   [ ] Pisahkan valid.
-   [ ] Pertahankan rejected.
-   [ ] Validate `valid + rejected` terhadap staging sesuai rules.

## 5.5 Star Schema

-   [ ] Finalize `dim_date`.
-   [ ] Finalize `dim_station`.
-   [ ] Finalize `fact_trips`.
-   [ ] Confirm fact grain.
-   [ ] Define surrogate/business keys.
-   [ ] Define station relationship.
-   [ ] Define date relationship.
-   [ ] Define unknown/missing member strategy jika dibutuhkan.

## 5.6 Analytical Marts

-   [ ] `mart_daily_trip_summary.sql`.
-   [ ] `mart_station_performance.sql`.
-   [ ] Pastikan mart menjawab business questions.
-   [ ] Jangan membuat mart yang tidak digunakan.

## 5.7 Tests

-   [ ] `ride_id` not null.
-   [ ] `ride_id` unique pada grain final.
-   [ ] Accepted values.
-   [ ] Relationships.
-   [ ] Required dimension keys.
-   [ ] Duration validation.
-   [ ] Jalankan `dbt build`.
-   [ ] Semua critical tests PASS.

### Exit Criteria

-   [ ] RAW → STAGING berhasil.
-   [ ] STAGING → INTERMEDIATE berhasil.
-   [ ] Valid/rejected bekerja.
-   [ ] Star schema terbentuk.
-   [ ] Marts terbentuk.
-   [ ] `dbt build` sukses.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 6 --- AIRFLOW ORCHESTRATION

**Goal:** Mengorkestrasi batch pipeline secara daily.

## Setup

-   [ ] Configure Docker Compose.
-   [ ] Airflow database berjalan.
-   [ ] Airflow scheduler berjalan.
-   [ ] Airflow UI/API component yang dibutuhkan berjalan.
-   [ ] DAG terdeteksi tanpa import error.

## DAG

Target DAG:

``` text
finpro_citibike_alfian_daily_pipeline
```

-   [ ] Schedule daily.
-   [ ] Timezone `Asia/Jakarta`.
-   [ ] Set retries = 2 untuk critical tasks.
-   [ ] Set retry delay = 5 minutes.
-   [ ] Gunakan logical/business date.
-   [ ] Satu run hanya memproses satu tanggal.

Target dependency:

``` text
extract_daily
 ↓
upload_gcs
 ↓
check_gcs
 ↓
load_raw
 ↓
check_raw
 ↓
dbt_staging
 ↓
check_staging
 ↓
dbt_intermediate
 ↓
check_intermediate
 ↓
dbt_mart
 ↓
check_mart
```

## Validation

-   [ ] Test satu daily run.
-   [ ] Test rerun tanggal sama.
-   [ ] Test intentional task failure.
-   [ ] Validate retry.
-   [ ] Validate downstream stop saat critical upstream gagal.
-   [ ] Test beberapa tanggal.
-   [ ] Jalankan historical backfill Jan--Jul menggunakan logic yang
    sama.
-   [ ] Validate no duplicate setelah backfill.

### Exit Criteria

-   [ ] Daily DAG end-to-end sukses.
-   [ ] Logical date bekerja.
-   [ ] Retry bekerja.
-   [ ] Checkpoint layer bekerja.
-   [ ] Historical backfill berhasil.
-   [ ] Idempotency tetap terjaga.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 7 --- STREAMING REPLAY

**Goal:** Mensimulasikan incoming Citi Bike trip melalui Pub/Sub.

## Replay Data

-   [ ] Prepare Aug 2026.
-   [ ] Pisahkan dari batch historical scope.
-   [ ] Validate schema compatible dengan batch business grain.

## Producer

File: `streaming/producer.py`

-   [ ] Read replay dataset.
-   [ ] Convert record ke event payload.
-   [ ] Publish event ke Pub/Sub.
-   [ ] Atur replay interval/rate.
-   [ ] Include deterministic identifier.
-   [ ] Log publish success/failure.

## Schema

File: `streaming/schemas.py`

-   [ ] Define required fields.
-   [ ] Define data types.
-   [ ] Define event metadata jika diperlukan.
-   [ ] Validate payload sebelum publish/process.

## Processor

File: `streaming/processor.py`

-   [ ] Consume subscription.
-   [ ] Parse payload.
-   [ ] Validate event.
-   [ ] Handle malformed message.
-   [ ] Load ke RAW stream.
-   [ ] Acknowledge message setelah safe processing.
-   [ ] Log processing result.

## BigQuery

-   [ ] Buat/validasi `raw_trips_stream`.
-   [ ] Validate incoming events.
-   [ ] Test duplicate replay.
-   [ ] Pastikan duplicate tidak menggandakan final fact.

## Convergence

``` text
RAW_BATCH ──┐
            ▼
         STAGING
            ▲
RAW_STREAM ─┘
```

-   [ ] `stg_batch_trips` bekerja.
-   [ ] `stg_stream_trips` bekerja.
-   [ ] `stg_trips` menggabungkan keduanya.
-   [ ] Schema compatible.
-   [ ] Duplicate strategy bekerja.
-   [ ] Streaming trip muncul di final `fact_trips`.

### Exit Criteria

-   [ ] Producer → Pub/Sub sukses.
-   [ ] Pub/Sub → processor sukses.
-   [ ] Processor → BigQuery sukses.
-   [ ] dbt menerima batch + stream.
-   [ ] Final business grain tetap sama.
-   [ ] Duplicate protection tervalidasi.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 8 --- DATA QUALITY, AUDIT & ALERT

**Goal:** Menambah observability setelah core pipeline stabil.

> Folder `quality/` dan `alerts/` baru ditambahkan pada fase ini jika
> implementasinya memang diperlukan.

## GCS Check

-   [ ] Object exists.
-   [ ] File/object tidak kosong.
-   [ ] Expected schema/columns tersedia.
-   [ ] Daily partition sesuai.

## RAW Check

-   [ ] Row count \> 0 untuk expected processing day.
-   [ ] Required technical fields tersedia.
-   [ ] Processing date sesuai.

## STAGING Check

-   [ ] Casting berhasil.
-   [ ] Required fields sesuai rule.
-   [ ] Accepted technical values.
-   [ ] Duplicate behavior diketahui.

## INTERMEDIATE Check

-   [ ] Valid records sesuai rules.
-   [ ] Rejected records retained.
-   [ ] Rejected reason tersedia.
-   [ ] Reconciliation valid + rejected dilakukan.

## MART Check

-   [ ] Fact grain benar.
-   [ ] `ride_id` unique.
-   [ ] Dimension relationships valid.
-   [ ] Critical measures valid.

## Audit

-   [ ] Gunakan `finpro_citibike_alfian_audit`.
-   [ ] Buat audit table.
-   [ ] Record run date.
-   [ ] Record layer.
-   [ ] Record check name.
-   [ ] Record status.
-   [ ] Record row/failed count.
-   [ ] Record timestamp.

## Alert

-   [ ] Configure Slack webhook/connection secara aman.
-   [ ] Alert hanya untuk meaningful failure.
-   [ ] Sertakan DAG.
-   [ ] Sertakan task.
-   [ ] Sertakan run date.
-   [ ] Sertakan error/status.
-   [ ] Test intentional failure alert.

### Exit Criteria

-   [ ] Setiap critical layer punya quality gate.
-   [ ] Audit history tersedia.
-   [ ] Failure dapat diketahui.
-   [ ] Alert bekerja.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 9 --- DASHBOARD

**Goal:** Menjawab business questions menggunakan MART.

## Connection

-   [ ] Connect Looker Studio ke BigQuery MART.
-   [ ] Pastikan dashboard tidak membaca RAW langsung.

## KPI

-   [ ] Total trips.
-   [ ] Member trips.
-   [ ] Casual trips.
-   [ ] Average trip duration.

## Analysis

-   [ ] Trips by date.
-   [ ] Trips by month jika relevan.
-   [ ] Trips by hour.
-   [ ] Weekday vs weekend.
-   [ ] Top start stations.
-   [ ] Top end stations.
-   [ ] Popular routes.
-   [ ] Member vs casual.
-   [ ] Electric vs classic bike.

## Quality

-   [ ] Setiap chart menjawab business question.
-   [ ] Filter/date controls bekerja.
-   [ ] Angka dashboard divalidasi terhadap BigQuery.
-   [ ] Tidak ada chart dekoratif tanpa tujuan.
-   [ ] Dashboard screenshot disiapkan untuk README/slides.

### Exit Criteria

-   [ ] Business questions terjawab.
-   [ ] Dashboard numbers tervalidasi.
-   [ ] Dashboard siap demo.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 10 --- DOCUMENTATION

**Goal:** Membuat project dapat dipahami tanpa membaca seluruh source
code.

## README

-   [ ] Project overview.
-   [ ] Problem statement.
-   [ ] Business questions.
-   [ ] Architecture.
-   [ ] Batch flow.
-   [ ] Streaming flow.
-   [ ] Tech stack.
-   [ ] Dataset.
-   [ ] Data model.
-   [ ] Data quality.
-   [ ] How to setup.
-   [ ] How to run.
-   [ ] Dashboard.
-   [ ] Key findings.
-   [ ] Repository structure.

## `architecture.md`

-   [ ] Source architecture.
-   [ ] Batch architecture.
-   [ ] Streaming architecture.
-   [ ] GCS role.
-   [ ] BigQuery role.
-   [ ] dbt role.
-   [ ] Airflow role.
-   [ ] Pub/Sub role.
-   [ ] Batch-stream convergence.
-   [ ] Idempotency explanation.

## `data_dictionary.md`

-   [ ] RAW tables.
-   [ ] Staging models.
-   [ ] Intermediate models.
-   [ ] `fact_trips`.
-   [ ] `dim_date`.
-   [ ] `dim_station`.
-   [ ] Analytical marts.
-   [ ] Grain.
-   [ ] Key definitions.

## `data_quality.md`

-   [ ] DQ rule ID.
-   [ ] Rule description.
-   [ ] Layer.
-   [ ] Severity.
-   [ ] Action on failure.
-   [ ] Rejection reasons.

## `decisions.md`

-   [ ] Replay vs GBFS decision.
-   [ ] Daily batch decision.
-   [ ] Layer naming decision.
-   [ ] Star schema decision.
-   [ ] Idempotency decision.
-   [ ] Rejected-data strategy.
-   [ ] Semua revisi penting selama development.

## `qna.md`

-   [ ] Why batch + streaming?
-   [ ] Why replay?
-   [ ] Why Pub/Sub?
-   [ ] Why GCS?
-   [ ] Why BigQuery?
-   [ ] Why dbt?
-   [ ] Why Airflow?
-   [ ] Why star schema?
-   [ ] What is grain?
-   [ ] How does idempotency work?
-   [ ] How does rerun work?
-   [ ] What happens to invalid records?
-   [ ] How do batch and streaming converge?
-   [ ] How do you guarantee data quality?
-   [ ] What happens when a task fails?

### Exit Criteria

-   [ ] Orang lain dapat memahami architecture.
-   [ ] Orang lain dapat menjalankan project dari README.
-   [ ] Semua major design decisions terdokumentasi.
-   [ ] Q&A sidang tersedia.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 11 --- GITHUB CLEANUP & REPRODUCIBILITY

**Goal:** Membuat repository aman dan profesional.

## Security

-   [ ] `.env` tidak tracked.
-   [ ] Credential JSON tidak tracked.
-   [ ] Tidak ada API key.
-   [ ] Tidak ada Slack webhook secret.
-   [ ] Tidak ada password real.
-   [ ] Cek Git history untuk secret.

## Cleanup

-   [ ] Hapus `__pycache__`.
-   [ ] Hapus temp files.
-   [ ] Hapus unnecessary logs.
-   [ ] Hapus unused scripts.
-   [ ] Hapus dead code.
-   [ ] Hapus dataset besar dari Git.
-   [ ] Update `requirements.txt`.
-   [ ] Update `.env.example`.

## Reproducibility

-   [ ] Fresh clone repository.
-   [ ] Create virtual environment.
-   [ ] Install dependencies.
-   [ ] Configure `.env`.
-   [ ] Add credential locally.
-   [ ] Test connection.
-   [ ] Run core pipeline.
-   [ ] Jalankan dbt.
-   [ ] Jalankan Airflow.
-   [ ] Validate hasil sesuai environment utama.

### Exit Criteria

-   [ ] Repo aman dipublish.
-   [ ] Repo bersih.
-   [ ] Setup instructions benar.
-   [ ] Fresh clone dapat dijalankan.

**STATUS: TODO**

------------------------------------------------------------------------

# PHASE 12 --- FINAL DEMO & SIDANG

**Goal:** Bisa mempertanggungjawabkan engineering decisions, bukan
sekadar menunjukkan pipeline berjalan.

## Architecture Understanding

-   [ ] Bisa jelaskan source → dashboard tanpa membaca catatan.
-   [ ] Bisa jelaskan batch flow.
-   [ ] Bisa jelaskan streaming flow.
-   [ ] Bisa jelaskan RAW.
-   [ ] Bisa jelaskan STAGING.
-   [ ] Bisa jelaskan INTERMEDIATE.
-   [ ] Bisa jelaskan MART.
-   [ ] Bisa jelaskan star schema.
-   [ ] Bisa jelaskan grain.
-   [ ] Bisa jelaskan idempotency.
-   [ ] Bisa jelaskan valid/rejected.
-   [ ] Bisa jelaskan checkpoint.

## Demo Scenario

-   [ ] Normal daily batch run.
-   [ ] Show GCS partition.
-   [ ] Show BigQuery RAW.
-   [ ] Show dbt models.
-   [ ] Show fact/dim.
-   [ ] Show dashboard.
-   [ ] Rerun tanggal sama.
-   [ ] Buktikan tidak duplicate.
-   [ ] Show rejected record.
-   [ ] Show streaming producer.
-   [ ] Show Pub/Sub.
-   [ ] Show stream masuk RAW.
-   [ ] Show stream muncul di final model.
-   [ ] Simulasikan failure jika aman.
-   [ ] Tunjukkan retry/check/alert jika sudah diimplementasikan.

## Presentation

-   [ ] Problem statement.
-   [ ] Business questions.
-   [ ] Dataset.
-   [ ] Architecture diagram.
-   [ ] Batch pipeline.
-   [ ] Streaming pipeline.
-   [ ] Transformation layers.
-   [ ] Star schema.
-   [ ] Data quality.
-   [ ] Idempotency.
-   [ ] Dashboard.
-   [ ] Key findings.
-   [ ] Challenges.
-   [ ] Lessons learned.
-   [ ] Future improvements.

### Exit Criteria

-   [ ] Demo dapat dilakukan end-to-end.
-   [ ] Bisa menjawab alasan di balik setiap teknologi.
-   [ ] Bisa menjelaskan failure/recovery.
-   [ ] Bisa menjelaskan trade-off.
-   [ ] Final Project siap dikumpulkan/presentasi.

**STATUS: TODO**

------------------------------------------------------------------------

# 4. MASTER PROGRESS TRACKER

Update bagian ini setiap selesai fase.

  Phase   Area                           Status
  ------- ------------------------------ --------
  0       Project Design                 LOCKED
  1       Dataset Profiling              TODO
  2       Project Foundation             TODO
  3       GCP Infrastructure             TODO
  4       Batch Pipeline                 TODO
  5       dbt Transformation             TODO
  6       Airflow Orchestration          TODO
  7       Streaming Replay               TODO
  8       Data Quality / Audit / Alert   TODO
  9       Dashboard                      TODO
  10      Documentation                  TODO
  11      GitHub Cleanup                 TODO
  12      Demo & Sidang                  TODO

Status yang digunakan:

``` text
TODO
IN PROGRESS
BLOCKED
DONE
LOCKED
REVISE
```

------------------------------------------------------------------------

# 5. CURRENT POSITION

Saat memulai guideline ini:

``` text
PHASE 0  ██████████  LOCKED
PHASE 1  ░░░░░░░░░░  NEXT
PHASE 2  ░░░░░░░░░░
PHASE 3  ░░░░░░░░░░
PHASE 4  ░░░░░░░░░░
PHASE 5  ░░░░░░░░░░
PHASE 6  ░░░░░░░░░░
PHASE 7  ░░░░░░░░░░
PHASE 8  ░░░░░░░░░░
PHASE 9  ░░░░░░░░░░
PHASE 10 ░░░░░░░░░░
PHASE 11 ░░░░░░░░░░
PHASE 12 ░░░░░░░░░░
```

**NEXT TARGET: PHASE 1 --- DATASET PROFILING**

------------------------------------------------------------------------

# 6. SCOPE GUARDRAIL

## Jangan dilakukan sebelum waktunya

-   [ ] Jangan membuat dashboard sebelum MART stabil.
-   [ ] Jangan mengorkestrasi script yang belum berhasil manual.
-   [ ] Jangan membuat streaming sebelum core batch + dbt stabil.
-   [ ] Jangan menambah Dataflow kecuali muncul requirement teknis yang
    nyata.
-   [ ] Jangan mengganti Pub/Sub dengan Kafka tanpa alasan requirement.
-   [ ] Jangan mengganti BigQuery.
-   [ ] Jangan mengganti GCS.
-   [ ] Jangan mengganti dbt.
-   [ ] Jangan mengganti Airflow.
-   [ ] Jangan menambah dimension/fact hanya supaya schema terlihat
    kompleks.
-   [ ] Jangan menghapus rejected records tanpa alasan.
-   [ ] Jangan membuat DQ rules berdasarkan feeling.
-   [ ] Jangan hardcode environment-specific configuration.
-   [ ] Jangan push credentials/secrets ke Git.
-   [ ] Jangan membuat dua pipeline berbeda untuk backfill dan daily
    processing jika logic yang sama dapat digunakan.

------------------------------------------------------------------------

# 7. DEFINITION OF DONE --- FINAL PROJECT

Project baru dianggap **DONE** jika:

-   [ ] Historical batch Jan--Jul berhasil diproses.
-   [ ] Batch berjalan dengan konsep daily processing.
-   [ ] Batch idempotent.
-   [ ] August replay streaming berhasil.
-   [ ] Pub/Sub berfungsi.
-   [ ] Batch dan streaming converge.
-   [ ] RAW tersedia.
-   [ ] STAGING tersedia.
-   [ ] INTERMEDIATE valid tersedia.
-   [ ] INTERMEDIATE rejected tersedia.
-   [ ] Star schema tersedia.
-   [ ] Analytical marts tersedia.
-   [ ] Airflow daily DAG berhasil.
-   [ ] Layer checkpoints tersedia.
-   [ ] Data quality tests berhasil.
-   [ ] Failure handling/retry tersedia.
-   [ ] Audit/alert requirement final terpenuhi.
-   [ ] Dashboard menjawab business questions.
-   [ ] README lengkap.
-   [ ] Architecture terdokumentasi.
-   [ ] Data dictionary tersedia.
-   [ ] Design decisions terdokumentasi.
-   [ ] GitHub aman dari secrets.
-   [ ] Fresh setup/reproducibility diuji.
-   [ ] Demo scenario diuji.
-   [ ] Q&A sidang dipersiapkan.
-   [ ] User memahami alasan engineering di balik project.

------------------------------------------------------------------------

# 8. WORKING PRINCIPLE

> **Build the simplest pipeline that correctly solves the problem,
> validate every layer, document every important decision, and only add
> complexity when the project actually needs it.**

Urutan utama yang tidak boleh hilang:

``` text
DESIGN
  ↓
PROFILE
  ↓
FOUNDATION
  ↓
INFRASTRUCTURE
  ↓
BATCH
  ↓
DBT
  ↓
AIRFLOW
  ↓
STREAMING
  ↓
QUALITY / AUDIT / ALERT
  ↓
DASHBOARD
  ↓
DOCUMENTATION
  ↓
CLEANUP
  ↓
DEMO & SIDANG
```

**Rule terakhir:** Jika selama pengerjaan muncul ide baru, jangan
langsung implementasikan. Bandingkan dulu dengan guideline ini dan catat
sebagai proposed change. Project hanya berubah jika perubahan tersebut
menyelesaikan masalah nyata atau memenuhi requirement yang sebelumnya
belum terpenuhi.
