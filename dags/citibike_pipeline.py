# Airflow Operator
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import pendulum
from datetime import datetime, timedelta

# Batch 
from batch.ingest_station_info import main as ingest_station_info
from batch.extract_data import main as extract_data
from batch.upload_to_gcs import upload_to_gcs
from batch.load_to_bigquery import load_to_bigquery

# Alert
from config.alerts import slack_failure_alert, slack_success_alert
from config.settings import AIRFLOW_DAG_ID, AIRFLOW_TIMEZONE

# Quality Checking
from quality.quality_checks import (
    check_staging,
    check_intermediate,
    check_mart,
    check_analytics,
)

local_tz = pendulum.timezone(AIRFLOW_TIMEZONE)

# Dag Config
default_args = {
    "owner": "alfian",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": slack_failure_alert,
}

with DAG(
    dag_id=AIRFLOW_DAG_ID,
    description="NYC Citi Bike daily batch data pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    schedule="0 8 * * *", # Setiap jam 8 Pagi
    catchup=False,
    max_active_runs=1,
    on_success_callback=slack_success_alert,
    tags=["citibike", "batch", "bigquery", "dbt"],
) as dag:

    # 1. Tandai awal batch pipeline.
    start = EmptyOperator(task_id="start")

    # ========================================================
    # INGESTION
    # Historical trip dan station information berjalan paralel.
    # ========================================================

    with TaskGroup(group_id="ingestion") as ingestion:

        with TaskGroup(group_id="historical") as historical:
            extract = PythonOperator(
                task_id="extract_data",
                python_callable=extract_data,
            )

            upload = PythonOperator(
                task_id="upload_to_gcs",
                python_callable=upload_to_gcs,
            )

            load = PythonOperator(
                task_id="load_to_bigquery",
                python_callable=load_to_bigquery,
            )

            extract >> upload >> load

        station_info = PythonOperator(
            task_id="ingest_station_information",
            python_callable=ingest_station_info,
        )

    # ========================================================
    # STAGING
    # Standardisasi data RAW dan validasi layer staging.
    # ========================================================

    with TaskGroup(group_id="staging") as staging:
        build_staging = BashOperator(
            task_id="build_staging",
            bash_command=(
                "cd ${DBT_PROJECT_DIR} && "
                "dbt build --select "
                "stg_citibike_trips "
                "stg_station_information"
            ),
        )

        check_staging_quality = PythonOperator(
            task_id="check_staging",
            python_callable=check_staging,
            op_kwargs={
                "run_id": "{{ run_id }}",
                "dag_id": "{{ dag.dag_id }}",
            },
        )

        build_staging >> check_staging_quality

    # ========================================================
    # INTERMEDIATE
    # Business cleaning dan validasi layer intermediate.
    # ========================================================

    with TaskGroup(group_id="intermediate") as intermediate:
        build_intermediate = BashOperator(
            task_id="build_intermediate",
            bash_command=(
                "cd ${DBT_PROJECT_DIR} && "
                "dbt build --select "
                "int_citibike_trips_clean "
                "int_citibike_trips_invalid "
                "int_station_information_clean"
            ),
        )

        check_intermediate_quality = PythonOperator(
            task_id="check_intermediate",
            python_callable=check_intermediate,
            op_kwargs={
                "run_id": "{{ run_id }}",
                "dag_id": "{{ dag.dag_id }}",
            },
        )

        build_intermediate >> check_intermediate_quality

    # ========================================================
    # WAREHOUSE
    # Membentuk star schema dan validasi warehouse.
    # ========================================================

    with TaskGroup(group_id="warehouse") as warehouse:
        build_warehouse = BashOperator(
            task_id="build_warehouse",
            bash_command=(
                "cd ${DBT_PROJECT_DIR} && "
                "dbt build --select "
                "dim_date "
                "dim_station "
                "dim_rider_type "
                "dim_bike_type "
                "fact_trips"
            ),
        )

        check_warehouse_quality = PythonOperator(
            task_id="check_warehouse",
            python_callable=check_mart,
            op_kwargs={
                "run_id": "{{ run_id }}",
                "dag_id": "{{ dag.dag_id }}",
            },
        )

        build_warehouse >> check_warehouse_quality

    # ========================================================
    # ANALYTICS
    # Membentuk serving marts dan validasi analytics.
    # ========================================================

    with TaskGroup(group_id="analytics") as analytics:
        build_analytics = BashOperator(
            task_id="build_analytics",
            bash_command=(
                "cd ${DBT_PROJECT_DIR} && "
                "dbt build --select "
                "mart_daily_rides "
                "mart_hourly_rides "
                "mart_station_performance "
            ),
        )

        check_analytics_quality = PythonOperator(
            task_id="check_analytics",
            python_callable=check_analytics,
            op_kwargs={
                "run_id": "{{ run_id }}",
                "dag_id": "{{ dag.dag_id }}",
            },
        )

        build_analytics >> check_analytics_quality

    completed = EmptyOperator(task_id="completed")

    start >> ingestion >> staging >> intermediate >> warehouse >> analytics >> completed