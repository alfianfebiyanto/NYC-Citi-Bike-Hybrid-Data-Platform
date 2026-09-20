import pendulum
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from config.alerts import slack_failure_alert
from quality.quality_checks import (
    check_gbfs_streaming,
    check_gbfs_dlq,
)

WIB = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "alfian",
    "depends_on_past": False,
    "retries": 0,
    "on_failure_callback": slack_failure_alert,
}

with DAG(
    dag_id="citibike_realtime_refresh",
    description="Refresh realtime Citi Bike station status dari RAW hingga analytics.",
    default_args=default_args,
    start_date=datetime(2026, 9, 18, tzinfo=WIB),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["citibike", "realtime", "bigquery", "dbt"],
) as dag:
    
    # 1. Tandai awal realtime refresh pipeline.
    start = EmptyOperator(task_id="start")

    # 2. Validasi freshness dan monitor DLQ sebelum transformasi.
    with TaskGroup(group_id="streaming_quality") as streaming_quality:

        check_freshness = PythonOperator(
            task_id="check_freshness",
            python_callable=check_gbfs_streaming,
        )

        check_dlq = PythonOperator(
            task_id="check_dlq",
            python_callable=check_gbfs_dlq,
        )

        check_freshness >> check_dlq

    # 3. Transformasi RAW station status ke staging.
    with TaskGroup(group_id="staging") as staging:

        build_staging = BashOperator(
            task_id="build_station_status",
            bash_command="""
                cd ${DBT_PROJECT_DIR} &&
                dbt run --select stg_station_status
            """,
        )

    # 4. Validasi dan transformasi staging ke intermediate.
    with TaskGroup(group_id="intermediate") as intermediate:

        build_intermediate = BashOperator(
            task_id="build_station_status",
            bash_command="""
                cd ${DBT_PROJECT_DIR} &&
                dbt run --select int_station_status
            """,
        )

    # 5. Bangun realtime fact table pada warehouse layer.
    with TaskGroup(group_id="warehouse") as warehouse:

        build_warehouse = BashOperator(
            task_id="build_fact_station_status",
            bash_command="""
                cd ${DBT_PROJECT_DIR} &&
                dbt run --select fact_station_status
            """,
        )

    # 6. Refresh analytics mart untuk station status terbaru.
    with TaskGroup(group_id="analytics") as analytics:

        build_analytics = BashOperator(
            task_id="build_station_live",
            bash_command="""
                cd ${DBT_PROJECT_DIR} &&
                dbt run --select mart_station_live
            """,
        )

    completed = EmptyOperator(task_id="completed")

    start >> streaming_quality >> staging >> intermediate >> warehouse >> analytics >> completed