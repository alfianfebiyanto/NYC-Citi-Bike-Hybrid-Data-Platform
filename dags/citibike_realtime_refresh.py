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
    check_streaming_staging,
    check_streaming_intermediate,
    check_streaming_warehouse,
    check_streaming_analytics,
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

    # 2. Validasi RAW streaming dan monitor DLQ.
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
            task_id="build_staging",
            bash_command="""
                cd ${DBT_PROJECT_DIR} &&
                dbt run --select stg_station_status
            """,
        )

        quality_staging = PythonOperator(
            task_id="quality_staging",
            python_callable=check_streaming_staging,
            op_kwargs={
                "run_id": "{{ run_id }}",
                "dag_id": "{{ dag.dag_id }}",
            },
        )

        build_staging >> quality_staging

    # 4. Transformasi staging ke intermediate.
    with TaskGroup(group_id="intermediate") as intermediate:

        build_intermediate = BashOperator(
            task_id="build_intermediate",
            bash_command="""
                cd ${DBT_PROJECT_DIR} &&
                dbt run --select int_station_status
            """,
        )

        quality_intermediate = PythonOperator(
            task_id="quality_intermediate",
            python_callable=check_streaming_intermediate,
            op_kwargs={
                "run_id": "{{ run_id }}",
                "dag_id": "{{ dag.dag_id }}",
            },
        )

        build_intermediate >> quality_intermediate

    # 5. Bangun realtime fact table pada warehouse layer.
    with TaskGroup(group_id="warehouse") as warehouse:

        build_warehouse = BashOperator(
            task_id="build_warehouse",
            bash_command="""
                cd ${DBT_PROJECT_DIR} &&
                dbt run --select fact_station_status
            """,
        )

        quality_warehouse = PythonOperator(
            task_id="quality_warehouse",
            python_callable=check_streaming_warehouse,
            op_kwargs={
                "run_id": "{{ run_id }}",
                "dag_id": "{{ dag.dag_id }}",
            },
        )

        build_warehouse >> quality_warehouse

    # 6. Refresh analytics mart untuk station status terbaru.
    with TaskGroup(group_id="analytics") as analytics:

        build_analytics = BashOperator(
            task_id="build_mart",
            bash_command="""
                cd ${DBT_PROJECT_DIR} &&
                dbt run --select mart_station_live
            """,
        )

        quality_analytics = PythonOperator(
            task_id="quality_mart",
            python_callable=check_streaming_analytics,
            op_kwargs={
                "run_id": "{{ run_id }}",
                "dag_id": "{{ dag.dag_id }}",
            },
        )

        build_analytics >> quality_analytics

    # 7. Tandai realtime refresh selesai.
    completed = EmptyOperator(task_id="completed")

    start >> streaming_quality >> staging >> intermediate >> warehouse >> analytics >> completed