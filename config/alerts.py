import os
from datetime import datetime
import pendulum
import requests

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
LOCAL_TZ = pendulum.timezone("Asia/Jakarta")

def send_slack(message):
    """Kirim message ke Slack."""

    # 1. Pastikan Slack webhook sudah dikonfigurasi.
    if not SLACK_WEBHOOK_URL:
        raise ValueError("SLACK_WEBHOOK_URL belum dikonfigurasi.")

    # 2. Kirim message ke Slack melalui webhook.
    response = requests.post(
        SLACK_WEBHOOK_URL,
        json={"text": message},
        timeout=10,
    )

    # 3. Pastikan request ke Slack berhasil.
    response.raise_for_status()

def slack_failure_alert(context):
    """Kirim alert ketika task Airflow gagal."""

    # 1. Ambil informasi task yang gagal dari Airflow context.
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    exception = context.get("exception")
    log_url = context["task_instance"].log_url

    # 2. Siapkan detail error dan waktu kejadian.
    error_message = str(exception) if exception else "Unknown error"
    current_time = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S WIB")

    # 3. Susun message failure untuk Slack.
    message = (
        "*Citi Bike Pipeline FAILED*\n\n"
        f"*DAG*    : `{dag_id}`\n"
        f"*Task*   : `{task_id}`\n"
        f"*Run ID* : `{run_id}`\n"
        f"*Time*   : `{current_time}`\n"
        f"*Error*  : `{error_message}`\n"
        f"*Log*    : {log_url}"
    )

    send_slack(message)

def slack_success_alert(context):
    """Kirim alert ketika seluruh DAG berhasil."""

    # 1. Ambil informasi DAG dari Airflow context.
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    current_time = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S WIB")

    # 2. Siapkan URL Airflow untuk melihat DAG run.
    airflow_url = f"http://localhost:8085/dags/{dag_id}/grid"

    # 3. Susun message success untuk Slack.
    message = (
        "*Citi Bike Pipeline SUCCESS*\n\n"
        f"*DAG*    : `{dag_id}`\n"
        f"*Run ID* : `{run_id}`\n"
        f"*Time*   : `{current_time}`\n"
        "*Status* : `All pipeline tasks completed successfully`\n"
        f"*Airflow*: <{airflow_url}|View DAG Run>"
    )

    send_slack(message)

def slack_streaming_failure_alert(context):
    """Kirim alert ketika monitoring streaming gagal."""

    # 1. Ambil informasi task yang gagal dari Airflow context.
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exception = context.get("exception")
    log_url = context["task_instance"].log_url

    error_message = str(exception) if exception else "Unknown error"
    current_time = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S WIB")

    message = (
        "*Citi Bike Streaming ALERT*\n\n"
        f"*DAG*      : `{dag_id}`\n"
        f"*Check*    : `{task_id}`\n"
        f"*Time*     : `{current_time}`\n"
        f"*Status*   : `FAILED`\n"
        f"*Error*    : `{error_message}`\n"
        "*Interval* : `Checked every 5 minutes`\n"
        f"*Log*      : {log_url}"
    )

    send_slack(message)