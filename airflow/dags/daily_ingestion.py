"""
Daily Stock Ingestion DAG
Schedule: 2 AM UTC every day
Pipeline: fetch_stocks → load_to_snowflake → dbt run → dbt test
"""

import os
import urllib.request
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Scripts live at /opt/airflow/python inside the container (mounted volume)
PYTHON_DIR = "/opt/airflow/python"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def send_slack_alert(context):
    """Send a Slack notification when any task fails."""
    if not SLACK_WEBHOOK_URL:
        return

    dag_id   = context.get("dag").dag_id
    task_id  = context.get("task_instance").task_id
    log_url  = context.get("task_instance").log_url
    exec_dt  = context.get("execution_date")

    message = {
        "text": (
            f":red_circle: *StockForge pipeline failed*\n"
            f"*DAG*: `{dag_id}`\n"
            f"*Task*: `{task_id}`\n"
            f"*Time*: `{exec_dt}`\n"
            f"*Logs*: {log_url}"
        )
    }

    data = json.dumps(message).encode("utf-8")
    req  = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req)


default_args = {
    "owner": "stockforge",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": send_slack_alert,
}

with DAG(
    dag_id="daily_stock_ingestion",
    description="Fetch stock data, load to Snowflake, run dbt transformations",
    schedule_interval="0 2 * * *",   # 2 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["stockforge", "ingestion"],
) as dag:

    fetch_stocks = BashOperator(
        task_id="fetch_stocks",
        bash_command=f"cd {PYTHON_DIR} && python fetch_stocks.py",
    )

    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command=f"cd {PYTHON_DIR} && python load_to_snowflake.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/stockforge && dbt run --profiles-dir /opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/stockforge && dbt test --profiles-dir /opt/airflow/dbt",
    )

    # Pipeline order
    fetch_stocks >> load_to_snowflake >> dbt_run >> dbt_test
