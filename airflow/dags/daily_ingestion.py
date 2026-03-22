"""
Daily Stock Ingestion DAG
Schedule: 2 AM UTC every day
Pipeline: fetch_stocks → load_to_snowflake → dbt run → dbt test
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Scripts live at /opt/airflow/python inside the container (mounted volume)
PYTHON_DIR = "/opt/airflow/python"

default_args = {
    "owner": "stockforge",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
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
