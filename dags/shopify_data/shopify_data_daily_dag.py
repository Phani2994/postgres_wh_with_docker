
import os
from datetime import timedelta

from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator

from shopify_data import prog

POSTGRES_USER = os.environ.get(
    "POSTGRES_USER", "algolia_user"
)
POSTGRES_DB = os.environ.get(
    "POSTGRES_DB", "algolia_wh"
)
POSTGRES_PW = os.environ.get(
    "POSTGRES_PW", "algolia_pwd"
)


default_args = {
    "owner": "data_team",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    # "on_failure_callback": failed_task_slack_notification,
}

with models.DAG(
    f"shopify_data_daily",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    tags=[
        "daily_job",
        "shopify"
    ],
    max_active_runs=1,  # for dags
) as dag:

    update_shopify_data = PythonOperator(
        task_id="update_shopify_data",
        python_callable=prog.main,
        op_args=[POSTGRES_USER, POSTGRES_PW, POSTGRES_DB],
    )

    (
        update_shopify_data
    )
