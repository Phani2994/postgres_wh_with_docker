
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

create_table_sql = """CREATE TABLE shopify_data_temp
    (
	id text NULL,
	shop_domain text NULL,
	application_id text NULL,
	autocomplete_enabled bool NULL,
	user_created_at_least_one_qr bool NULL,
	nbr_merchandised_queries int8 NULL,
	nbrs_pinned_items text NULL,
	showing_logo bool NULL,
	has_changed_sort_orders bool NULL,
	analytics_enabled bool NULL,
	use_metafields bool NULL,
	nbr_metafields float8 NULL,
	use_default_colors bool NULL,
	show_products bool NULL,
	instant_search_enabled bool NULL,
	instant_search_enabled_on_collection bool NULL,
	only_using_faceting_on_collection bool NULL,
	use_merchandising_for_collection bool NULL,
	index_prefix text NULL,
	indexing_paused bool NULL,
	install_channel text NULL,
	export_date text NULL,
	has_specific_prefix text NULL
    );
"""

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

    # TS_NODASH = "{{ ts_nodash }}"

    update_shopify_data = PythonOperator(
        task_id="update_shopify_data",
        python_callable=prog.main,
        op_args=[POSTGRES_USER, POSTGRES_PW, POSTGRES_DB],
    )
    # create_table = PostgresOperator(
    #     task_id="create_table",
    #     postgres_conn_id="postgres",
    #     sql=create_table_sql,
    #     params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
    # )

    # task to save the processed data csv file

    # remove_prefix_object_list = PythonOperator(
    #     task_id="remove_prefix_on_object_list",
    #     python_callable=strip_prefix_file,
    #     op_kwargs={
    #         "prefix": f"{TS_NODASH}/",
    #         "files_list": GCS_Files_objectIds_from_temp_bucket.output,
    #     },
    # )

    # compare_object_list_main_and_temp_bucket = PythonOperator(
    #     task_id="compare_object_list_main_and_temp_bucket",
    #     python_callable=compare_list,
    #     op_kwargs={
    #         "soft_fail_if_more_elements_in_first_list": False,
    #         "soft_fail_if_more_elements_in_second_list": True,
    #     },
    #     templates_dict={
    #         "first_list": '{{ task_instance.xcom_pull(task_ids="GCS_Files_objectIds", key="return_value") }}',
    #         "second_list": remove_prefix_object_list.output,
    #     },
    # )

    (
   
           update_shopify_data
        # create_table
        # >> remove_prefix_object_list
        # >> compare_object_list_main_and_temp_bucket
    )
