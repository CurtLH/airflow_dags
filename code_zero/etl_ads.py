from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

# define the path to the template files
tmpl_search_path = "/home/curtis/github/airflow_dags/code_zero/sql"

default_args = {
    "owner": "curtis",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 20)
}

dag = DAG(
    "etl_raw_to_ads",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    schedule_interval="@daily",
    template_searchpath=tmpl_search_path
)

raw_table_exists = PostgresOperator(
    dag=dag,
    task_id="raw_table_exists", 
    postgres_conn_id="lsu_aws_postgres",
    sql="/raw_table_exists.sql"
)

etl_ads = PostgresOperator(
    dag=dag,
    task_id="etl_ads", 
    postgres_conn_id="lsu_aws_postgres",
    sql="/etl_ads.sql"
)

raw_table_exists >> etl_ads
