from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    "owner": "curtis",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 20)
}

dag = DAG(
    "ETL_ads",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    schedule_interval="@daily"
)

raw_table_exists = PostgresOperator(
    task_id="raw_table_exists", 
    sql=raw_table_exists_query, 
    postgres_conn_id="lsu_aws_postgres", 
    dag=dag
)

create_ads_table = PostgresOperator(
    task_id="create_ads_table", 
    sql=create_ads_query, 
    postgres_conn_id="lsu_aws_postgres", 
    dag=dag
)

insert_ads = PostgresOperator(
    task_id="insert_ads", 
    sql=insert_ads_query, 
    postgres_conn_id="lsu_aws_postgres", 
    dag=dag
)

raw_table_exists >> create_ads_table >> insert_ads
