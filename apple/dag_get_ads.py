from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

default_args = {
    "owner": "curtis",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 19)
    "schedule_interval": "@daily",
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}


dag = DAG("apple_get_ads", default_args=default_args)


def get_ads():

    conn = PostgresHook(postgres_conn_id="postgres_curtis", schema="curtis").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    urls = apple.get_urls()
    for url in urls:
        try:
            html = apple.get_html(url)
            cur.execute(
                """INSERT INTO apple_refurb_ads_raw (url, html)
                           VALUES (%s, %s)""",
                [url, r.text],
            )

        except:
            pass

    cur.close()
    conn.close()

create_table_query = \
    """
    CREATE TABLE IF NOT EXISTS apple_refurb_ads_raw
    (id SERIAL,
     date date default current_date,
     url varchar,
     html varchar)
    """

create_table = PostgresOperator(
    task_id = "create_table",
    sql = create_table_query, 
    postgres_conn_id = "postgres_curtis",
    dag = dag
)

get_ads = PythonOperator(
    task_id = "get_ads",
    python_callable = apple.get_ad_html,
    dag = dag
)

get_ads.set_upstream(create_table)
