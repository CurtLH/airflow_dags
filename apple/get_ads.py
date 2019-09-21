from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import apple

default_args = {
    "owner": "curtis",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 19),
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
}


dag = DAG(
    "get_ads", default_args=default_args, schedule_interval="0 14 * * *", catchup=False
)


def get_ads_html():

    conn = PostgresHook(postgres_conn_id="postgres_apple").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    urls = apple.get_urls()
    for url in urls:
        html = apple.get_html(url)
        cur.execute(
            """INSERT INTO apple_refurb_ads_raw (url, html)
                      VALUES (%s, %s)""",
            [url, html],
        )

    cur.close()
    conn.close()


create_table_query = """
    CREATE TABLE IF NOT EXISTS apple_refurb_ads_raw
    (id SERIAL,
     date date default current_date,
     url varchar,
     html varchar)
    """

create_table = PostgresOperator(
    task_id="create_table",
    sql=create_table_query,
    postgres_conn_id="postgres_apple",
    dag=dag,
)

get_ads = PythonOperator(task_id="get_ads", python_callable=get_ads_html, dag=dag, retries=5)

get_ads.set_upstream(create_table)
