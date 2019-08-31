from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from psycopg2 import sql
from bs4 import BeautifulSoup as bs
import apple

default_args = {"owner": "curtis", "start_date": datetime(2019, 7, 19)}
dag = DAG("etl_ads", default_args=default_args, schedule_interval="@daily")

create_table_query = """
    CREATE TABLE IF NOT EXISTS apple_refurb_ads
    (id SERIAL,
     datetime date default current_date,
     url varchar,
     id_num varchar,
     price int,
     date varchar,
     screen varchar,
     color varchar)
    """


def etl(ds, **kwargs):

    query = """
    SELECT *
    FROM apple_refurb_ads_raw
    """

    src_conn = PostgresHook(
        postgres_conn_id="postgres_curtis", schema="curtis"
    ).get_conn()
    dest_conn = PostgresHook(
        postgres_conn_id="postgres_curtis", schema="curtis"
    ).get_conn()

    src_cur = src_conn.cursor("serverCursor")
    src_cur.execute(query)
    dest_cur = dest_conn.cursor()

    while True:
        records = src_cur.fetchmany(size=100)
        data = []
        for line in records:
            url = line[2]
            soup = bs(line[3], "html.parser")
            data.append(
                [
                    url,
                    apple.get_id_num(url),
                    apple.get_price(soup),
                    apple.get_date(soup),
                    apple.get_screen(soup),
                    apple.get_color(url),
                ]
            )

        if not records:
            break

        execute_values(
            dest_cur,
            "INSERT INTO apple_refurb_ads (url, id_num, price, date, screen, color) VALUES %s",
            data,
        )
        dest_conn.commit()

    src_cur.close()
    dest_cur.close()
    src_conn.close()
    dest_conn.close()


create_table = PostgresOperator(
    task_id="create_table",
    sql=create_table_query,
    postgres_conn_id="postgres_curtis",
    dag=dag,
)

etl_ads = PythonOperator(
    task_id="etl_ads", provide_context=True, python_callable=etl, dag=dag
)

etl_ads.set_upstream(create_table)
