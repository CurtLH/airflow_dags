from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs
import apple
import logging

default_args = {
    "owner": "curtis",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 19),
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
}


dag = DAG(
    "etl", default_args=default_args, schedule_interval="5 14 * * *", catchup=False
)

create_table_query = """
    CREATE TABLE IF NOT EXISTS ads
    (id SERIAL,
     datetime date default current_date,
     url varchar,
     id_num varchar,
     price int,
     date varchar,
     screen varchar,
     color varchar,
     PRIMARY KEY (id_num, price)
     )
    """


def etl():

    conn = PostgresHook(postgres_conn_id="postgres_apple").get_conn()
    src_cur = conn.cursor()
    dest_cur = conn.cursor()

    query = """
    SELECT *
    FROM raw
    """

    src_cur.execute(query)

    while True:
        records = src_cur.fetchone()
        if not records:
            break

        row = []
        url = records[2]
        soup = bs(records[3], "html.parser")
        row.append(
            [
                url,
                apple.get_id_num(url),
                apple.get_price(soup),
                apple.get_date(soup),
                apple.get_screen(soup),
                apple.get_color(url),
            ]
        )

        try:
            dest_cur.execute(
                """INSERT INTO ads (url, id_num, price, date, screen, color)
                                VALUES (%s, %s, %s, %s, %s, %s)""",
                [i for i in row[0]],
            )
            conn.commit()
            logging.info("New record inserted")
        except BaseException:
            pass
            logging.info("Duplicate records")

    src_cur.close()
    dest_cur.close()
    conn.close()


create_table = PostgresOperator(
    task_id="create_table",
    sql=create_table_query,
    postgres_conn_id="postgres_apple",
    dag=dag,
)

etl_ads = PythonOperator(task_id="etl_ads", python_callable=etl, dag=dag)

etl_ads.set_upstream(create_table)
