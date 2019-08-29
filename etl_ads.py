from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from psycopg2 import sql
from bs4 import BeautifulSoup as bs
from collections import defaultdict

default_args = {"owner": "curtis", "start_date": datetime(2019, 7, 19)}
dag = DAG("etl_ads", default_args=default_args, schedule_interval="@daily")


def get_specs(soup):

    specs = defaultdict(list)
    section = soup.find(
        "div", {"class": "as-productinfosection-panel TechSpecs-panel row"}
    )
    for cat in section.select(".h3-para-title"):
        k = cat.text.strip()
        for item in cat.find_next_siblings():
            if item.name != "div":
                break
            else:
                specs[k.lower()].append(item.text.strip().lower())

    return dict(specs)


def get_price(soup):

    price = soup.find(
        "div",
        {"class": "as-price-currentprice as-pdp-currentprice as-pdp-refurbishedprice"},
    )
    price = price.findAll("span")[1]
    price = price.getText().replace("\n", "").strip()
    price = price.replace("$", "").replace(",", "")
    price = float(price)

    return price


def get_date(soup):

    specs = soup.find(
        "div", {"class": "as-productinfosection-mainpanel column large-9 small-12"}
    )
    for tag in specs.findAll("p"):
        parsed = tag.getText()
        if "released" in parsed:
            date = parsed.replace("\n", "").strip().lower()
            break
        else:
            date = ""

    return date


def get_screen(soup):

    specs = soup.find(
        "div", {"class": "as-productinfosection-mainpanel column large-9 small-12"}
    )
    for tag in specs.findAll("p"):
        parsed = tag.getText()
        if "-inch" in parsed.lower() and not parsed.startswith("http"):
            screen = parsed.replace("\n", "").strip().lower()
            break
        else:
            screen = ""

    return screen.strip().lower()


def get_color(url):

    if "space-gray" in url.lower():
        return "space-gray"
    elif "silver" in url.lower():
        return "silver"
    else:
        return "N/A"


def get_id_num(url):

    return url.split("/")[5].lower()


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
                    get_id_num(url),
                    get_price(soup),
                    get_date(soup),
                    get_screen(soup),
                    get_color(url),
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
