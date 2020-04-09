import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

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

def extract_phone(text):

    # define punction to exclude from phone numbers
    punct = "!\"#%&'()*+,-./:;<=>?@[\\]^_`{|}~"

    # zap the text into lowercase
    text = text.lower()

    # remove all punctation
    text = "".join(l for l in text if l not in punct)

    # remove all spaces
    text = text.replace(" ", "")

    # create a dict of numeric words to replace with numbers
    numbers = {
        "zero": "0",
        "one": "1",
        "two": "2",
        "three": "3",
        "four": "4",
        "five": "5",
        "six": "6",
        "seven": "7",
        "eight": "8",
        "nine": "9",
    }

    # look for each number spelled out in the text, and if found, replace with the numeric alternative
    for num in numbers:
        if num in text:
            text = text.replace(num, numbers[num])

    # extract all number sequences
    numbers = re.findall(r"\d+", text)

    # filter number strings to only include unique strings longer that are betweeb 7 and 11 characters in length
    phones = list(set([i for i in numbers if len(i) >= 7 and len(i) <= 11]))

    # convert set to semicolon delimited
    if len(phones) > 0:
        return phones

    else:
        return []


def get_all_phones(items):
    
    all_phones = set()
    
    for i in items:
        if type(i) == str:
            p = extract_phone(i)
            all_phones.update(p)
        
    return list(all_phones)


def get_phones_from_ads():

    # connect to the database
    conn = PostgresHook(postgres_conn_id="lsu_aws_postgres").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # define the query to get the ads that have not been processed
    query = """
    SELECT id, body, title, mobile 
    FROM bedpage.ads
    WHERE id NOT IN (SELECT DISTINCT(id)
                     FROM bedpage.phones)
    """

    # execute the query
    cur.execute(query)

    # get the data from the query
    data = [line for line in cur]

    # iterate over each ad and extract the phone numbers
    all_phones = []
    for ad in data:
        phones = get_all_phones([ad[1], ad[2], ad[3]])
        if len(phones) > 0:
            for p in phones:
                all_phones.append((ad[0], p))
        else:
            all_phones.append((ad[0], None)) 

    # insert new records into the database
    for line in all_phones:
        try:
            cur.execute(
                """
                INSERT INTO bedpage.phones (id, phone)
                VALUES (%s, %s)
                """,
                [line[0], line[1]]
            )
            print("New phone number inserted")
        except:
            print(f"Error inserting: {line}")

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

create_phones_table = PostgresOperator(
    dag=dag,
    task_id="create_phones_table", 
    postgres_conn_id="lsu_aws_postgres",
    sql="/create_phones_table.sql"
)

etl_phones = PythonOperator(
    dag=dag,
    task_id="elt_phones",
    python_callable=etl_phones_from_ads
)

raw_table_exists >> etl_ads >> create_phones_table >> etl_phones
