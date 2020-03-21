from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "curtis",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 19),
    "schedule_interval": "@daily",
}

dag = DAG(
    "get_files_from_s3", default_args=default_args, catchup=True, max_active_runs=1
)


def check_prefix(ds_nodash, **kwargs):
    hook = S3Hook("aws_s3_lsu")
    prefix = f"bedpage/{ds_nodash}"
    check = hook.check_for_prefix("htprawscrapes", prefix, "/")
    if check == True:
        print(f"The prefix {prefix} exists!")
    else:
        raise Exception(f"The prefix {prefix} does not exists!")


def etl_files(ds_nodash, **kwargs):
    import json
    from hashlib import sha256
    from code_zero.bedpage import Bedpage

    conn = PostgresHook(postgres_conn_id="postgres_bedpage").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    hook = S3Hook("aws_s3_lsu")
    bucket_name = "htprawscrapes"
    prefix = f"bedpage/{ds_nodash}/"

    keys = hook.list_keys(bucket_name, prefix=prefix)

    print(f"There are {len(keys)} in {prefix}")

    for key in keys:
        s3_key = f"s3://{bucket_name}/{key}"
        data = hook.read_key(key, bucket_name="htprawscrapes")
        ad = Bedpage(data)
        x = vars(ad)
        del x['soup']
        sha = sha256(json.dumps(x, sort_keys=True).encode('utf-8')).hexdigest()

        try:
            cur.execute(
                """INSERT INTO ads (s3_key, sha256, ad_id, city, category, url, title, body, published_date, modified_date, phone, email, location, age)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    s3_key,
                    sha, 
                    x["ad_id"],
                    x["city"],
                    x["category"],
                    x["url"],
                    x["title"],
                    x["body"],
                    x["published_date"],
                    x["modified_date"],
                    x["phone"],
                    x["email"],
                    x["location"],
                    x["age"],
                ],
            )
            print(f"{s3_key} inserted into the database")
        except:
            print(f"Error inserting {s3_key} into the database")
            pass

    conn.close()


check_for_prefix = PythonOperator(
    task_id="check_prefix", python_callable=check_prefix, provide_context=True, dag=dag
)

etl_files_from_s3 = PythonOperator(
    task_id="etl_files", python_callable=etl_files, provide_context=True, dag=dag
)

check_for_prefix >> etl_files_from_s3
