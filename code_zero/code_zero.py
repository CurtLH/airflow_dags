from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "curtis",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 19)
}

dag = DAG(
    "ETL_files_from_S3",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    schedule_interval="@daily"
)


table_exists_query = """
  SELECT EXISTS (
     SELECT FROM information_schema.tables
     WHERE  table_schema = 'bedpage'
     AND    table_name   = 'raw'
     );
"""


def check_prefix(ds_nodash, **kwargs):
    hook = S3Hook("lsu_aws_s3")
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

    conn = PostgresHook(postgres_conn_id="lsu_aws_postgres").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    hook = S3Hook("lsu_asw_s3")
    bucket_name = "htprawscrapes"
    prefix = f"bedpage/{ds_nodash}/"

    keys = hook.list_keys(bucket_name, prefix=prefix)

    print(f"There are {len(keys)} in {prefix}")

    for key in keys[:10]:
        s3_key = f"s3://{bucket_name}/{key}"
        data = hook.read_key(key, bucket_name="htprawscrapes")

        try:

            ad = Bedpage(data)
            x = vars(ad)
            del x['soup']
            sha = sha256(json.dumps(x, sort_keys=True).encode('utf-8')).hexdigest()

            cur.execute(
                """INSERT INTO bedpage.raw (s3_key, sha256, ad) VALUES (%s, %s, %s)""",
                [s3_key, sha, json.dumps(x)],
                
            )
            print(f"{s3_key} inserted into the database")
        except:
            print(f"Error inserting {s3_key} into the database")
            pass

    conn.close()


table_exists = PostgresOperator(
    task_id="table_exists", sql=table_exists_query, postgres_conn_id="lsu_aws_postgres", dag=dag
)

prefix_exists = PythonOperator(
    task_id="prefix_exists", python_callable=check_prefix, provide_context=True, dag=dag
)

etl_files = PythonOperator(
    task_id="etl_files", python_callable=etl_files, provide_context=True, dag=dag
)

table_exists >> prefix_exists >> etl_files
