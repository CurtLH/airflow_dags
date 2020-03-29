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
    "Testing_ETL_files_from_S3",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    schedule_interval="@daily"
)

def etl_files(ds_nodash, **kwargs):

    import os
    import glob
    import tarfile
    import boto3
    import json
    from hashlib import sha256
    from code_zero.bedpage import Bedpage

    conn = PostgresHook(postgres_conn_id="lsu_aws_postgres").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    s3 = boto3.client('s3')

    hook = S3Hook("lsu_asw_s3")
    bucket_name = "htprawscrapes"
    prefix = f"bedpage/{ds_nodash}/"

    # get the files in the prefox
    keys = hook.list_keys(bucket_name, prefix=prefix)
    print(f"There are {len(keys)} in {prefix}")

    # create a tmp directory
    tmp_dir = "/tmp/airflow/"
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)

    # iterate over each file
    for key in keys[:1]:

        # print the key
        print(key)
    
        # download file
        filename = key.split('/')[-1]
        s3.download_file(bucket_name, key, tmp_dir+filename)

        # create a tmp directory to hold the uncompressed files
        file_dir = tmp_dir + 'files'
        os.mkdir(file_dir)

        # uncompress file
        tf = tarfile.open(tmp_dir + filename)
        tf.extractall(path=file_dir)

        # get the path to each file
        files = glob.glob(os.path.join(file_dir, "*html"))
        print(f"There are {len(files)} files to process")

        # iterate over each file
        for f in files:

            print(f"Processing {f}")

            try:

                # read the file
                with open(f) as f:
                    data = f.read()
                print("File opened")

                # parse HTML
                ad = Bedpage(data)
                print("HTML parsed")

                # convert class to dict and delete source HTML from dict
                x = vars(ad)
                del x['soup']
                print("Converted to dictionary")

                # create sha256 of dict
                sha = sha256(json.dumps(x, sort_keys=True).encode('utf-8')).hexdigest()
                print("Create SHA")

                # insert row into database
                cur.execute(
                    """INSERT INTO bedpage.raw2 (s3_key, sha256, ad) VALUES (%s, %s, %s)""",
                    [key, sha, json.dumps(x)],
                
                )
                print(f"{f} inserted into the database")
            except:
                print(f"Error inserting {f} into the database")
                pass

        # delete tmp directory


etl_files = PythonOperator(
    task_id="etl_files", python_callable=etl_files, provide_context=True, dag=dag
)

etl_files
