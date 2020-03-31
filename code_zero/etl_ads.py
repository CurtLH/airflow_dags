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


raw_table_exists_query = """
  SELECT EXISTS (
     SELECT FROM information_schema.tables
     WHERE  table_schema = 'bedpage'
     AND    table_name   = 'raw2'
     );
"""

create_ads_table_query = """
  create table if not exists bedpage.ads2 (
        id numeric primary key,
        post_id varchar,
        date_published timestamp,
        date_modified timestamp,
        title varchar,
        body varchar,
        url varchar,
        city varchar,
        category varchar,
        poster_age varchar,
        email varchar,
        phone varchar,
        mobile varchar,
        location varchar
);
"""

insert_ads_query = """
insert
        into
        bedpage.ads2 (id,
        post_id,
        date_published,
        date_modified,
        title,
        body,
        url,
        city,
        category,
        poster_age,
        email,
        phone,
        mobile,
        location)
select
        id,
        ad -> 'details' ->> 'post id' as post_id,
        (ad -> 'details' ->> 'datePublished')::timestamp as date_published,
        (ad -> 'details' ->> 'dateModified')::timestamp as date_modified,
        ad ->> 'title' as title,
        ad ->> 'body' as body,
        ad -> 'details' ->> 'url' as url,
        substring(split_part(ad -> 'details' ->> 'url', '.', 1), 9) as city,
        split_part(ad -> 'details' ->> 'url', '/', 4) as category,
        ad -> 'details' ->> 'poster''s age' as poster_age,
        ad -> 'details' ->> 'email' as email,
        ad ->> 'phone' as phone,
        ad -> 'details' ->> 'mobile' as mobile,
        ad -> 'details' ->> 'location' as location
from
        bedpage.raw2
where
        not exists (
        select
                id
        from
                bedpage.ads2
        where
                id = bedpage.raw2.id );
"""

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
