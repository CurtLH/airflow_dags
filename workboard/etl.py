from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import requests
import json

default_args = {
    "owner": "curtis",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 26),
    "retries": 0,
    "retry_delay": timedelta(seconds=60),
}

dag = DAG(
    "workboard_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

create_users_query = """
    CREATE TABLE users AS
      SELECT
        user_id,
        user_data ->> 'email' AS email,
       (user_data ->> 'org_id')::integer AS org_id,
        user_data ->> 'status' AS status,
        TO_TIMESTAMP((user_data ->> 'create_at')::integer) AS create_at,
        user_data ->> 'external_id' AS external_id,
        user_data ->> 'created_from' AS created_from,
        TO_TIMESTAMP((user_data ->> 'last_visited_at')::integer) AS last_visited_at
      FROM
        users_raw;

    ALTER TABLE users
    ADD PRIMARY KEY (user_id),
    ADD FOREIGN KEY (user_id)
    REFERENCES users_raw(user_id);
"""

create_teams_query = """
    CREATE TABLE teams AS
    SELECT
      team_id,
      team_data ->> 'team_name' AS team_name,
      team_data ->> 'team_type' AS team_type,
      TO_TIMESTAMP((team_data ->> 'created_at')::integer) AS created_at,
     (team_data ->> 'team_owner')::integer AS team_owner,
      TO_TIMESTAMP((team_data ->> 'updated_at')::integer) AS updated_at,
      team_data ->> 'external_id' AS external_id,
      team_data ->> 'created_from' AS created_from,
      team_data ->> 'parent_team_id' AS parent_team_id
    FROM teams_raw;

    ALTER TABLE teams
    ADD PRIMARY KEY (team_id),
    ADD FOREIGN KEY (team_id)
        REFERENCES teams_raw(team_id);
"""

create_team_members_query = """
    CREATE TABLE team_members AS
    SELECT
        team_id,
        id,
        email,
        last_name,
        team_role,
        first_name
    FROM
        team_members_raw,
        JSONB_TO_RECORDSET(team_member_data -> 'team_members')
            AS elems(
                id integer,
                email varchar,
                last_name varchar,
                team_role varchar,
                first_name varchar
            );
"""

create_goals_query = """
    CREATE TABLE goals AS
      SELECT
        (goal_data ->> 'goal_id')::integer AS goal_id,
         goal_data ->> 'goal_name' as goal_name,
         goal_data -> 'goal_type' ->> 'name' AS goal_type_name,
        (goal_data ->> 'goal_owner')::integer AS goal_owner,
        (goal_data ->> 'goal_status')::integer AS goal_status,
        (goal_data ->> 'goal_team_id')::integer AS goal_team_id,
         goal_data ->> 'goal_progress' AS goal_progress,
         TO_TIMESTAMP((goal_data ->> 'goal_create_at')::integer) AS goal_create_at,
         goal_data ->> 'goal_narrative' AS goal_narrative,
         goal_data ->> 'goal_team_name' AS goal_team_name,
        (goal_data ->> 'goal_created_by')::integer AS goal_created_by,
         goal_data ->> 'goal_permission' AS goal_permission,
         TO_TIMESTAMP((goal_data ->> 'goal_start_date')::integer) AS goal_start_date,
        (goal_data ->> 'goal_updated_by')::integer AS goal_updated_by,
         TO_TIMESTAMP((goal_data ->> 'goal_modified_at')::integer) AS goal_modified_at,
         TO_TIMESTAMP((goal_data ->> 'goal_target_date')::integer) AS goal_target_date,
         goal_data ->> 'goal_owner_full_name' AS goal_owner_full_name
      FROM goals_raw;

    ALTER TABLE goals
    ADD PRIMARY KEY (goal_id),
    ADD FOREIGN KEY (goal_id)
        REFERENCES goals_raw(goal_id);
"""

create_metrics_query = """
    CREATE TABLE metrics AS
      SELECT
        goal_id,
        metric_id,
        metric_name,
        metric_unit ->> 'name' AS metric_unit,
        metric_owner,
        metric_target,
        TO_TIMESTAMP(metric_create_at) AS metric_create_at,
        metric_created_by,
        metric_updated_by,
        TO_TIMESTAMP(metric_last_update) AS metric_last_update,
        TO_TIMESTAMP(metric_next_update) AS metric_next_update,
        metric_source_from,
        metric_initial_data,
        metric_progress_type ->> 'name' AS metric_progress_type,
        metric_achieve_target,
        metric_update_interval ->> 'name' AS metric_update_interval,
        metric_update_interval_day
      FROM
        goals_raw,
        JSONB_TO_RECORDSET(goal_data -> 'goal_metrics')
          AS elems(
            metric_id integer,
            metric_name varchar,
            metric_unit jsonb,
            metric_owner integer,
            metric_target numeric,
            metric_create_at integer,
            metric_created_by integer,
            metric_updated_by integer,
            metric_last_update integer,
            metric_next_update integer,
            metric_source_from varchar,
            metric_initial_data numeric,
            metric_progress_type jsonb,
            metric_achieve_target numeric,
            metric_update_interval jsonb,
            metric_update_interval_day varchar
        );

    ALTER TABLE metrics
    ADD PRIMARY KEY (metric_id),
    ADD FOREIGN KEY (goal_id)
        REFERENCES goals_raw(goal_id);
"""

drop_tables_query = """
    DROP TABLE IF EXISTS metrics;
    DROP TABLE IF EXISTS goals;
    DROP TABLE IF EXISTS teams;
    DROP TABLE IF EXISTS team_members;
    DROP TABLE IF EXISTS users;
"""

add_constraints_query = """
    ALTER TABLE goals
    ADD FOREIGN KEY (goal_owner)
    REFERENCES users(user_id);

    ALTER TABLE goals
    ADD FOREIGN KEY (goal_team_id)
    REFERENCES teams(team_id);

    ALTER TABLE metrics
    ADD FOREIGN KEY (goal_id)
    REFERENCES goals(goal_id);
"""

drop_tables = PostgresOperator(
    task_id="drop_tables",
    sql=drop_tables_query,
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

create_users = PostgresOperator(
    task_id="create_users",
    sql=create_users_query,
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

create_teams = PostgresOperator(
    task_id="create_teams",
    sql=create_teams_query,
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

create_team_members = PostgresOperator(
    task_id="create_team_members",
    sql=create_team_members_query,
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

create_goals = PostgresOperator(
    task_id="create_goals",
    sql=create_goals_query,
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

create_metrics = PostgresOperator(
    task_id="create_metrics",
    sql=create_metrics_query,
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

add_constraints = PostgresOperator(
    task_id="add_constraints",
    sql=add_constraints_query,
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

# set upstream tasks
drop_tables >> [
    create_users,
    create_teams,
    create_team_members,
    create_goals,
    create_metrics,
] >> add_constraints
