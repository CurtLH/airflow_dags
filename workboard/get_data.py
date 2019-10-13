from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import requests
import json

default_args = {
    "owner": "curtis",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 26),
    "retries": 3,
    "retry_delay": timedelta(seconds=60),
}

dag = DAG(
    "workboard_get_data",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)


def get_user_data():

    conn = PostgresHook(postgres_conn_id="postgres_workboard").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    params = {"token": Variable.get("workboard_token")}
    base_url = "https://www.myworkboard.com/wb/apis"

    r = requests.get(base_url + "/user?include=org_members", params=params)
    if r.status_code != 200:
        raise ValueError(
            "Request not successful. Status code {}".format(
                r.status_code))
    else:
        users = r.json()["data"]["user"]
        logging.info("There are {} users".format(len(users)))
        for u in users:
            try:
                cur.execute(
                    """INSERT INTO users_raw (user_id, user_data)
                        VALUES (%s, %s) ON CONFLICT (user_id)
                        DO UPDATE SET user_data = EXCLUDED.user_data
                    """,
                    [u["user_id"], json.dumps(u)],
                )
                logging.info(
                    "User id #{} inserted into the table".format(
                        u["user_id"]))
            except BaseException:
                logging.warning("Records skipped")
                pass

    cur.close()
    conn.close()


def get_team_data():

    conn = PostgresHook(postgres_conn_id="postgres_workboard").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    params = {"token": Variable.get("workboard_token")}
    base_url = "https://www.myworkboard.com/wb/apis"

    r = requests.get(base_url + "/team?include=org_teams", params=params)
    if r.status_code != 200:
        raise ValueError(
            "Request not successful. Status code {}".format(
                r.status_code))
    else:
        teams = r.json()["data"]["team"]
        logging.info("There are {} teams".format(len(teams)))
        for t in teams:
            try:
                cur.execute(
                    """INSERT INTO teams_raw (team_id, team_data)
                        VALUES (%s, %s) ON CONFLICT (team_id)
                        DO UPDATE SET team_data = EXCLUDED.team_data
                    """,
                    [t["team_id"], json.dumps(t)],
                )
                logging.info(
                    "Team id #{} inserted into the table".format(
                        t["team_id"]))
            except BaseException:
                logging.warning("Record skipped")
                pass

    cur.close()
    conn.close()


def get_team_member_data():

    conn = PostgresHook(postgres_conn_id="postgres_workboard").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT team_id FROM teams_raw")
    team_ids = [line[0] for line in cur]

    params = {"token": Variable.get("workboard_token")}
    base_url = "https://www.myworkboard.com/wb/apis"

    for t in team_ids:
        r = requests.get(base_url + "/team/{}".format(t), params=params)
        if r.status_code != 200:
            raise ValueError(
                "Request not successful. Status code {}".format(r.status_code)
            )
        else:
            team = r.json()["data"]["team"]
            try:
                cur.execute(
                    """INSERT INTO team_members_raw (team_id, team_member_data)
                        VALUES (%s, %s) ON CONFLICT (team_id)
                        DO UPDATE SET team_member_data = EXCLUDED.team_member_data
                    """,
                    [team["team_id"], json.dumps(team)],
                )
                logging.info(
                    "Team id #{} inserted into the table".format(
                        team["team_id"])
                )
            except BaseException:
                logging.warning("Record skipped")
                pass

    cur.close()
    conn.close()


def get_goal_data(goal_status=1):

    conn = PostgresHook(postgres_conn_id="postgres_workboard").get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    params = {
        "token": Variable.get("workboard_token"),
        "goal_status": goal_status}

    base_url = "https://www.myworkboard.com/wb/apis"

    r = requests.get(base_url + "/goal", params=params)
    if r.status_code != 200:
        raise ValueError(
            "Request not successful. Status code {}".format(
                r.status_code))
    else:
        goals = r.json()["data"]["goal"]
        logging.info("There are {} person(s) with goals".format(len(goals)))
        for person in goals:
            logging.info(
                "{} has {} goals".format(
                    person["user_email"], len(person["people_goals"])
                )
            )
            for g in person["people_goals"]:
                try:
                    cur.execute(
                        """INSERT INTO goals_raw (goal_id, goal_data)
                           VALUES (%s, %s) ON CONFLICT (goal_id)
                           DO UPDATE SET goal_data = EXCLUDED.goal_data
                        """,
                        [int(g["goal_id"]), json.dumps(g)],
                    )
                    logging.info(
                        "Goal id #{} inserted into the table".format(
                            g["goal_id"])
                    )
                except BaseException:
                    logging.warning("Record skipped")
                    pass

    cur.close()
    conn.close()


users_raw_exists = PostgresOperator(
    task_id="users_raw_exists",
    sql="SELECT COUNT(*) FROM users_raw",
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

get_users = PythonOperator(
    task_id="get_users",
    python_callable=get_user_data,
    dag=dag)

teams_raw_exists = PostgresOperator(
    task_id="teams_raw_exists",
    sql="SELECT COUNT(*) FROM teams_raw",
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

get_teams = PythonOperator(
    task_id="get_teams",
    python_callable=get_team_data,
    dag=dag)

team_members_raw_exists = PostgresOperator(
    task_id="team_members_raw_exists",
    sql="SELECT COUNT(*) FROM team_members_raw",
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

get_team_members = PythonOperator(
    task_id="get_team_member_data", python_callable=get_team_member_data, dag=dag
)

goals_raw_exists = PostgresOperator(
    task_id="goals_raw_exists",
    sql="SELECT COUNT(*) FROM goals_raw",
    postgres_conn_id="postgres_workboard",
    dag=dag,
)

get_open_goals = PythonOperator(
    task_id="get_open_goals",
    python_callable=get_goal_data,
    op_kwargs={"goal_status": "1"},
    dag=dag,
)

get_closed_goals = PythonOperator(
    task_id="get_closed_goals",
    python_callable=get_goal_data,
    op_kwargs={"goal_status": "2"},
    dag=dag,
)

trigger_etl = TriggerDagRunOperator(
    task_id="trigger_etl", trigger_dag_id="workboard_etl", dag=dag
)

# set upstream tasks
users_raw_exists >> get_users
teams_raw_exists >> get_teams >> team_members_raw_exists >> get_team_members
goals_raw_exists >> [get_open_goals, get_closed_goals]
[get_users, get_team_members, get_open_goals, get_closed_goals] >> trigger_etl
