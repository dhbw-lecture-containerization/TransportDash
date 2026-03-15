import datetime
import json
import pendulum

import requests
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id="car_traffic_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 3, 12),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)
def CarTrafficDag():
    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="postgres_conn",
        sql="""
            CREATE SCHEMA IF NOT EXISTS car;
        """
    )

    create_highway_table = SQLExecuteQueryOperator(
        task_id="create_highway_table",
        conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS car.highways (
                id INTEGER PRIMARY KEY,
                name VARCHAR(10) NOT NULL
            );""",
    )

    @task
    def get_highways():
        resp = requests.get("https://verkehr.autobahn.de/o/autobahn/")
        if (not resp.ok): return 1

        data = json.loads(resp.content)

        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        for idx, road in enumerate(data["roads"]):
            print(f"{idx}: \"{road}\"")
            cur.execute(f"INSERT INTO car.highways (id, name) VALUES ({idx}, '{road}') ON CONFLICT DO NOTHING")

        conn.commit()

    create_schema >> create_highway_table >> get_highways()

dag = CarTrafficDag()
