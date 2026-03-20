import datetime
import json
import pendulum
from tqdm import tqdm

import requests
from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def parse_warning(warning : dict):
    out = {
        "id": warning["identifier"],
        "title": warning["title"],
        "description": "\n".join(warning["description"]),
        "latitude": float(warning["coordinate"]["lat"]),
        "longitude": float(warning["coordinate"]["long"]),
    }

    return out

@dag(
    dag_id="car_traffic_dag",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 3, 12),
    catchup=False,
    max_active_runs=1,
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

    create_timestamp_table = SQLExecuteQueryOperator(
        task_id="create_timestamp_table",
        conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS car.timestamps (
                id SERIAL PRIMARY KEY,
                timestamp timestamptz UNIQUE NOT NULL
            );""",
    )

    create_highway_table = SQLExecuteQueryOperator(
        task_id="create_highway_table",
        conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS car.highways (
                id SERIAL PRIMARY KEY,
                name VARCHAR(10) UNIQUE NOT NULL
            );""",
    )

    create_warnings_table = SQLExecuteQueryOperator(
        task_id="create_warnings_table",
        conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS car.warnings (
                id char(128) PRIMARY KEY,
                highwayId INTEGER NOT NULL REFERENCES car.highways(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                type TEXT NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL
            );""",
    )

    create_warning_timestamps_table = SQLExecuteQueryOperator(
        task_id="create_warning_timestamps_table",
        conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS car.warningTimestamps (
                warningId char(128) NOT NULL REFERENCES car.warnings(id) ON DELETE CASCADE,
                timestampId INTEGER NOT NULL REFERENCES car.timestamps(id) ON DELETE CASCADE,
                PRIMARY KEY (warningId, timestampId)
            );"""
    )

    @task
    def get_highways():
        sql = """
            WITH ret AS (
                INSERT INTO car.highways (name) VALUES ('{name}') 
                ON CONFLICT DO NOTHING
                RETURNING id
            ) 
            SELECT * FROM ret
            UNION
                SELECT id FROM car.highways WHERE name='{name}';
        """

        resp = requests.get("https://verkehr.autobahn.de/o/autobahn/")
        if (not resp.ok): raise resp.raise_for_status()

        data = json.loads(resp.content)
        roads = []

        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        for road in data["roads"]:
            cur.execute(sql.format(name=road))
            roads.append((cur.fetchone()[0], road))
        conn.commit()

        return roads
    get_highways = get_highways()

    @task
    def create_timestamp():
        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        cur.execute("INSERT INTO car.timestamps (timestamp) VALUES (now()) RETURNING id")
        conn.commit()

        return cur.fetchone()[0]
    create_timestamp = create_timestamp()
    
    @task
    def get_warnings():
        sql = """
            INSERT INTO car.warnings (id, highwayId, title, description, type, latitude, longitude) 
                VALUES ({id!r}, {highway_id}, {title!r}, {description!r}, {type!r}, {latitude}, {longitude})
                ON CONFLICT DO NOTHING;
        """
        task_instance = get_current_context()["ti"]
        highways = task_instance.xcom_pull(task_ids='get_highways')
        warning_ids = []

        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        for (highway_id, highway_name) in tqdm(highways):
            resp = requests.get(f"https://verkehr.autobahn.de/o/autobahn/{highway_name}/services/warning")
            if (not resp.ok): raise resp.raise_for_status()

            data = json.loads(resp.content)
            warnings = data["warning"]

            for warning in warnings:
                warning = parse_warning(warning)
                cur.execute(sql.format(highway_id=highway_id, type="Warnung", **warning))
                warning_ids.append(warning["id"])
            
            resp = requests.get(f"https://verkehr.autobahn.de/o/autobahn/{highway_name}/services/roadworks")
            if (not resp.ok): raise resp.raise_for_status()

            data = json.loads(resp.content)
            warnings = data["roadworks"]

            for warning in warnings:
                warning = parse_warning(warning)
                cur.execute(sql.format(highway_id=highway_id, type="Baustelle", **warning))
                warning_ids.append(warning["id"])

        conn.commit()
        return warning_ids
    get_warnings = get_warnings()

    @task
    def timestamp_warnings():
        sql = """
            INSERT INTO car.warningTimestamps (warningId, timestampId)
                VALUES ('{warning_id}', {timestamp_id});
        """
        task_instance = get_current_context()["ti"]
        timestamp_id = task_instance.xcom_pull(task_ids='create_timestamp')
        warning_ids = task_instance.xcom_pull(task_ids='get_warnings')

        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        for warning_id in warning_ids:
            cur.execute(sql.format(warning_id=warning_id, timestamp_id=timestamp_id))
        
        conn.commit()
    timestamp_warnings = timestamp_warnings()

    create_schema >> create_timestamp_table
    create_schema >> create_highway_table
    [create_schema, create_highway_table] >> create_warnings_table
    [create_schema, create_timestamp_table, create_warnings_table] >> create_warning_timestamps_table

    [create_timestamp_table, create_highway_table] >> get_highways
    [create_timestamp_table, create_highway_table] >> create_timestamp

    [create_warnings_table, get_highways] >> get_warnings
    [create_warning_timestamps_table, create_timestamp, get_warnings] >> timestamp_warnings

dag = CarTrafficDag()
