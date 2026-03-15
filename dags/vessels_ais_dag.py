from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pendulum
import websockets

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_conn"
AIS_WS_URL = "wss://stream.aisstream.io/v0/stream"
AIS_API_KEY = "suckass"


async def collect_and_insert(
    postgres_conn_id: str,
    api_key: str,
    bounding_boxes: list,
    seconds: int = 20,
    max_messages: int = 300,
) -> int:
    
    
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    conn.autocommit = False

    insert_sql = """
    INSERT INTO ais_positions (
        mmsi,
        ship_name,
        latitude,
        longitude,
        sog,
        cog,
        navigational_status,
        timestamp,
        raw_payload
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    ON CONFLICT (mmsi, timestamp)
    DO UPDATE SET
        ship_name = EXCLUDED.ship_name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        sog = EXCLUDED.sog,
        cog = EXCLUDED.cog,
        navigational_status = EXCLUDED.navigational_status,
        raw_payload = EXCLUDED.raw_payload
    """

    inserted = 0

    try:
        async with websockets.connect(
            AIS_WS_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
        ) as ws:
            await ws.send(
                json.dumps(
                    {
                        "APIKey": api_key,
                        "BoundingBoxes": bounding_boxes,
                        "FilterMessageTypes": ["PositionReport"],
                    }
                )
            )

            deadline = asyncio.get_running_loop().time() + seconds

            with conn.cursor() as cur:
                while asyncio.get_running_loop().time() < deadline and inserted < max_messages:
                    timeout = max(0.1, deadline - asyncio.get_running_loop().time())

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        break

                    data = json.loads(raw)

                    meta = data.get("MetaData", {})
                    msg = data.get("Message", {})
                    pos = msg.get("PositionReport", {})

                    mmsi = meta.get("MMSI")
                    if mmsi is None:
                        continue

                    # Für Dev reicht ein Fallback auf "jetzt", falls kein Timestamp mitkommt
                    meta_ts = meta.get("time_utc") or meta.get("timestamp")

                    if meta_ts:
                        try:
                            timestamp = pendulum.parse(meta_ts)
                        except Exception:
                            timestamp = datetime.now(timezone.utc)
                    else:
                        timestamp = datetime.now(timezone.utc)

                    cur.execute(
                        insert_sql,
                        (
                            int(mmsi),
                            meta.get("ShipName"),
                            pos.get("Latitude"),
                            pos.get("Longitude"),
                            pos.get("Sog"),
                            pos.get("Cog"),
                            pos.get("NavigationalStatus"),
                            timestamp,
                            json.dumps(data),
                        ),
                    )
                    inserted += 1

            conn.commit()
            return inserted

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@dag(
    dag_id="ais_devcontainer_to_postgres",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "dev",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ais", "postgres", "devcontainer"],
)
def ais_devcontainer_to_postgres():

    init_schema = SQLExecuteQueryOperator(
        task_id="init_schema",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS ais_positions (
            mmsi BIGINT NOT NULL,
            ship_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            sog DOUBLE PRECISION,
            cog DOUBLE PRECISION,
            navigational_status INTEGER,
            timestamp TIMESTAMPTZ NOT NULL,
            raw_payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (mmsi, timestamp)
        );

        CREATE INDEX IF NOT EXISTS idx_ais_positions_timestamp
        ON ais_positions (timestamp DESC);
        """,
    )

    @task()
    def ingest() -> int:
        boxes = [
                    [
                        [47.3024876979, 5.98865807458],
                        [54.983104153, 15.0169958839]
                    ]
                ]
        return asyncio.run(
            collect_and_insert(
                postgres_conn_id=POSTGRES_CONN_ID,
                api_key=AIS_API_KEY,
                bounding_boxes=boxes,
                seconds=20,
                max_messages=300,
            )
        )

    init_schema >> ingest()


dag = ais_devcontainer_to_postgres()