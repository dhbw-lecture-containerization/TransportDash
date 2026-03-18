from __future__ import annotations

import asyncio
import json
from urllib.parse import quote_plus
from urllib.request import urlopen
from datetime import datetime, timedelta, timezone

import pendulum
import websockets

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_conn"
AIS_WS_URL = "wss://stream.aisstream.io/v0/stream"
AIS_API_KEY = "pls use your own key, thx, pls no steal mine thx, thx bye :) pls no steal thx :)"


def fetch_weather_for_destination(destination: str) -> dict | None:
    destination = (destination or "").strip()
    if not destination:
        return None

    # Open-Meteo geocoding + current weather (no API key required)
    geocode_url = (
        "https://geocoding-api.open-meteo.com/v1/search"
        f"?name={quote_plus(destination)}&count=1&language=en&format=json"
    )

    try:
        with urlopen(geocode_url, timeout=10) as response:
            geocode_data = json.loads(response.read().decode("utf-8"))

        results = geocode_data.get("results") or []
        if not results:
            return None

        best = results[0]
        latitude = best.get("latitude")
        longitude = best.get("longitude")

        if latitude is None or longitude is None:
            return None

        weather_url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={latitude}&longitude={longitude}&current=temperature_2m,"
            "apparent_temperature,precipitation,weather_code,wind_speed_10m"
            "&timezone=UTC"
        )

        with urlopen(weather_url, timeout=10) as response:
            weather_data = json.loads(response.read().decode("utf-8"))

        return {
            "destination": destination,
            "latitude": latitude,
            "longitude": longitude,
            "resolved_name": best.get("name"),
            "country": best.get("country"),
            "admin1": best.get("admin1"),
            "current": weather_data.get("current", {}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "provider": "open-meteo",
        }
    except Exception:
        return None


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
    INSERT INTO ais.positions (
        mmsi,
        ship_name,
        latitude,
        longitude,
        sog,
        cog,
        navigational_status,
        destination,
        timestamp,
        raw_payload
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    ON CONFLICT (mmsi, timestamp)
    DO UPDATE SET
        ship_name = EXCLUDED.ship_name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        sog = EXCLUDED.sog,
        cog = EXCLUDED.cog,
        navigational_status = EXCLUDED.navigational_status,
        destination = EXCLUDED.destination,
        raw_payload = EXCLUDED.raw_payload
    """

    update_destination_sql = """
    UPDATE ais.positions
    SET destination = %s
    WHERE mmsi = %s
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
                        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
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
                    static = msg.get("ShipStaticData", {})

                    mmsi = meta.get("MMSI")

                    if mmsi is None:
                        continue

                    destination = (
                        meta.get("Destination")
                        or static.get("Destination")
                        or static.get("destination")
                        or "UNKNOWN"
                    )

                    # Static messages --> different UPDATE but same table, who cares about Normalformen rn
                    if static and destination != "UNKNOWN":
                        cur.execute(update_destination_sql, (destination, int(mmsi)))
                        continue

                    if not pos:
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
                            destination,
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


def collect_destination_weather(postgres_conn_id: str, limit: int = 50) -> int:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    conn.autocommit = False

    select_destinations_sql = """
    SELECT DISTINCT destination
    FROM ais.positions
    WHERE destination IS NOT NULL
      AND destination <> ''
      AND destination <> 'UNKNOWN'
    ORDER BY destination
    LIMIT %s
    """

    upsert_weather_sql = """
    INSERT INTO ais.destination_weather (
        destination,
        latitude,
        longitude,
        weather,
        provider,
        updated_at
    )
    VALUES (%s, %s, %s, %s::jsonb, %s, NOW())
    ON CONFLICT (destination)
    DO UPDATE SET
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        weather = EXCLUDED.weather,
        provider = EXCLUDED.provider,
        updated_at = NOW()
    """

    updated = 0

    try:
        with conn.cursor() as cur:
            cur.execute(select_destinations_sql, (limit,))
            destinations = [row[0] for row in cur.fetchall()]

            for destination in destinations:
                weather = fetch_weather_for_destination(destination)
                if not weather:
                    continue

                cur.execute(
                    upsert_weather_sql,
                    (
                        destination,
                        weather.get("latitude"),
                        weather.get("longitude"),
                        json.dumps(weather),
                        weather.get("provider", "open-meteo"),
                    ),
                )
                updated += 1

        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@dag(
    dag_id="ais_devcontainer_to_postgres",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
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
        DO $$
        BEGIN
            CREATE SCHEMA ais;
        EXCEPTION
            WHEN duplicate_schema THEN NULL;
        END
        $$;

        CREATE TABLE IF NOT EXISTS ais.positions (
            mmsi BIGINT NOT NULL,
            ship_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            sog DOUBLE PRECISION,
            cog DOUBLE PRECISION,
            navigational_status INTEGER,
            destination VARCHAR(255),
            timestamp TIMESTAMPTZ NOT NULL,
            raw_payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (mmsi, timestamp)
        );

        CREATE INDEX IF NOT EXISTS idx_ais_positions_timestamp
        ON ais.positions (timestamp DESC);

        CREATE TABLE IF NOT EXISTS ais.destination_weather (
            destination VARCHAR(255) PRIMARY KEY,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            weather JSONB NOT NULL,
            provider VARCHAR(64) NOT NULL DEFAULT 'open-meteo',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_ais_destination_weather_updated_at
        ON ais.destination_weather (updated_at DESC);
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

    @task()
    def enrich_destination_weather() -> int:
        return collect_destination_weather(
            postgres_conn_id=POSTGRES_CONN_ID,
            limit=50,
        )

    init_schema >> ingest() >> enrich_destination_weather()


dag = ais_devcontainer_to_postgres()