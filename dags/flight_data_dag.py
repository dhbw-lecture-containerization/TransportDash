from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import requests
from datetime import datetime, timedelta, timezone
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from traffic.data import opensky
from pathlib import Path


BBOX = (5.98865807458, 47.3024876979, 15.0169958839, 54.983104153)


def get_opensky_token() -> str | None:
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
    if not client_id or not client_secret:
        return None

    token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    r = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=10,
    )
    if r.status_code != 200:
        return None
    data = r.json()
    return data.get("access_token")


def fetch_est_route_for_aircraft(icao24, token, lookback_hours=3):
    if token is None:
        return None, None
    now = datetime.now(timezone.utc)
    begin = int((now - timedelta(hours=lookback_hours)).timestamp())
    end = int(now.timestamp())

    url = f"https://opensky-network.org/api/flights/aircraft?icao24={icao24}&begin={begin}&end={end}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=20)
    if r.status_code != 200:
        return None, None

    flights = r.json()
    if not flights:
        return None, None

    flights = sorted(flights, key=lambda v: v.get("lastSeen", v.get("firstSeen", 0)), reverse=True)
    for flight in flights:
        dep = flight.get("estDepartureAirport")
        arr = flight.get("estArrivalAirport")
        if dep or arr:
            return dep, arr

    return None, None


def init_db():

    hook = PostgresHook(postgres_conn_id="postgres_conn")
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:

        conn.execute(text("""
        CREATE SCHEMA IF NOT EXISTS airtraffic;
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS airtraffic.aircraft (
            icao24 TEXT PRIMARY KEY,
            callsign TEXT,
            origin_country TEXT,
            departure_airport TEXT,
            arrival_airport TEXT,
            first_seen TIMESTAMPTZ DEFAULT now()
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS airtraffic.positions (
            id BIGSERIAL PRIMARY KEY,
            icao24 TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            last_position TIMESTAMPTZ,

            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,

            altitude DOUBLE PRECISION,
            geoaltitude DOUBLE PRECISION,

            onground BOOLEAN,
            groundspeed DOUBLE PRECISION,
            track DOUBLE PRECISION,
            vertical_rate DOUBLE PRECISION,

            squawk TEXT,
            spi BOOLEAN,
            position_source INTEGER,

            CONSTRAINT fk_aircraft
                FOREIGN KEY (icao24)
                REFERENCES airtraffic.aircraft(icao24),

            CONSTRAINT unique_position
                UNIQUE (icao24, timestamp)
        );
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_positions_aircraft_time
        ON airtraffic.positions (icao24, timestamp);
        """))



def fetch_planes_task(**context):

    west, south, east, north = BBOX

    states = opensky.api_states(bounds=(west, south, east, north))

    if states is None:
        raise RuntimeError("No data returned from OpenSky")

    df = states.data

    df = df.dropna(subset=["latitude", "longitude"])

    planes_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    BASE_DIR = Path(__file__).resolve().parents[1]

    ASSETS = BASE_DIR / "assets" / "geography"

    gdf = gpd.read_file(ASSETS / "ne_110m_admin_0_countries.shp")
    germany = gdf.loc[gdf["NAME"] == "Germany"]

    germany = germany.to_crs(planes_gdf.crs)

    planes = planes_gdf[
        planes_gdf.within(germany.geometry.iloc[0])
    ]

    planes = planes.drop(columns=["geometry"])

    context["ti"].xcom_push(
        key="planes_df",
        value=planes.to_json(date_format="iso")
    )


def store_aircraft(**context):

    df_json = context["ti"].xcom_pull(
        task_ids="fetch_planes",
        key="planes_df"
    )

    if df_json is None:
        raise ValueError(
            "No planes_df found in XCom. Did fetch_planes_task succeed?"
        )

    df = pd.read_json(df_json)

    aircraft = df[["icao24", "callsign", "origin_country"]].drop_duplicates()

    token = get_opensky_token()
    if token is None:
        print("[store_aircraft] No OpenSky token found. origin/destination enrichment will be skipped.")

    enriched_rows = []
    for _, row in aircraft.iterrows():
        departure_airport = None
        arrival_airport = None
        if token is not None:
            departure_airport, arrival_airport = fetch_est_route_for_aircraft(
                row["icao24"], token, lookback_hours=3
            )

        enriched_rows.append({
            "icao24": row["icao24"],
            "callsign": row["callsign"],
            "origin_country": row["origin_country"],
            "departure_airport": departure_airport,
            "arrival_airport": arrival_airport,
        })

    updated = pd.DataFrame(enriched_rows)

    hook = PostgresHook(postgres_conn_id="postgres_conn")
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        for _, row in updated.iterrows():
            conn.execute(
                text("""
                INSERT INTO airtraffic.aircraft
                (icao24, callsign, origin_country, departure_airport, arrival_airport)
                VALUES
                (:icao24, :callsign, :origin_country, :departure_airport, :arrival_airport)
                ON CONFLICT (icao24)
                DO UPDATE SET
                    callsign = EXCLUDED.callsign,
                    origin_country = EXCLUDED.origin_country,
                    departure_airport = EXCLUDED.departure_airport,
                    arrival_airport = EXCLUDED.arrival_airport
                """),
                row.to_dict()
            )

def store_positions(**context):

    df_json = context["ti"].xcom_pull(
        task_ids="fetch_planes",
        key="planes_df"
    )

    if df_json is None:
        raise ValueError("No planes_df found in XCom. Did fetch_planes_task succeed?")

    df = pd.read_json(df_json)

    cols = [
        "icao24",
        "timestamp",
        "last_position",
        "longitude",
        "latitude",
        "altitude",
        "geoaltitude",
        "onground",
        "groundspeed",
        "track",
        "vertical_rate",
        "squawk",
        "spi",
        "position_source",
    ]

    positions = df[cols]

    hook = PostgresHook(postgres_conn_id="postgres_conn")
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        for _, row in positions.iterrows():
            conn.execute(
                text("""
                INSERT INTO airtraffic.positions (
                    icao24,
                    timestamp,
                    last_position,
                    longitude,
                    latitude,
                    altitude,
                    geoaltitude,
                    onground,
                    groundspeed,
                    track,
                    vertical_rate,
                    squawk,
                    spi,
                    position_source
                )
                VALUES (
                    :icao24,
                    :timestamp,
                    :last_position,
                    :longitude,
                    :latitude,
                    :altitude,
                    :geoaltitude,
                    :onground,
                    :groundspeed,
                    :track,
                    :vertical_rate,
                    :squawk,
                    :spi,
                    :position_source
                )
                ON CONFLICT (icao24, timestamp)
                DO NOTHING
                """),
                row.to_dict()
            )


with DAG(
    dag_id="opensky_airtraffic",
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
):

    init = PythonOperator(
        task_id="init_database",
        python_callable=init_db,
        retries=0
    )

    fetch = PythonOperator(
        task_id="fetch_planes",
        python_callable=fetch_planes_task
    )

    aircraft = PythonOperator(
        task_id="store_aircraft",
        python_callable=store_aircraft
    )

    positions = PythonOperator(
        task_id="store_positions",
        python_callable=store_positions
    )

    init >> fetch >> aircraft >> positions