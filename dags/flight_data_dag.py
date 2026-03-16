from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from traffic.data import opensky
from pathlib import Path


BBOX = (5.98865807458, 47.3024876979, 15.0169958839, 54.983104153)


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

    # remove geometry column
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

    hook = PostgresHook(postgres_conn_id="postgres_conn")
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        for _, row in aircraft.iterrows():
            conn.execute(
                text("""
                INSERT INTO airtraffic.aircraft
                (icao24, callsign, origin_country)
                VALUES
                (:icao24, :callsign, :origin_country)
                ON CONFLICT (icao24)
                DO UPDATE SET
                    callsign = EXCLUDED.callsign
                """),
                row.to_dict()
            )

def store_positions(**context):

    # Pull from fetch_planes_task
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