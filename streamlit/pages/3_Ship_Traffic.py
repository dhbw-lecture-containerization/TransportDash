import json
import os
from zoneinfo import ZoneInfo

import psycopg2
import pydeck as pdk
import streamlit as st


DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_NAME = os.getenv("POSTGRES_DB", "airflow")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")


def load_random_ship(cursor):
    cursor.execute(
        """
        WITH latest_known AS (
            SELECT DISTINCT ON (mmsi)
                mmsi,
                ship_name,
                latitude,
                longitude,
                sog,
                cog,
                navigational_status,
                destination,
                timestamp
            FROM ais.positions
            WHERE NULLIF(BTRIM(destination), '') IS NOT NULL
              AND LOWER(BTRIM(destination)) <> 'unknown'
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY mmsi, timestamp DESC
        )
        SELECT
            lk.mmsi,
            lk.ship_name,
            lk.latitude,
            lk.longitude,
            lk.sog,
            lk.cog,
            lk.navigational_status,
            lk.destination,
            lk.timestamp,
            dw.latitude AS destination_latitude,
            dw.longitude AS destination_longitude,
            dw.weather,
            dw.provider,
            dw.updated_at AS weather_updated_at
        FROM latest_known AS lk
        LEFT JOIN ais.destination_weather AS dw
          ON LOWER(BTRIM(dw.destination)) = LOWER(BTRIM(lk.destination))
        ORDER BY RANDOM()
        LIMIT 1
        """
    )
    return cursor.fetchone()


def parse_weather_payload(weather_payload):
    if weather_payload is None:
        return {}
    if isinstance(weather_payload, dict):
        return weather_payload
    if isinstance(weather_payload, str):
        try:
            return json.loads(weather_payload)
        except json.JSONDecodeError:
            return {}
    return {}


st.title("🚢 Ship Traffic")
st.caption("Zufälliges Schiff mit bekanntem Zielhafen und Wetter am Zielort")

refresh_clicked = st.button("🔀 Anderes zufälliges Schiff")
if refresh_clicked:
    st.rerun()

db_connection = None

try:
    db_connection = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
    )

    with db_connection.cursor() as cursor:
        ship = load_random_ship(cursor)

    if not ship:
        st.warning("Kein Schiff mit bekanntem Ziel gefunden.")
        st.stop()

    (
        mmsi,
        ship_name,
        latitude,
        longitude,
        sog,
        cog,
        navigational_status,
        destination,
        timestamp,
        destination_latitude,
        destination_longitude,
        weather_payload,
        weather_provider,
        weather_updated_at,
    ) = ship

    weather = parse_weather_payload(weather_payload)
    current_weather = weather.get("current", {})

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Schiff", ship_name or "Unbekannt")
    col2.metric("MMSI", str(mmsi))
    col3.metric("Ziel", destination)
    col4.metric(
        "Letzte Position",
        timestamp.astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M") if timestamp else "-",
    )

    st.subheader("Aktuelle Position")
    details_col1, details_col2, details_col3 = st.columns(3)
    details_col1.metric("Geschwindigkeit (SOG)", f"{sog:.1f} kn" if sog is not None else "-")
    details_col2.metric("Kurs (COG)", f"{cog:.1f}°" if cog is not None else "-")
    details_col3.metric("Navigationsstatus", str(navigational_status) if navigational_status is not None else "-")

    map_points = [
        {
            "label": "Schiff",
            "lat": latitude,
            "lon": longitude,
            "color": [30, 144, 255],
            "radius": 25000,
        }
    ]

    if destination_latitude is not None and destination_longitude is not None:
        map_points.append(
            {
                "label": "Ziel",
                "lat": destination_latitude,
                "lon": destination_longitude,
                "color": [255, 140, 0],
                "radius": 30000,
            }
        )

    st.pydeck_chart(
        pdk.Deck(
            initial_view_state=pdk.ViewState(
                latitude=latitude,
                longitude=longitude,
                zoom=4,
            ),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=map_points,
                    get_position="[lon, lat]",
                    get_color="color",
                    get_radius="radius",
                    pickable=True,
                )
            ],
            tooltip={"text": "{label}"},
        )
    )

    st.subheader("Wetter am Zielort")
    if current_weather:
        weather_col1, weather_col2, weather_col3, weather_col4 = st.columns(4)
        weather_col1.metric("Temperatur", f"{current_weather.get('temperature_2m', '-')} °C")
        weather_col2.metric("Gefühlt", f"{current_weather.get('apparent_temperature', '-')} °C")
        weather_col3.metric("Wind", f"{current_weather.get('wind_speed_10m', '-')} km/h")
        weather_col4.metric("Niederschlag", f"{current_weather.get('precipitation', '-')} mm")

        info_col1, info_col2 = st.columns(2)
        info_col1.write(f"Weather code: {current_weather.get('weather_code', '-')}")
        info_col2.write(
            "Aktualisiert: "
            + (
                weather_updated_at.astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M")
                if weather_updated_at
                else "-"
            )
        )
        if weather_provider:
            st.caption(f"Quelle: {weather_provider}")
    else:
        st.info("Für dieses Ziel liegt noch kein Wetterdatensatz vor.")

except Exception as exc:
    st.error(f"Fehler beim Laden der Schiffsdaten: {exc}")
finally:
    if db_connection:
        db_connection.close()