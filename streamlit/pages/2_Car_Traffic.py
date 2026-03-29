from zoneinfo import ZoneInfo
import os
import streamlit as st
import pydeck as pdk
import pandas as pd
import psycopg2
import textwrap

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_NAME = os.getenv("POSTGRES_DB", "airflow")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

try:
    db_connection = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
    )
except Exception as exc:
    st.error(f"Fehler bei der Datenbankverbindung: {exc}")
    st.stop()

cursor = db_connection.cursor()

cursor.execute("SELECT * FROM car.timestamps ORDER BY timestamp DESC LIMIT 1")
(recent_timestamp_id, recent_timestamp) = cursor.fetchall()[0]

recent_warnings = []
cursor.execute(f"""
    SELECT * 
        FROM car.warningtimestamps AS wt 
        LEFT JOIN car.warnings AS w ON wt.warningId=w.id 
        LEFT JOIN car.highways AS h ON h.id=w.highwayId 
        LEFT JOIN car.timestamps AS t ON t.id=wt.timestampId
    WHERE wt.timestampid={recent_timestamp_id}
""")
def extract_warning(warning):
    (id, _, _, _, title, description, type, lat, long, _, highway, _, timestamp) = warning
    return {
        "id": id,
        "title": title,
        "description": "\n".join(textwrap.wrap(description.replace("\\n", "\n"), width=80, replace_whitespace=False)),
        "lat": lat,
        "lon": long,
        "highway": highway,
        "timestamp": timestamp,
        "type": type
    }
for warning in cursor.fetchall():
    recent_warnings.append(extract_warning(warning))
recent_warnings = pd.DataFrame(recent_warnings)

def get_color(row):
    if row["type"] == "Warnung":
        return [255, 0, 0]
    elif row["type"] == "Baustelle":
        return [255, 165, 0]
    else:
        return [0, 255, 0]
recent_warnings["color"] = recent_warnings.apply(get_color, axis=1)
def get_radius(row):
    if row["type"] == "Warnung":
        return 5000
    elif row["type"] == "Baustelle":
        return 1000
    else:
        return 5000
recent_warnings["radius"] = recent_warnings.apply(get_radius, axis=1)

st.title("🚗 Car Traffic")

col1, col2, col3 = st.columns(3)
col1.metric("🚧 Baustellen", sum(recent_warnings["type"]=="Baustelle"))
col2.metric("⚠️ Warnungen", sum(recent_warnings["type"]=="Warnung"))
col3.metric("⏱️ Aktualisiert", recent_timestamp.astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m (%H:%M)"))

st.pydeck_chart(pdk.Deck(
    initial_view_state=pdk.ViewState(
        latitude=recent_warnings["lat"].mean(),
        longitude=recent_warnings["lon"].mean(),
        zoom=6,
    ),
    layers=[
        pdk.Layer(
            "ScatterplotLayer",
            data=recent_warnings,
            get_position='[lon, lat]',
            get_color='color',
            get_radius='radius',
            pickable=True,
        ),
    ],
    tooltip={"text": "{highway}: {type}\n{description}"}
))

st.subheader("Alle Events als Tabelle")
st.dataframe(recent_warnings.drop(columns=["id", "color", "radius"]))

cursor.close()
db_connection.close()