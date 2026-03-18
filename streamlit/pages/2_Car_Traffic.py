import streamlit as st
import pydeck as pdk
import pandas as pd
import psycopg2
import textwrap

st.cache_data(ttl=60)
db_connection = psycopg2.connect(
    host="db",
    user="postgres",
    password="postgres",
    database="postgres",
    port=5432
)
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
        "description": "\n".join(textwrap.wrap(description.replace("\\n", "\n"), width=50, replace_whitespace=False)),
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
        return [255, 165, 0]
    elif row["type"] == "Baustelle":
        return [0, 0, 255]
    else:
        return [0, 255, 0]
recent_warnings["color"] = recent_warnings.apply(get_color, axis=1)

st.title("🚗 Car Traffic")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("🚧 Baustellen", 0)
col2.metric("⚠️ Warnings", len(recent_warnings))
col5.metric("⏱️ Aktualisiert", recent_timestamp.strftime("%H:%M"))

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
            get_radius=5000,
            pickable=True,
        ),
    ],
    tooltip={"text": "{highway}: {type}\n{description}"}
))


cursor.close()
db_connection.close()