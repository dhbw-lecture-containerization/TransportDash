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

cursor.execute("SELECT id FROM car.timestamps ORDER BY timestamp DESC LIMIT 1")
(recent_timestamp_id,) = cursor.fetchall()[0]

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
    (id, _, _, _, title, description, lat, long, bbox_lat1, bbox_long1, bbox_lat2, bboxlong2, _, highway, _, timestamp) = warning
    return {
        "id": id,
        "title": title,
        "description": "\n".join(textwrap.wrap(description.replace("\\n", "\n"), width=50, replace_whitespace=False)),
        "lat": lat,
        "lon": long,
        "bbox": ((bbox_lat1, bbox_long1), (bbox_lat2, bboxlong2)),
        "highway": highway,
        "timestamp": timestamp,
        "type": "Warnung"
    }
for warning in cursor.fetchall():
    recent_warnings.append(extract_warning(warning))
recent_warnings = pd.DataFrame(recent_warnings)

st.title("🚗 Car Traffic")

st.write("Car traffic data overview")

# Example placeholder
st.metric("Cars today", 54000)
st.bar_chart([100, 200, 150, 300])

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
            get_color=[255, 165, 0],
            get_radius=5000,
            pickable=True,
        ),
    ],
    tooltip={"text": "{highway}: {type}\n{description}"}
))


cursor.close()
db_connection.close()