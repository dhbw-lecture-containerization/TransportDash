import streamlit as st
import polars as pl
import pandas as pd  # grrrr, pandas
import matplotlib.pyplot as plt
import pydeck as pdk
import logging
from pathlib import Path
import plotly.express as px


st.set_page_config(
    page_title="Air Traffic",
    layout="wide"
)

st.header("Flugverkehrsdaten Dashboard")
st.caption("""⚠️ Hinweis: Dieses Dashboard visualisiert Daten, die mit dem Airflow DAG gesammelt werden.
           Die Daten und Auswertungen erheben keinen Anspruch auf Vollständigkeit oder Eignung zur Verwendung in wissenschaftlichen Arbeiten.
           Sie sind lediglich als Technologie-Demonstrator / proof-of-concept zu sehen.""")
POSTGRES_URI = "postgres://postgres:postgres@db:5432/postgres"


@st.cache_data(ttl=600)
def read_flights(uri: str) -> pl.DataFrame:
    logging.debug("reading flights")
    return pl.read_database_uri(
        query="SELECT * FROM airtraffic.aircraft",
        uri=uri
    )

@st.cache_data(ttl=600)
def read_flight_points(uri: str) -> pl.DataFrame:
    logging.debug("reading flight points")
    return pl.read_database_uri(
        query="SELECT * FROM airtraffic.positions",
        uri=uri
    )


def create_combined_flight_table(flights: pl.LazyFrame, points: pl.LazyFrame) -> pl.LazyFrame:
    return flights.join(
        points.group_by("icao24").agg(pl.all().exclude("icao24")),
        on="icao24",
        how="inner"
    )

def get_airline_data_lf() -> pl.LazyFrame:
    logging.debug("reading airline data")
    lf = pl.scan_csv(Path(__file__).parent.parent.parent / "assets" / "airlines" / "airlines.csv")
    lf = lf.rename(
        {
            "Name": "airline_name",
            "IATA": "airline_IATA_code",
            "ICAO": "airline_ICAO_code",
            "Callsign": "airline_callsign",
            "Country": "airline_country",
            "Active": "airline_active"
        }
    )
    #INFO: this is obviously not an actual airline however it seems to appear quite often
    chx_lf = pl.DataFrame(
        {
            "airline_name": "ADAC Luftrettung",
            "airline_IATA_code": None,
            "airline_ICAO_code": "CHX",
            "airline_callsign": None,
            "airline_country": "Germany",
            "airline_active": None
        }
    ).cast(
        {
            "airline_IATA_code": pl.String(),
            "airline_callsign": pl.String(),
            "airline_active": pl.String()
        }
    ).lazy()
    return pl.concat([lf, chx_lf], how="vertical_relaxed")


@st.cache_data(ttl=600)
def get_latest_positions_df() -> pl.DataFrame:
    logging.debug("computing latest positions")
    flights = read_flights(uri=POSTGRES_URI).lazy()
    flight_points = read_flight_points(uri=POSTGRES_URI).lazy()
    airlines = get_airline_data_lf()

    return flight_points.sort("timestamp").group_by_dynamic(
        "timestamp",
        every="10m",
        period="10m"
    ).agg(
        pl.all().exclude("timestamp")
    ).sort(
        "timestamp", descending=True
    ).head(
        1
    ).explode(
        pl.all().exclude("timestamp")
    ).join(
        flights,
        on="icao24",
        how="inner"
    ).with_columns([
        pl.col("altitude").fill_null(0).alias("altitude_filled")
    ]).with_columns(
        pl.col("callsign").str.slice(0, 3).alias("ICAO_3letter")
    ).join(
        airlines,
        left_on="ICAO_3letter",
        right_on="airline_ICAO_code",
        how="left"
    ).collect(engine="cpu")



flights_df = read_flights(POSTGRES_URI)
flight_points_df = read_flight_points(POSTGRES_URI)
latest_positions_df = get_latest_positions_df()
# transition to lazy computation (initial DFs are eager as lazy DB pulling is hard and not worth it)
flights = flights_df.lazy()
flight_points = flight_points_df.lazy()
combined_flights_table = create_combined_flight_table(flights, flight_points)

st.subheader("Flugzeuge in der Luft aktuell")


def get_aircraft_counts_lf(points: pl.LazyFrame) -> pl.LazyFrame:
    return points.sort("timestamp").group_by_dynamic(
        "timestamp",
        every="10m",
        period="10m"
    ).agg(
        pl.col("icao24").n_unique().alias("n_aircraft")
    ).sort(pl.col("n_aircraft"), descending=True).filter(pl.col("n_aircraft") > 100)


@st.cache_data(ttl=600)
def get_aircraft_counts_df() -> pl.DataFrame:
    logging.debug("computing aircraft counts")
    flight_points_df = read_flight_points(uri=POSTGRES_URI)
    return get_aircraft_counts_lf(flight_points_df.lazy()).collect(engine="cpu")


@st.cache_data(ttl=600)
def get_current_aircraft_counts():
    logging.debug("computing current aircraft counts")
    flight_points_df = read_flight_points(uri=POSTGRES_URI)
    return get_aircraft_counts_lf(flight_points_df.lazy()).head(1).collect(engine="cpu")


current_aircraft_count = get_current_aircraft_counts()
current_aircraft_timestamp = current_aircraft_count["timestamp"][0]
current_aircraft_count_num = current_aircraft_count.select(pl.col("n_aircraft"))[0][0]


st.metric(f"Anzahl an Flugzeugen über Deutschland aktuell: ", current_aircraft_count_num)
st.caption(f"Letzte Messung: {current_aircraft_timestamp:%d-%m-%Y %H:%M:%S}")

col1, col2 = st.columns(2)


aircraft_counts = get_aircraft_counts_df()


col1.text("Entwicklung der Flugzeuganzahl")
col1.line_chart(
    aircraft_counts,
    x="timestamp",
    y="n_aircraft"
)

@st.cache_data(ttl=600)
def get_aircraft_counts_dif(aircraft_counts: pl.DataFrame) -> pl.DataFrame:
    logging.debug("comptuing aircraft count Delta")
    return aircraft_counts.with_columns(
        pl.col("n_aircraft").cast(pl.Int64) - pl.col("n_aircraft").cast(pl.Int64).shift(1)
    )


aircraft_counts_dif = get_aircraft_counts_dif(aircraft_counts)

col2.text("Differenz der Flugzeuganzahl")
col2.line_chart(
    aircraft_counts_dif,
    x="timestamp",
    y="n_aircraft"
)


st.subheader("Aktuelle Flugzeugpositionen über Deutschland")

@st.cache_data(ttl=600)
def compute_visualization_df(latest_positions: pl.DataFrame) -> pd.DataFrame:
    logging.debug("computing visualization DataFrame")


    altitudes = latest_positions["altitude_filled"]
    min_alt = altitudes.min()
    max_alt = altitudes.max() if altitudes.max() > min_alt else min_alt + 1


    latest_positions = latest_positions.with_columns([
        ((altitudes - min_alt) / (max_alt - min_alt) * 255).alias("r"),
        pl.lit(50.0).alias("g"),
        ((1 - (altitudes - min_alt) / (max_alt - min_alt)) * 255).alias("b")
    ])


    latest_positions = latest_positions.with_columns([
        pl.concat_list(["r", "g", "b"]).alias("color")
    ])


    pdf = latest_positions.to_pandas()


    pdf["color"] = pdf["color"].apply(lambda x: [float(c) for c in x])  # this is diabolical - R.I.P. Performance

    return pdf


visualization_df = compute_visualization_df(latest_positions_df)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=visualization_df,
    get_position='[longitude, latitude]',
    get_radius=5000,
    get_fill_color='color',
    pickable=True,
)

view_state = pdk.ViewState(
    latitude=51.0,
    longitude=10.0,
    zoom=5,
    pitch=0,
)

st.pydeck_chart(
    pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>Callsign:</b> {callsign}<br/><b>Airline:</b> {airline_name}<br/><b>Altitude:</b> {altitude}",
            "style": {"backgroundColor": "black", "color": "white"}
        }
    )
)


st.subheader("Airline Analysen")
col1, col2 = st.columns(2)

def get_airlines_aggregate_lf(flights: pl.LazyFrame, airlines: pl.LazyFrame) -> pl.LazyFrame:
    return flights.with_columns(
        pl.col("callsign").str.slice(0, 3).alias("ICAO_3letter")
    ).group_by(
        "ICAO_3letter"
    ).agg(
        pl.col("icao24").n_unique().alias("n_aircraft")
    ).join(
        airlines,
        left_on="ICAO_3letter",
        right_on="airline_ICAO_code",
        how="left"
    ).sort(
        by=pl.col("n_aircraft"),
        descending=True
    )

@st.cache_data(ttl=600)
def get_airlines_viz_df(top_k: int):
    flights = read_flights(uri=POSTGRES_URI)
    n_airlines = flights.with_columns(
        pl.col("callsign").str.slice(0, 3).alias("ICAO_3letter")
    ).select("ICAO_3letter").n_unique()
    flights_lf = flights.lazy()
    airlines = get_airline_data_lf()
    agg_lf = get_airlines_aggregate_lf(flights_lf, airlines)

    cols = [
        "ICAO_3letter",
        "n_aircraft",
        "airline_name",
        "airline_IATA_code",
        "airline_callsign",
        "airline_country",
        "airline_active",
    ]
    top_k_df = agg_lf.head(top_k).select(cols)

    others = agg_lf.tail(n_airlines - top_k).select(
        pl.sum("n_aircraft").alias("n_aircraft")
    ).with_columns([
        pl.lit("-").alias("ICAO_3letter"),
        pl.lit("Others/unknown").alias("airline_name"),
        pl.lit("Others/unknown").alias("airline_IATA_code"),
        pl.lit("Others/unknown").alias("airline_callsign"),
        pl.lit("Others/unknown").alias("airline_country"),
        pl.lit("Others/unknown").alias("airline_active"),
    ])
    others = others.select(cols)
    result = pl.concat([top_k_df, others]).with_columns([
        pl.when(pl.col("airline_name").is_null())
          .then(pl.lit("Unknown"))
          .otherwise(pl.col("airline_name"))
          .alias("airline_name"),

        pl.when(pl.col("airline_country").is_null())
          .then(pl.lit("Unknown"))
          .otherwise(pl.col("airline_country"))
          .alias("airline_country"),
    ]).with_columns([
        (
            pl.col("airline_name") + " (" + pl.col("ICAO_3letter") + ")"
        ).alias("airline_label")
    ])

    return result.collect()


viz_df = get_airlines_viz_df(top_k=10)


fig_airlines = px.pie(
    viz_df.to_pandas(),
    names="airline_label",
    values="n_aircraft",
    title="Airline Verteilung",
)

fig_airlines.update_traces(textposition="inside", textinfo="percent+label")
col1.plotly_chart(fig_airlines)


df_country = get_airlines_aggregate_lf(
    read_flights(uri=POSTGRES_URI).lazy(),
    get_airline_data_lf()
).group_by(
    "airline_country"
).agg(
    pl.col("n_aircraft").sum()
).sort(
    "n_aircraft",
    descending=True
).select(
    pl.col(["n_aircraft", "airline_country"])
).collect()

fig_countries = px.pie(
    df_country.to_pandas(),
    names="airline_country",
    values="n_aircraft",
    title="Verteilung nach Registrierungs-Ländern",
)

fig_countries.update_traces(textposition="inside", textinfo="percent+label")
col2.plotly_chart(fig_countries)


st.subheader("Database-Metriken")

col1, col2, col3 = st.columns(3)


col1.metric("gespeicherte Flugzeuge / Flüge: ", len(flights_df))
col2.metric("gespeicherte Punkte: ", len(flight_points_df))
col3.metric("Timestamps: ", len(aircraft_counts))

