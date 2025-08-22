import streamlit as st
import pandas as pd
from datetime import datetime
import pytz
from google.cloud import bigquery
from streamlit_autorefresh import st_autorefresh
import pydeck as pdk
from sklearn.preprocessing import MinMaxScaler

# python3 -m streamlit run app.py

# ---------- CONFIG ----------
PROJECT_ID = "lon-trans-streaming-pipeline"
TABLE_ID = "bus_density_streaming_pipeline.most_recent_predicted_arrivals"
FULL_TABLE = f"{PROJECT_ID}.{TABLE_ID}"

st.set_page_config(layout="wide", page_title="🚌 London Bus Density")
st.title("🚌 🇬🇧 London Predibus, How many buses are arriving in the next minutes?")
st.caption("Real-time forecast based on TfL live bus arrival data")

# ---------- AUTO REFRESH ----------
st_autorefresh(interval=30 * 1000, key="auto_refresh")  # every 30 seconds

# ---------- USER INPUT ----------
time_window = st.slider("Select forecast window (in minutes)", min_value=1, max_value=20, value=10, step=1)

# ---------- BIGQUERY CONNECTION ----------
client = bigquery.Client(project=PROJECT_ID)

def red_gray_blue(norm_val):
    if norm_val < 0.5:
        # Red (1,0,0) to Gray (128,128,128)
        t = norm_val / 0.5
        r = int(255 * (1 - t) + 128 * t)
        g = int(0 + 128 * t)
        b = int(0 + 128 * t)
    else:
        # Gray (128,128,128) to Blue (0,0,255)
        t = (norm_val - 0.5) / 0.5
        r = int(128 * (1 - t))
        g = int(128 * (1 - t))
        b = int(128 * (1 - t) + 255 * t)
    
    return [b, g, r, 180]  # keep alpha fixed

@st.cache_data(ttl=30)
def load_data():
    query = f"SELECT * FROM `{FULL_TABLE}`"
    return client.query(query).to_dataframe()

df = load_data()

# ---------- PULL TIME----------
latest_updated_time = df['pull_time'].max()
st.info(f"Data last updated from TFL: {latest_updated_time.strftime('%Y-%m-%d %H:%M:%S')}")

# ---------- DATA PROCESSING ----------
df_filtered = df[df['timeToStation'] <= time_window * 60]
df_grouped = (
    df_filtered
    .groupby('clusterAgglomerative')['vehicleId']
    .nunique()
    .reset_index(name='buses_approaching')
    .sort_values(by='buses_approaching', ascending=False)
    .merge(
        df.groupby('clusterAgglomerative')[['latitude', 'longitude']].mean().reset_index(),
        how='left',
        on='clusterAgglomerative'
    )
)

# Normalize bus density to 0–1
scaler = MinMaxScaler()
df_grouped['density_norm'] = scaler.fit_transform(df_grouped[['buses_approaching']])

# Map density to color
df_grouped['color'] = df_grouped['density_norm'].apply(red_gray_blue)

# ---------- MAP ----------

london_tz = pytz.timezone("Europe/London")
now_in_london = datetime.now(london_tz)
current_hour = now_in_london.hour

st.subheader("📍 Map of Bus Density by Cluster")
st.pydeck_chart(
    pdk.Deck(
        map_style= "dark" if current_hour > 17 or current_hour < 6 else "light",
        initial_view_state=pdk.ViewState(
            latitude=51.5074,
            longitude=-0.1278,
            zoom=10,
            pitch=45,
        ),
        layers=[
            # Scatterplot layer for density
            pdk.Layer(
                "ScatterplotLayer",
                data=df_grouped,
                get_position='[longitude, latitude]',
                get_radius="buses_approaching * 30",
                get_fill_color='color',
                pickable=True,
                auto_highlight=True,
            ),
            # Text layer for labels
            pdk.Layer(
                "TextLayer",
                data=df_grouped,
                get_position='[longitude, latitude]',
                get_text='buses_approaching',
                get_size=16,
                get_color=[255, 255, 255],
                get_angle=0,
                get_alignment_baseline="'bottom'",
                billboard=True,
            )
        ],
        tooltip={"text": "{buses_approaching} buses approaching this area"}
    ),
    use_container_width=True,
    height=1200
)
