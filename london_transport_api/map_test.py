import streamlit as st
import pydeck as pdk
import pandas as pd

# Read your data
df = pd.read_csv("/Users/delgadonoriega/Desktop/gcp-data-eng-bootcamp/Module_5_streaming_project/london_transport_api/bus_stops.csv")
df = df[['commonName', 'latitude', 'longitude']]

# Assign a color per commonName
# You can customize this with your own palette
name_to_color = {name: [int(hash(name) % 255), int((hash(name)*3) % 255), int((hash(name)*7) % 255)] for name in df['commonName'].unique()}
df['color'] = df['commonName'].map(name_to_color)

# Define the PyDeck layer
layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position='[longitude, latitude]',
    get_fill_color='color',
    get_radius=80,
    pickable=True,
    auto_highlight=True
)

# Define view
view_state = pdk.ViewState(
    latitude=df['latitude'].mean(),
    longitude=df['longitude'].mean(),
    zoom=11,
    pitch=0
)

# Render
st.title("🚌 Real-time Bus Density Map (Colored by commonName)")
st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip={"text": "{commonName}"}))
