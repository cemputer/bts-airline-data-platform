import os
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Must be the first Streamlit command
st.set_page_config(page_title="BTS Airline On-Time Performance", layout="wide")

# Load environment variables from .env
load_dotenv()

# --- BigQuery client setup ---
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

DATASET = f"{PROJECT_ID}.bts_dbt"

# --- Data loading functions ---
@st.cache_data(ttl=3600)
def load_delay_by_carrier() -> pd.DataFrame:
    query = f"SELECT * FROM `{DATASET}.mart_delay_by_carrier`"
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_delay_root_causes() -> pd.DataFrame:
    query = f"SELECT * FROM `{DATASET}.mart_delay_root_causes`"
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_airport_bottlenecks() -> pd.DataFrame:
    query = f"SELECT * FROM `{DATASET}.mart_airport_bottlenecks`"
    return client.query(query).to_dataframe()

# --- Load data ---
df_carrier = load_delay_by_carrier()
df_roots = load_delay_root_causes()
df_airports = load_airport_bottlenecks()

# ============================================================
# HEADER
# ============================================================
st.title("✈️ BTS Airline On-Time Performance Dashboard")
st.caption("Data source: Bureau of Transportation Statistics")

# ============================================================
# KPI CARDS
# ============================================================
st.subheader("Overview")

total_flights = int(df_carrier["total_flights"].sum())

total_delayed = (
    df_carrier["total_minor_delayed"]
    + df_carrier["total_major_delayed"]
    + df_carrier["total_severe_delayed"]
).sum()
delay_rate = round(total_delayed / total_flights * 100, 1)

cancel_rate = round(
    df_carrier["total_cancelled_flights"].sum() / total_flights * 100, 1
)

worst_airline = (
    df_carrier.groupby("reporting_airline")["avg_arr_delay_mins"]
    .mean()
    .idxmax()
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Flights", f"{total_flights:,}")
col2.metric("Delay Rate", f"{delay_rate}%")
col3.metric("Cancellation Rate", f"{cancel_rate}%")
col4.metric("Worst Airline (Arr. Delay)", worst_airline)

st.divider()

# ============================================================
# TILE 1 — Delay Root Cause Distribution (stacked bar)
# ============================================================
st.subheader("Delay Root Cause Distribution by Airline")

# Sort airlines by total delayed flights descending
df_roots_sorted = df_roots.sort_values("total_delayed_flights", ascending=False)

fig_roots = px.bar(
    df_roots_sorted,
    x="reporting_airline",
    y=[
        "carrier_delay_pct",
        "weather_delay_pct",
        "nas_delay_pct",
        "security_delay_pct",
        "late_aircraft_delay_pct",
    ],
    # Percentage-based: normalizes across airlines of different sizes
    labels={"value": "Delay Share (%)", "reporting_airline": "Airline", "variable": "Cause"},
    color_discrete_sequence=px.colors.qualitative.Set2,
)
fig_roots.update_layout(barmode="stack", legend_title="Delay Cause", height=420)
st.plotly_chart(fig_roots, use_container_width=True)

st.divider()

# ============================================================
# TILE 2 — Monthly Trend (sidebar year filter)
# ============================================================
st.subheader("Monthly Delay & Cancellation Trend")

# Sidebar: year selector — no "All" option, default 2024
selected_year = st.sidebar.selectbox(
    "Select Year",
    options=[2023, 2024, 2025],
    index=1,  # default: 2024
)

df_year = df_carrier[df_carrier["flight_year"] == selected_year].copy()

# Aggregate across all airlines per month
df_monthly = (
    df_year.groupby("flight_month")
    .agg(
        total_flights=("total_flights", "sum"),
        total_minor=("total_minor_delayed", "sum"),
        total_major=("total_major_delayed", "sum"),
        total_severe=("total_severe_delayed", "sum"),
        total_cancelled=("total_cancelled_flights", "sum"),
    )
    .reset_index()
    .sort_values("flight_month")
)

# Derive delay rate: not pre-computed in mart, calculated here
df_monthly["delay_rate_pct"] = (
    (df_monthly["total_minor"] + df_monthly["total_major"] + df_monthly["total_severe"])
    / df_monthly["total_flights"]
    * 100
).round(1)

df_monthly["cancel_rate_pct"] = (
    df_monthly["total_cancelled"] / df_monthly["total_flights"] * 100
).round(2)

# Map month numbers to names for readable x-axis
month_names = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}
df_monthly["month_name"] = df_monthly["flight_month"].map(month_names)

# Side-by-side charts: dual-axis avoided because scales differ (~20-40% vs ~1-3%)
left, right = st.columns(2)

with left:
    fig_delay = px.line(
        df_monthly,
        x="month_name",
        y="delay_rate_pct",
        markers=True,
        title=f"Monthly Delay Rate (%) — {selected_year}",
        labels={"month_name": "Month", "delay_rate_pct": "Delay Rate (%)"},
    )
    fig_delay.update_layout(height=380)
    st.plotly_chart(fig_delay, use_container_width=True)

with right:
    fig_cancel = px.line(
        df_monthly,
        x="month_name",
        y="cancel_rate_pct",
        markers=True,
        title=f"Monthly Cancellation Rate (%) — {selected_year}",
        labels={"month_name": "Month", "cancel_rate_pct": "Cancellation Rate (%)"},
        color_discrete_sequence=["#e45756"],
    )
    fig_cancel.update_layout(height=380)
    st.plotly_chart(fig_cancel, use_container_width=True)

st.divider()

# ============================================================
# TILE 3 — Airport Bottlenecks
# ============================================================
st.subheader("Top 10 Airports by Traffic")

# Top 10 airports sorted by total traffic
df_top10 = (
    df_airports.sort_values("total_traffic", ascending=False)
    .head(10)[["airport_code", "city_name", "total_traffic", "avg_taxi_out_mins", "avg_arr_delay_mins"]]
    .reset_index(drop=True)
)

left2, right2 = st.columns(2)

with left2:
    # Heat coloring on avg_arr_delay_mins: red = worse performance
    styled = df_top10.style.background_gradient(
        subset=["avg_arr_delay_mins"], cmap="RdYlGn_r"
    ).format({"total_traffic": "{:,}", "avg_taxi_out_mins": "{:.1f}", "avg_arr_delay_mins": "{:.1f}"})
    st.dataframe(styled, use_container_width=True, hide_index=True)

with right2:
    fig_airports = px.bar(
        df_top10.sort_values("avg_arr_delay_mins"),
        x="avg_arr_delay_mins",
        y="airport_code",
        orientation="h",
        title="Avg. Arrival Delay by Airport (minutes)",
        labels={"avg_arr_delay_mins": "Avg Arrival Delay (min)", "airport_code": "Airport"},
        color="avg_arr_delay_mins",
        color_continuous_scale="RdYlGn_r",
    )
    fig_airports.update_layout(height=380, coloraxis_showscale=False)
    st.plotly_chart(fig_airports, use_container_width=True)