"""Streamlit dashboard for UK housing metrics."""

from __future__ import annotations

import os

import duckdb
import pandas as pd
import streamlit as st


def get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


BUCKET = get_env("HOUSEUK_CURATED_BUCKET", "quibbler-house-data-lake")
CURATED_PREFIX = get_env("HOUSEUK_CURATED_PREFIX", "curated")

SALES_PATH = f"s3://{BUCKET}/{CURATED_PREFIX}/marts/mart_area_sales_stats.parquet"
RENT_PATH = f"s3://{BUCKET}/{CURATED_PREFIX}/marts/mart_area_rent_stats.parquet"
NORMALIZED_PATH = f"s3://{BUCKET}/{CURATED_PREFIX}/marts/mart_rightmove_normalized.parquet"


@st.cache_resource
def duckdb_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    region = get_env("AWS_REGION", "eu-west-1")
    con.execute(f"SET s3_region='{region}';")
    return con


def load_table(path: str) -> pd.DataFrame:
    con = duckdb_conn()
    return con.execute(f"select * from read_parquet('{path}')").df()


st.set_page_config(page_title="HouseUK Dashboard", layout="wide")

st.title("HouseUK: Rent, Sales, and Deal Normalization")
st.caption("Reading curated Parquet outputs from S3 via DuckDB.")

col1, col2, col3 = st.columns(3)
col1.metric("Curated Bucket", BUCKET)
col2.metric("Curated Prefix", CURATED_PREFIX)
col3.metric("Region", get_env("AWS_REGION", "eu-west-1"))

tabs = st.tabs(["Sales", "Rent", "Rightmove Deals"])

with tabs[0]:
    st.subheader("Sales Price History (Area)")
    try:
        sales = load_table(SALES_PATH)
        st.dataframe(sales.head(200), use_container_width=True)
        if not sales.empty:
            chart = (
                sales.groupby("month", as_index=False)["median_price"].mean().sort_values("month")
            )
            st.line_chart(chart, x="month", y="median_price")
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not load sales stats. {exc}")

with tabs[1]:
    st.subheader("Rental Price History (Area + Bedrooms)")
    try:
        rent = load_table(RENT_PATH)
        st.dataframe(rent.head(200), use_container_width=True)
        if not rent.empty:
            chart = (
                rent.groupby("month", as_index=False)["median_rent"].mean().sort_values("month")
            )
            st.line_chart(chart, x="month", y="median_rent")
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not load rent stats. {exc}")

with tabs[2]:
    st.subheader("Rightmove Normalized Deals")
    try:
        deals = load_table(NORMALIZED_PATH)
        st.dataframe(deals.head(200), use_container_width=True)
        if "cheap_sale_flag" in deals.columns:
            st.write("Cheap sale deals", deals["cheap_sale_flag"].sum())
        if "cheap_rent_flag" in deals.columns:
            st.write("Cheap rent deals", deals["cheap_rent_flag"].sum())
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not load normalized Rightmove data. {exc}")
