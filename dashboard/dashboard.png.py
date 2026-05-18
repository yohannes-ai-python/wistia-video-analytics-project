import streamlit as st
import pandas as pd
from pyathena import connect

st.set_page_config(page_title="Wistia Dashboard", layout="wide")

st.title("Wistia Video Analytics Dashboard")
st.write("AWS S3 Curated Layer → Athena → Streamlit")

conn = connect(
    s3_staging_dir="s3://wistia-video-analytics-yoh/athena-query-results/",
    region_name="us-east-1"
)

@st.cache_data(ttl=300)
def run_query(sql):
    return pd.read_sql(sql, conn)

# =========================
# 1. Main Fact Table
# =========================
fact_sql = """
SELECT *
FROM wistia_analytics_db.fact_visitor_engagement
"""

df_fact = run_query(fact_sql)

# =========================
# 2. KPI Metrics
# =========================
total_events = len(df_fact)
total_videos = df_fact["media_id"].nunique() if "media_id" in df_fact.columns else 0
total_visitors = df_fact["visitor_id"].nunique() if "visitor_id" in df_fact.columns else 0

col1, col2, col3 = st.columns(3)

col1.metric("Total Events", total_events)
col2.metric("Unique Videos", total_videos)
col3.metric("Unique Visitors", total_visitors)

st.divider()

# =========================
# 3. Visitors by Media
# =========================
if "media_id" in df_fact.columns and "visitor_id" in df_fact.columns:
    st.subheader("Visitors by Media")

    visitors_by_media = (
        df_fact.groupby("media_id")["visitor_id"]
        .nunique()
        .reset_index()
        .rename(columns={"visitor_id": "unique_visitors"})
    )

    st.bar_chart(
        visitors_by_media,
        x="media_id",
        y="unique_visitors"
    )

    st.dataframe(visitors_by_media, use_container_width=True)

st.divider()

# =========================
# 4. Visitors by Channel
# =========================
try:
    channel_sql = """
    SELECT *
    FROM wistia_analytics_db.visitors_in_each_channel
    """

    df_channel = run_query(channel_sql)

    st.subheader("Visitors by Channel")
    st.dataframe(df_channel, use_container_width=True)

    if "channel" in df_channel.columns:
        numeric_cols = df_channel.select_dtypes(include="number").columns.tolist()

        if numeric_cols:
            st.bar_chart(
                df_channel,
                x="channel",
                y=numeric_cols[0]
            )

except Exception as e:
    st.warning(f"Channel KPI table not available: {e}")

st.divider()

# =========================
# 5. Visitors by Browser
# =========================
try:
    browser_sql = """
    SELECT *
    FROM wistia_analytics_db.visitors_in_each_browser
    """

    df_browser = run_query(browser_sql)

    st.subheader("Visitors by Browser")
    st.dataframe(df_browser, use_container_width=True)

    if "browser" in df_browser.columns:
        numeric_cols = df_browser.select_dtypes(include="number").columns.tolist()

        if numeric_cols:
            st.bar_chart(
                df_browser,
                x="browser",
                y=numeric_cols[0]
            )

except Exception as e:
    st.warning(f"Browser KPI table not available: {e}")

st.divider()

# =========================
# 6. Top Videos
# =========================
try:
    top_video_sql = """
    SELECT *
    FROM wistia_analytics_db.top_videos
    """

    df_top = run_query(top_video_sql)

    st.subheader("Top Videos")
    st.dataframe(df_top, use_container_width=True)

    numeric_cols = df_top.select_dtypes(include="number").columns.tolist()

    if "media_id" in df_top.columns and numeric_cols:
        st.bar_chart(
            df_top,
            x="media_id",
            y=numeric_cols[0]
        )

except Exception as e:
    st.warning(f"Top videos table not available: {e}")

st.divider()

# =========================
# 7. Raw Fact Data Preview
# =========================
st.subheader("Fact Visitor Engagement Data Preview")
st.dataframe(df_fact.head(100), use_container_width=True)