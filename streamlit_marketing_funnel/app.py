"""
Marketing Funnel Analytics Dashboard
=====================================
Connects to BigQuery fct_funnel_summary and fct_funnel_steps
built by the dbt pipeline.

Run locally:
    streamlit run app.py

Deploy:
    Push to GitHub → connect repo on share.streamlit.io
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from datetime import date, timedelta

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Marketing Funnel Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Import fonts */
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    /* Remove default Streamlit padding */
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

    /* KPI metric cards */
    [data-testid="metric-container"] {
        background: #f8f9fc;
        border: 1px solid #e8eaf0;
        border-radius: 12px;
        padding: 1rem 1.25rem;
    }
    [data-testid="metric-container"] label {
        font-size: 12px !important;
        font-weight: 500;
        color: #6b7280 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        font-size: 28px !important;
        font-weight: 600;
        color: #111827 !important;
    }
    [data-testid="metric-container"] [data-testid="stMetricDelta"] {
        font-size: 13px !important;
    }

    /* Section headers */
    .section-header {
        font-size: 13px;
        font-weight: 600;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        margin: 1.5rem 0 0.75rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #e8eaf0;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: #f8f9fc;
        border-right: 1px solid #e8eaf0;
    }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stMultiSelect label,
    [data-testid="stSidebar"] .stDateInput label {
        font-size: 12px;
        font-weight: 500;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
</style>
""", unsafe_allow_html=True)


# ── BigQuery connection ─────────────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    """
    Supports two auth methods:
      1. Streamlit Cloud: store your service account JSON in
         st.secrets["gcp_service_account"]
      2. Local dev: uses Application Default Credentials
         (run: gcloud auth application-default login)
    """
    if "gcp_service_account" in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        return bigquery.Client(credentials=credentials,
                               project=credentials.project_id)
    else:
        # Local: uses gcloud ADC
        project = os.environ.get("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID")
        return bigquery.Client(project=project)


# ── Data loading ────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)   # Cache for 1 hour
def load_summary(project_id: str, start_date: str, end_date: str,
                 channels: list[str]) -> pd.DataFrame:
    channel_filter = ""
    if channels:
        ch_list = ", ".join(f"'{c}'" for c in channels)
        channel_filter = f"AND channel_group IN ({ch_list})"

    query = f"""
        SELECT
            session_date,
            channel_group,
            attributed_source,
            attributed_medium,
            attributed_campaign,
            device_category,
            country,
            total_sessions,
            sessions_with_view_item,
            sessions_with_add_to_cart,
            sessions_with_checkout,
            sessions_with_payment,
            sessions_with_purchase,
            rate_session_to_page_view,
            rate_page_view_to_view_item,
            rate_view_item_to_add_to_cart,
            rate_cart_to_checkout,
            rate_checkout_to_payment,
            rate_payment_to_purchase,
            overall_conversion_rate,
            total_revenue,
            avg_order_value,
            engaged_sessions,
            engagement_rate,
            avg_session_duration_seconds
        FROM `{project_id}.marts.fct_funnel_summary`
        WHERE session_date BETWEEN '{start_date}' AND '{end_date}'
        {channel_filter}
        ORDER BY session_date
    """
    client = get_bq_client()
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_available_channels(project_id: str) -> list[str]:
    query = f"""
        SELECT DISTINCT channel_group
        FROM `{project_id}.marts.fct_funnel_summary`
        ORDER BY channel_group
    """
    client = get_bq_client()
    df = client.query(query).to_dataframe()
    return df["channel_group"].tolist()


# ── Demo data fallback ───────────────────────────────────────────────────────
def load_demo_data() -> pd.DataFrame:
    """
    Returns realistic synthetic data so the dashboard renders
    even without a BigQuery connection — useful for Streamlit Cloud
    demos where you haven't wired up credentials yet.
    """
    import numpy as np
    rng = np.random.default_rng(42)
    dates = pd.date_range("2020-11-01", "2021-01-31", freq="D")
    channels = ["Organic Search", "Paid Search", "Email",
                "Direct", "Paid Social", "Referral"]
    devices = ["desktop", "mobile", "tablet"]

    rows = []
    for d in dates:
        for ch in channels:
            sessions = int(rng.integers(80, 600))
            cr = rng.uniform(0.01, 0.06)
            purchases = max(1, int(sessions * cr))
            aov = rng.uniform(45, 180)
            rows.append({
                "session_date": d.date(),
                "channel_group": ch,
                "device_category": rng.choice(devices),
                "total_sessions": sessions,
                "sessions_with_view_item":   int(sessions * rng.uniform(0.35, 0.60)),
                "sessions_with_add_to_cart": int(sessions * rng.uniform(0.15, 0.30)),
                "sessions_with_checkout":    int(sessions * rng.uniform(0.08, 0.18)),
                "sessions_with_payment":     int(sessions * rng.uniform(0.05, 0.12)),
                "sessions_with_purchase":    purchases,
                "overall_conversion_rate":   cr,
                "rate_view_item_to_add_to_cart": rng.uniform(0.30, 0.55),
                "rate_cart_to_checkout":     rng.uniform(0.45, 0.70),
                "rate_checkout_to_payment":  rng.uniform(0.65, 0.85),
                "rate_payment_to_purchase":  rng.uniform(0.80, 0.97),
                "total_revenue":             purchases * aov,
                "avg_order_value":           aov,
                "engagement_rate":           rng.uniform(0.45, 0.75),
                "avg_session_duration_seconds": rng.uniform(90, 400),
            })
    return pd.DataFrame(rows)


# ── Chart helpers ────────────────────────────────────────────────────────────
CHART_COLORS = {
    "Organic Search": "#4F86C6",
    "Paid Search":    "#E07B4F",
    "Email":          "#5BAD8F",
    "Paid Social":    "#9B7DD4",
    "Direct":         "#7CB8C4",
    "Referral":       "#D4875B",
    "Other":          "#A8A8A8",
}

PLOTLY_LAYOUT = dict(
    font_family="DM Sans",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=0, r=0, t=28, b=0),
    legend=dict(orientation="h", y=-0.15, x=0,
                font_size=12, bgcolor="rgba(0,0,0,0)"),
)


def funnel_chart(df_agg: pd.DataFrame) -> go.Figure:
    stages = [
        ("Sessions",          df_agg["total_sessions"].sum()),
        ("Viewed product",    df_agg["sessions_with_view_item"].sum()),
        ("Added to cart",     df_agg["sessions_with_add_to_cart"].sum()),
        ("Began checkout",    df_agg["sessions_with_checkout"].sum()),
        ("Entered payment",   df_agg["sessions_with_payment"].sum()),
        ("Purchased",         df_agg["sessions_with_purchase"].sum()),
    ]
    labels, values = zip(*stages)
    pcts = [f"{v/values[0]*100:.1f}%" for v in values]

    fig = go.Figure(go.Funnel(
        y=list(labels),
        x=list(values),
        textposition="inside",
        textinfo="value+percent initial",
        hovertemplate="%{y}: %{x:,} sessions (%{percentInitial:.1%} of total)<extra></extra>",
        marker=dict(
            color=["#4F86C6", "#5A91CC", "#639BD4", "#7AAADA", "#92BCE3", "#4F86C6"],
            line=dict(width=0),
        ),
        connector=dict(line=dict(color="#e8eaf0", width=1)),
    ))
    fig.update_layout(**PLOTLY_LAYOUT, height=340)
    return fig


def conversion_trend_chart(df: pd.DataFrame) -> go.Figure:
    daily = (
        df.groupby("session_date")
          .agg(sessions=("total_sessions", "sum"),
               purchases=("sessions_with_purchase", "sum"))
          .reset_index()
    )
    daily["cvr"] = daily["purchases"] / daily["sessions"] * 100

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily["session_date"], y=daily["cvr"].round(2),
        mode="lines", name="Conversion rate %",
        line=dict(color="#4F86C6", width=2),
        fill="tozeroy",
        fillcolor="rgba(79,134,198,0.08)",
        hovertemplate="%{x}: %{y:.2f}%<extra></extra>",
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        height=220,
        yaxis=dict(ticksuffix="%", gridcolor="#f0f0f0", zeroline=False),
        xaxis=dict(gridcolor="rgba(0,0,0,0)"),
    )
    return fig


def revenue_by_channel_chart(df: pd.DataFrame) -> go.Figure:
    ch = (
        df.groupby("channel_group")["total_revenue"]
          .sum()
          .sort_values(ascending=True)
          .reset_index()
    )
    colors = [CHART_COLORS.get(c, "#A8A8A8") for c in ch["channel_group"]]

    fig = go.Figure(go.Bar(
        x=ch["total_revenue"].round(0),
        y=ch["channel_group"],
        orientation="h",
        marker_color=colors,
        text=ch["total_revenue"].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
        hovertemplate="%{y}: $%{x:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        height=280,
        xaxis=dict(showticklabels=False, showgrid=False),
        yaxis=dict(gridcolor="rgba(0,0,0,0)"),
    )
    return fig


def sessions_by_channel_trend(df: pd.DataFrame) -> go.Figure:
    daily_ch = (
        df.groupby(["session_date", "channel_group"])["total_sessions"]
          .sum()
          .reset_index()
    )
    fig = go.Figure()
    for ch in daily_ch["channel_group"].unique():
        d = daily_ch[daily_ch["channel_group"] == ch]
        fig.add_trace(go.Scatter(
            x=d["session_date"], y=d["total_sessions"],
            mode="lines", name=ch,
            line=dict(color=CHART_COLORS.get(ch, "#A8A8A8"), width=1.5),
            hovertemplate=f"{ch}: %{{y:,}}<extra></extra>",
        ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        height=260,
        yaxis=dict(gridcolor="#f0f0f0", zeroline=False),
        xaxis=dict(gridcolor="rgba(0,0,0,0)"),
    )
    return fig


def step_rates_bar(df_agg: pd.DataFrame) -> go.Figure:
    steps = [
        ("View item",    "rate_view_item_to_add_to_cart"),
        ("Add to cart",  "rate_cart_to_checkout"),
        ("Checkout",     "rate_checkout_to_payment"),
        ("Payment",      "rate_payment_to_purchase"),
    ]
    # Weighted avg across rows
    labels, rates = [], []
    for label, col in steps:
        if col in df_agg.columns:
            labels.append(label)
            rates.append(df_agg[col].mean() * 100)

    colors = ["#E07B4F" if r < 60 else "#5BAD8F" for r in rates]

    fig = go.Figure(go.Bar(
        x=labels, y=[round(r, 1) for r in rates],
        marker_color=colors,
        text=[f"{r:.1f}%" for r in rates],
        textposition="outside",
        hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        height=240,
        yaxis=dict(ticksuffix="%", range=[0, 110],
                   gridcolor="#f0f0f0", zeroline=False),
        xaxis=dict(gridcolor="rgba(0,0,0,0)"),
    )
    return fig


# ── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Funnel Analytics")
    st.markdown("---")

    use_demo = st.toggle("Use demo data", value=True,
                         help="Turn off to connect to your BigQuery project")

    if not use_demo:
        project_id = st.text_input("GCP Project ID", value="YOUR_GCP_PROJECT_ID")
    else:
        project_id = "demo"

    st.markdown("**Date range**")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", value=date(2020, 11, 1),
                                   min_value=date(2020, 11, 1),
                                   max_value=date(2021, 1, 31))
    with col2:
        end_date = st.date_input("To", value=date(2021, 1, 31),
                                 min_value=date(2020, 11, 1),
                                 max_value=date(2021, 1, 31))

    channel_options = ["Organic Search", "Paid Search", "Email",
                       "Direct", "Paid Social", "Referral", "Other"]
    selected_channels = st.multiselect(
        "Channels", options=channel_options, default=channel_options
    )

    device_options = ["desktop", "mobile", "tablet"]
    selected_devices = st.multiselect(
        "Devices", options=device_options, default=device_options
    )

    st.markdown("---")
    st.markdown(
        "<small style='color:#9ca3af'>Data: GA4 BigQuery public dataset<br>"
        "Pipeline: dbt Core + BigQuery</small>",
        unsafe_allow_html=True
    )


# ── Load data ────────────────────────────────────────────────────────────────
with st.spinner("Loading data..."):
    if use_demo:
        df_raw = load_demo_data()
    else:
        try:
            df_raw = load_summary(
                project_id, str(start_date), str(end_date), selected_channels
            )
        except Exception as e:
            st.error(f"BigQuery connection failed: {e}\n\nSwitch on **Use demo data** to preview the dashboard.")
            st.stop()

# Apply filters
df = df_raw[
    (df_raw["session_date"] >= start_date) &
    (df_raw["session_date"] <= end_date)
]
if selected_channels:
    df = df[df["channel_group"].isin(selected_channels)]
if selected_devices and "device_category" in df.columns:
    df = df[df["device_category"].isin(selected_devices)]

if df.empty:
    st.warning("No data for the selected filters.")
    st.stop()


# ── KPI row ──────────────────────────────────────────────────────────────────
total_sessions  = int(df["total_sessions"].sum())
total_purchases = int(df["sessions_with_purchase"].sum())
total_revenue   = df["total_revenue"].sum()
avg_cvr         = total_purchases / total_sessions if total_sessions else 0
avg_aov         = df["avg_order_value"].mean()

st.markdown('<p class="section-header">Overview</p>', unsafe_allow_html=True)
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total sessions",   f"{total_sessions:,}")
k2.metric("Purchases",        f"{total_purchases:,}")
k3.metric("Conversion rate",  f"{avg_cvr:.2%}")
k4.metric("Total revenue",    f"${total_revenue:,.0f}")
k5.metric("Avg order value",  f"${avg_aov:.0f}")


# ── Funnel + trend row ───────────────────────────────────────────────────────
st.markdown('<p class="section-header">Funnel</p>', unsafe_allow_html=True)
fc1, fc2 = st.columns([1, 1])

with fc1:
    st.markdown("**Stage drop-off**")
    st.plotly_chart(funnel_chart(df), use_container_width=True)

with fc2:
    st.markdown("**Step-to-step conversion rates**")
    st.plotly_chart(step_rates_bar(df), use_container_width=True)
    st.markdown("**Overall conversion rate over time**")
    st.plotly_chart(conversion_trend_chart(df), use_container_width=True)


# ── Channel row ──────────────────────────────────────────────────────────────
st.markdown('<p class="section-header">Channel attribution</p>',
            unsafe_allow_html=True)
cc1, cc2 = st.columns([1, 1])

with cc1:
    st.markdown("**Revenue by channel**")
    st.plotly_chart(revenue_by_channel_chart(df), use_container_width=True)

with cc2:
    st.markdown("**Sessions by channel over time**")
    st.plotly_chart(sessions_by_channel_trend(df), use_container_width=True)


# ── Channel breakdown table ──────────────────────────────────────────────────
st.markdown('<p class="section-header">Channel breakdown</p>',
            unsafe_allow_html=True)

ch_table = (
    df.groupby("channel_group")
      .agg(
          Sessions=("total_sessions", "sum"),
          Purchases=("sessions_with_purchase", "sum"),
          Revenue=("total_revenue", "sum"),
          Avg_CVR=("overall_conversion_rate", "mean"),
          Avg_AOV=("avg_order_value", "mean"),
      )
      .reset_index()
      .rename(columns={"channel_group": "Channel"})
      .sort_values("Revenue", ascending=False)
)
ch_table["CVR"]     = ch_table["Avg_CVR"].map("{:.2%}".format)
ch_table["AOV"]     = ch_table["Avg_AOV"].map("${:,.0f}".format)
ch_table["Revenue"] = ch_table["Revenue"].map("${:,.0f}".format)
ch_table["Sessions"] = ch_table["Sessions"].map("{:,}".format)
ch_table["Purchases"] = ch_table["Purchases"].map("{:,}".format)

st.dataframe(
    ch_table[["Channel", "Sessions", "Purchases", "CVR", "AOV", "Revenue"]],
    use_container_width=True,
    hide_index=True,
)

st.caption("Data source: GA4 BigQuery public dataset · Pipeline: dbt Core · Dashboard: Streamlit")
