"""
Real-Time Crime Analytics — Streamlit Dashboard
================================================
Reads directly from PostgreSQL (bigdata db).
No Kafka / Spark / Storm needed — all data is pre-computed.

Run:
    pip install streamlit psycopg2-binary plotly pandas
    streamlit run dashboard.py
"""

import time
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from psycopg2.extras import RealDictCursor

# ── Page config ───────────────────────────────────────────
st.set_page_config(
    page_title="Chicago Crime Analytics",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme colours ─────────────────────────────────────────
PRIMARY   = "#0054A6"
SECONDARY = "#DC2626"
ACCENT    = "#059669"
WARNING   = "#D97706"
NEUTRAL   = "#6B7280"

# ── DB connection ─────────────────────────────────────────
DB_CFG = dict(
    host="localhost",
    port=5432,
    dbname="bigdata",
    user="postgres",
    password="123456789",
)

@st.cache_resource
def get_conn():
    return psycopg2.connect(**DB_CFG)

def query(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    except Exception:
        conn.rollback()
        raise

# ── Sidebar navigation ────────────────────────────────────
st.sidebar.image(
    "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a7/Camponotus_flavomarginatus_ant.jpg/1px-blank.png",
    width=1,
)
st.sidebar.markdown(
    f"""
    <div style='text-align:center; padding:10px 0 20px 0;'>
        <span style='font-size:2rem;'>🚨</span><br>
        <span style='font-size:1.2rem; font-weight:700; color:{PRIMARY};'>
            Chicago Crime Analytics
        </span><br>
        <span style='font-size:0.75rem; color:{NEUTRAL};'>Lambda Architecture Dashboard</span>
    </div>
    """,
    unsafe_allow_html=True,
)

page = st.sidebar.radio(
    "Navigate",
    [
        "📊 Overview",
        "📈 Crime Trends",
        "🔒 Arrest Analysis",
        "💥 Violence & Gunshots",
        "🗺️ Crime Hotspots",
        "👤 Sex Offender Density",
        "⚡ Live Alerts",
    ],
)

st.sidebar.markdown("---")
st.sidebar.caption("FAST NUCES — Big Data Analytics")
st.sidebar.caption("Muhammad Ali · Atif Ibrahim Abbasi")

# ═══════════════════════════════════════════════════════════
#  PAGE 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════
if page == "📊 Overview":
    st.markdown(
        f"<h1 style='color:{PRIMARY};'>🚨 Real-Time Crime Analytics Dashboard</h1>"
        f"<p style='color:{NEUTRAL};'>Chicago Open Data · Lambda Architecture · Apache Spark + Kafka + Storm</p>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # KPI row
    total_crimes   = query("SELECT SUM(crime_count) AS n FROM crime_trends_by_year").iloc[0]["n"]
    total_alerts   = query("SELECT COUNT(*) AS n FROM speed_layer_alerts").iloc[0]["n"]
    critical_alerts = query("SELECT COUNT(*) AS n FROM speed_layer_alerts WHERE severity='CRITICAL'").iloc[0]["n"]
    hotspot_count  = query("SELECT COUNT(*) AS n FROM hotspots").iloc[0]["n"]
    top_district   = query("SELECT district FROM arrest_rate_by_district ORDER BY arrest_rate DESC LIMIT 1").iloc[0]["district"]
    gunshot_pct    = query("SELECT gunshot_proportion FROM gunshot_injury_proportion").iloc[0]["gunshot_proportion"]

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    def kpi(col, label, value, colour=PRIMARY, suffix=""):
        col.markdown(
            f"""<div style='background:#F8FAFC; border-left:4px solid {colour};
                            padding:14px 12px; border-radius:6px; text-align:center;'>
                <div style='font-size:1.6rem; font-weight:800; color:{colour};'>{value}{suffix}</div>
                <div style='font-size:0.72rem; color:{NEUTRAL}; margin-top:4px;'>{label}</div>
            </div>""",
            unsafe_allow_html=True,
        )

    kpi(c1, "Total Crime Records",  f"{int(total_crimes):,}")
    kpi(c2, "Speed Layer Alerts",   f"{int(total_alerts):,}",   SECONDARY)
    kpi(c3, "Critical Alerts",      f"{int(critical_alerts):,}", SECONDARY)
    kpi(c4, "ML Hotspot Clusters",  f"{int(hotspot_count)}",    ACCENT)
    kpi(c5, "Highest Arrest District", f"Dist {top_district}",  WARNING)
    kpi(c6, "Gunshot Injury Rate",  f"{float(gunshot_pct)*100:.1f}", NEUTRAL, "%")

    st.markdown("<br>", unsafe_allow_html=True)

    # Two-column charts
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Crimes by Hour of Day")
        df_h = query("SELECT hour_of_day, crime_count FROM crime_trends_by_hour ORDER BY hour_of_day")
        fig = px.bar(df_h, x="hour_of_day", y="crime_count",
                     color="crime_count", color_continuous_scale="Blues",
                     labels={"hour_of_day": "Hour", "crime_count": "Crimes"})
        fig.update_layout(showlegend=False, coloraxis_showscale=False,
                          margin=dict(t=10, b=10), height=280)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Crimes by Day of Week")
        df_d = query("SELECT day_of_week, crime_count FROM crime_trends_by_day_of_week ORDER BY day_of_week")
        days = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
        df_d["day_name"] = df_d["day_of_week"].map(days)
        fig = px.bar(df_d, x="day_name", y="crime_count",
                     color="crime_count", color_continuous_scale="Reds",
                     labels={"day_name": "Day", "crime_count": "Crimes"},
                     category_orders={"day_name": list(days.values())})
        fig.update_layout(showlegend=False, coloraxis_showscale=False,
                          margin=dict(t=10, b=10), height=280)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Alert Severity Distribution")
    df_sev = query("SELECT severity, COUNT(*) AS count FROM speed_layer_alerts GROUP BY severity")
    colour_map = {"CRITICAL": SECONDARY, "HIGH": WARNING, "MEDIUM": "#F59E0B", "LOW": ACCENT}
    fig = px.pie(df_sev, names="severity", values="count",
                 color="severity", color_discrete_map=colour_map,
                 hole=0.45)
    fig.update_layout(height=320, margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════
#  PAGE 2 — CRIME TRENDS
# ═══════════════════════════════════════════════════════════
elif page == "📈 Crime Trends":
    st.markdown(f"<h1 style='color:{PRIMARY};'>📈 Crime Trends</h1>", unsafe_allow_html=True)
    st.markdown("Temporal patterns computed by the Apache Spark batch layer over 50,000 records.")
    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["By Month", "By Hour", "By Day of Week"])

    with tab1:
        df_m = query("""
            SELECT year || '-' || LPAD(month::text, 2, '0') AS period,
                   crime_count
            FROM crime_trends_by_month
            ORDER BY year, month
        """)
        fig = px.area(df_m, x="period", y="crime_count",
                      title="Monthly Crime Volume",
                      labels={"period": "Month", "crime_count": "Crime Count"},
                      color_discrete_sequence=[PRIMARY])
        fig.update_traces(fill="tozeroy", fillcolor=f"rgba(0,84,166,0.15)")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_m.rename(columns={"period": "Month", "crime_count": "Crimes"}),
                     use_container_width=True, hide_index=True)

    with tab2:
        df_h = query("SELECT hour_of_day, crime_count FROM crime_trends_by_hour ORDER BY hour_of_day")
        fig = make_subplots(rows=1, cols=1)
        fig.add_trace(go.Bar(
            x=df_h["hour_of_day"], y=df_h["crime_count"],
            marker_color=[SECONDARY if v == df_h["crime_count"].max() else PRIMARY
                          for v in df_h["crime_count"]],
            name="Crimes",
        ))
        fig.update_layout(
            title="Crimes by Hour of Day (0=Midnight, 12=Noon)",
            xaxis_title="Hour", yaxis_title="Crime Count",
            height=400,
            xaxis=dict(tickmode="linear", tick0=0, dtick=1),
        )
        st.plotly_chart(fig, use_container_width=True)

        peak_hour = df_h.loc[df_h["crime_count"].idxmax(), "hour_of_day"]
        low_hour  = df_h.loc[df_h["crime_count"].idxmin(), "hour_of_day"]
        st.info(f"🔴 Peak crime hour: **{int(peak_hour):02d}:00** "
                f"({int(df_h['crime_count'].max()):,} crimes)   "
                f"🟢 Lowest crime hour: **{int(low_hour):02d}:00** "
                f"({int(df_h['crime_count'].min()):,} crimes)")

    with tab3:
        df_d = query("SELECT day_of_week, crime_count FROM crime_trends_by_day_of_week ORDER BY day_of_week")
        days = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
        df_d["day_name"] = df_d["day_of_week"].map(days)
        fig = px.bar(df_d, x="day_name", y="crime_count",
                     color="crime_count",
                     color_continuous_scale=["#DBEAFE", PRIMARY],
                     labels={"day_name": "Day", "crime_count": "Crimes"},
                     category_orders={"day_name": list(days.values())},
                     text="crime_count")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=400, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════
#  PAGE 3 — ARREST ANALYSIS
# ═══════════════════════════════════════════════════════════
elif page == "🔒 Arrest Analysis":
    st.markdown(f"<h1 style='color:{PRIMARY};'>🔒 Arrest Rate Analysis</h1>", unsafe_allow_html=True)
    st.markdown("Join of crimes ↔ arrests datasets on `case_number`, computed in Apache Spark.")
    st.markdown("---")

    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.subheader("Top 10 Crime Types by Arrest Rate")
        df_top = query("""
            SELECT primary_type, total_crimes, total_arrests,
                   ROUND(arrest_rate::numeric * 100, 1) AS arrest_pct
            FROM top10_crime_types_by_arrest_rate
            ORDER BY arrest_rate DESC
        """)
        fig = px.bar(df_top, x="arrest_pct", y="primary_type",
                     orientation="h",
                     color="arrest_pct", color_continuous_scale=["#FEE2E2", SECONDARY],
                     text="arrest_pct",
                     labels={"arrest_pct": "Arrest Rate (%)", "primary_type": "Crime Type"})
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(height=420, coloraxis_showscale=False,
                          yaxis=dict(categoryorder="total ascending"))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Arrests by Race")
        df_race = query("SELECT arrestee_race, total_arrests FROM arrests_by_race ORDER BY total_arrests DESC")
        fig = px.pie(df_race, names="arrestee_race", values="total_arrests",
                     hole=0.4,
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=420, legend=dict(orientation="v", x=1, y=0.5))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Arrest Rate by Police District")
    df_dist = query("""
        SELECT district,
               total_crimes,
               total_arrests,
               ROUND(arrest_rate::numeric * 100, 1) AS arrest_pct
        FROM arrest_rate_by_district
        ORDER BY arrest_rate DESC
    """)
    fig = px.bar(df_dist, x="district", y="arrest_pct",
                 color="arrest_pct", color_continuous_scale=["#DBEAFE", PRIMARY],
                 labels={"district": "District", "arrest_pct": "Arrest Rate (%)"},
                 hover_data={"total_crimes": True, "total_arrests": True})
    fig.update_layout(height=350, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw data — all crime types"):
        df_all = query("""
            SELECT primary_type AS "Crime Type",
                   total_crimes AS "Total Crimes",
                   total_arrests AS "Total Arrests",
                   ROUND(arrest_rate::numeric * 100, 2) AS "Arrest Rate (%)"
            FROM arrest_rate_by_crime_type
            ORDER BY arrest_rate DESC
        """)
        st.dataframe(df_all, use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════
#  PAGE 4 — VIOLENCE & GUNSHOTS
# ═══════════════════════════════════════════════════════════
elif page == "💥 Violence & Gunshots":
    st.markdown(f"<h1 style='color:{SECONDARY};'>💥 Violence & Gunshot Analysis</h1>", unsafe_allow_html=True)
    st.markdown("CPD Violence Reduction Strategic Initiative dataset — 10,000 records.")
    st.markdown("---")

    # Global gunshot KPI
    df_gun = query("SELECT total_incidents, gunshot_yes, gunshot_proportion FROM gunshot_injury_proportion")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Violence Incidents", f"{int(df_gun['total_incidents'][0]):,}")
    col2.metric("Confirmed Gunshot Injuries", f"{int(df_gun['gunshot_yes'][0]):,}")
    col3.metric("Gunshot Injury Rate", f"{float(df_gun['gunshot_proportion'][0])*100:.1f}%")

    st.markdown("<br>", unsafe_allow_html=True)

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Violence by Type")
        df_type = query("""
            SELECT victimization_primary,
                   SUM(incident_count) AS total
            FROM violence_by_month
            GROUP BY victimization_primary
            ORDER BY total DESC
        """)
        fig = px.bar(df_type, x="victimization_primary", y="total",
                     color="victimization_primary",
                     color_discrete_sequence=[SECONDARY, WARNING, PRIMARY, ACCENT, NEUTRAL],
                     labels={"victimization_primary": "Type", "total": "Incidents"},
                     text="total")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=380, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Top 10 Districts by Violence")
        df_vd = query("""
            SELECT district,
                   SUM(incident_count) AS total
            FROM violence_by_district
            GROUP BY district
            ORDER BY total DESC
            LIMIT 10
        """)
        fig = px.bar(df_vd, x="district", y="total",
                     color="total", color_continuous_scale=["#FEE2E2", SECONDARY],
                     labels={"district": "District", "total": "Incidents"},
                     text="total")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(height=380, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Gunshot Injury Proportion by District")
    df_gd = query("""
        SELECT district,
               total_incidents,
               gunshot_yes,
               ROUND(gunshot_proportion::numeric * 100, 1) AS gunshot_pct
        FROM gunshot_proportion_by_district
        WHERE district IS NOT NULL AND district != ''
        ORDER BY gunshot_pct DESC
        LIMIT 15
    """)
    fig = px.bar(df_gd, x="district", y="gunshot_pct",
                 color="gunshot_pct",
                 color_continuous_scale=["#FEF3C7", SECONDARY],
                 labels={"district": "District", "gunshot_pct": "Gunshot Rate (%)"},
                 hover_data={"total_incidents": True, "gunshot_yes": True},
                 text="gunshot_pct")
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(height=360, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Top Community Areas by Violence")
    df_ca = query("""
        SELECT community_area, total_incidents, homicides, non_fatal
        FROM top_community_areas_by_violence
        ORDER BY total_incidents DESC
        LIMIT 15
    """)
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Homicides", x=df_ca["community_area"],
                         y=df_ca["homicides"], marker_color=SECONDARY))
    fig.add_trace(go.Bar(name="Non-Fatal", x=df_ca["community_area"],
                         y=df_ca["non_fatal"], marker_color=WARNING))
    fig.update_layout(barmode="stack", height=400,
                      xaxis_title="Community Area", yaxis_title="Incidents",
                      legend=dict(orientation="h", y=1.05))
    st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════
#  PAGE 5 — CRIME HOTSPOTS (ML)
# ═══════════════════════════════════════════════════════════
elif page == "🗺️ Crime Hotspots":
    st.markdown(f"<h1 style='color:{ACCENT};'>🗺️ Crime Hotspot Map</h1>", unsafe_allow_html=True)
    st.markdown("KMeans clustering (k=10) on geo-located crime records using PySpark MLlib.")
    st.markdown("---")

    df_hs = query("""
        SELECT cluster_id,
               centroid_latitude  AS lat,
               centroid_longitude AS lon,
               crime_count
        FROM hotspots
        ORDER BY crime_count DESC
    """)

    df_hs["size"]  = df_hs["crime_count"] / df_hs["crime_count"].max() * 50
    df_hs["label"] = df_hs.apply(
        lambda r: f"Cluster {int(r['cluster_id'])} — {int(r['crime_count']):,} crimes", axis=1
    )

    fig = px.scatter_mapbox(
        df_hs,
        lat="lat", lon="lon",
        size="size",
        color="crime_count",
        color_continuous_scale=["#FEF3C7", WARNING, SECONDARY],
        hover_name="label",
        hover_data={"crime_count": True, "lat": ":.4f", "lon": ":.4f", "size": False},
        zoom=10,
        mapbox_style="carto-positron",
        title="KMeans Hotspot Centroids (bubble size ∝ crime count)",
        height=550,
    )
    fig.update_layout(margin=dict(t=40, b=0, l=0, r=0))
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Cluster Statistics")
    df_display = df_hs[["cluster_id", "lat", "lon", "crime_count"]].copy()
    df_display.columns = ["Cluster ID", "Centroid Lat", "Centroid Lon", "Crimes Assigned"]
    df_display["% of Total"] = (df_display["Crimes Assigned"] / df_display["Crimes Assigned"].sum() * 100).round(1)
    df_display["Crimes Assigned"] = df_display["Crimes Assigned"].apply(lambda x: f"{int(x):,}")
    df_display["% of Total"] = df_display["% of Total"].apply(lambda x: f"{x}%")
    st.dataframe(df_display.reset_index(drop=True), use_container_width=True, hide_index=True)

    st.info(
        "🔴 **Densest hotspot** (Cluster 5): 7,873 crimes centred at 41.877°N, -87.639°W "
        "— corresponds to the **Near West Side / Loop area** of Chicago.\n\n"
        "🔵 **Sparsest cluster** (Cluster 2): only 704 crimes in far north-west Chicago."
    )

# ═══════════════════════════════════════════════════════════
#  PAGE 6 — SEX OFFENDER DENSITY
# ═══════════════════════════════════════════════════════════
elif page == "👤 Sex Offender Density":
    st.markdown(f"<h1 style='color:{WARNING};'>👤 Sex Offender Density by District</h1>",
                unsafe_allow_html=True)
    st.markdown(
        "Offenders mapped to police districts via block→district lookup derived from the crimes dataset."
    )
    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        df_od = query("""
            SELECT district, offender_count
            FROM sex_offender_density_by_district
            WHERE district IS NOT NULL
            ORDER BY offender_count DESC
            LIMIT 15
        """)
        fig = px.bar(df_od, x="offender_count", y="district",
                     orientation="h",
                     color="offender_count",
                     color_continuous_scale=["#FEF3C7", WARNING],
                     text="offender_count",
                     labels={"offender_count": "Offenders", "district": "District"})
        fig.update_traces(textposition="outside")
        fig.update_layout(height=500, coloraxis_showscale=False,
                          yaxis=dict(categoryorder="total ascending"),
                          title="Registered Offenders per District")
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Cross-Dataset Correlation")
        st.caption("Arrest rate vs. sex offender density per community area (top 15 by crime volume)")
        df_corr = query("""
            SELECT community_area,
                   total_crimes,
                   offender_count,
                   ROUND(offender_density_per_100_crimes::numeric, 2) AS density_per_100
            FROM community_offender_vs_crime_correlation
            ORDER BY total_crimes DESC
            LIMIT 15
        """)
        fig = px.scatter(df_corr, x="total_crimes", y="density_per_100",
                         text="community_area",
                         size="offender_count",
                         color="offender_count",
                         color_continuous_scale=["#FEF3C7", WARNING],
                         labels={"total_crimes": "Total Crimes",
                                 "density_per_100": "Offenders per 100 Crimes",
                                 "offender_count": "Offender Count"},
                         height=500)
        fig.update_traces(textposition="top center", textfont_size=9)
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Priority: Offenders with Minor Victims")
    df_minor = query("""
        SELECT last || ', ' || first AS name,
               block, gender, race, victim_minor
        FROM sex_offenders_minor_victims_priority
        LIMIT 50
    """)
    st.warning(f"⚠️ {len(df_minor)} priority records found where VICTIM_MINOR = 'Y'")
    st.dataframe(
        df_minor.rename(columns={"name": "Name", "block": "Block",
                                  "gender": "Gender", "race": "Race",
                                  "victim_minor": "Minor Victim"}),
        use_container_width=True, hide_index=True
    )

# ═══════════════════════════════════════════════════════════
#  PAGE 7 — LIVE ALERTS (Speed Layer)
# ═══════════════════════════════════════════════════════════
elif page == "⚡ Live Alerts":
    st.markdown(f"<h1 style='color:{SECONDARY};'>⚡ Speed Layer — Live Anomaly Alerts</h1>",
                unsafe_allow_html=True)
    st.markdown(
        "Alerts generated by the Storm topology: "
        "**KafkaSpout → ParseBolt → DistrictBolt → WindowBolt → AnomalyBolt → AlertBolt**"
    )
    st.markdown("---")

    # Controls
    col_ctrl1, col_ctrl2, col_ctrl3, col_ctrl4 = st.columns([1, 1, 1, 2])
    severity_filter = col_ctrl1.selectbox("Severity", ["All", "CRITICAL", "HIGH", "MEDIUM", "LOW"])
    district_opts   = ["All"] + sorted(
        query("SELECT DISTINCT district FROM speed_layer_alerts WHERE district IS NOT NULL ORDER BY district")
        ["district"].tolist()
    )
    district_filter = col_ctrl2.selectbox("District", district_opts)
    limit           = col_ctrl3.slider("Max rows", 20, 500, 100, step=20)
    auto_refresh    = col_ctrl4.checkbox("🔄 Auto-refresh every 5 s", value=False)

    # KPI row
    df_kpi = query("""
        SELECT severity, COUNT(*) AS cnt
        FROM speed_layer_alerts
        GROUP BY severity
        ORDER BY cnt DESC
    """)
    kpi_map = dict(zip(df_kpi["severity"], df_kpi["cnt"]))
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("CRITICAL", f"{kpi_map.get('CRITICAL', 0):,}", delta=None)
    k2.metric("HIGH",     f"{kpi_map.get('HIGH',     0):,}", delta=None)
    k3.metric("MEDIUM",   f"{kpi_map.get('MEDIUM',   0):,}", delta=None)
    k4.metric("LOW",      f"{kpi_map.get('LOW',      0):,}", delta=None)

    st.markdown("<br>", unsafe_allow_html=True)

    # Alerts per district chart
    st.subheader("Alert Volume by District")
    df_ad = query("""
        SELECT district, severity, COUNT(*) AS cnt
        FROM speed_layer_alerts
        WHERE district IS NOT NULL
        GROUP BY district, severity
        ORDER BY district
    """)
    fig = px.bar(df_ad, x="district", y="cnt", color="severity",
                 color_discrete_map={
                     "CRITICAL": SECONDARY, "HIGH": WARNING,
                     "MEDIUM": "#F59E0B",  "LOW": ACCENT
                 },
                 labels={"cnt": "Alerts", "district": "District", "severity": "Severity"},
                 barmode="stack", height=320)
    fig.update_layout(legend=dict(orientation="h", y=1.05))
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Alert Feed")

    feed_placeholder = st.empty()

    def load_alerts():
        where_clauses = []
        params = []
        if severity_filter != "All":
            where_clauses.append("severity = %s")
            params.append(severity_filter)
        if district_filter != "All":
            where_clauses.append("district = %s")
            params.append(district_filter)
        where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        sql = f"""
            SELECT alert_id, district, severity,
                   crime_count_in_window,
                   window_seconds,
                   threshold,
                   latest_crime_type,
                   latest_case_number,
                   TO_CHAR(triggered_at, 'YYYY-MM-DD HH24:MI:SS') AS triggered_at
            FROM speed_layer_alerts
            {where}
            ORDER BY triggered_at DESC
            LIMIT %s
        """
        params.append(limit)
        return query(sql, params)

    def render_alerts(df):
        severity_emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}
        with feed_placeholder.container():
            st.dataframe(
                df.rename(columns={
                    "alert_id": "Alert ID",
                    "district": "District",
                    "severity": "Severity",
                    "crime_count_in_window": "Crimes in Window",
                    "window_seconds": "Window (s)",
                    "threshold": "Threshold",
                    "latest_crime_type": "Latest Crime Type",
                    "latest_case_number": "Case No.",
                    "triggered_at": "Triggered At",
                }),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Severity": st.column_config.TextColumn(
                        "Severity",
                        help="Alert severity level",
                    ),
                    "Crimes in Window": st.column_config.ProgressColumn(
                        "Crimes in 5-min Window",
                        min_value=0, max_value=120, format="%d",
                    ),
                },
            )

    df_alerts = load_alerts()
    render_alerts(df_alerts)

    if auto_refresh:
        time.sleep(5)
        st.rerun()
