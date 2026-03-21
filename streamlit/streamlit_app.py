"""
NeighbourWise AI — Unified Neighborhood Intelligence Dashboard
Combines Crime, Grocery, and Healthcare analytics in a single tabbed interface.
Runs natively in Snowflake (Streamlit in Snowflake).
"""

import streamlit as st
import pandas as pd
import altair as alt
import pydeck as pdk
import json
import re
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(
    page_title="NeighbourWise AI — Neighborhood Intelligence",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── UNIFIED STYLING ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .main { background-color: #fafaf7; padding-top: 1rem; }
    .block-container { padding-top: 1.2rem; padding-bottom: 2rem; padding-left: 2rem; padding-right: 2rem; }
    .hero-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d6a4f 45%, #52b788 100%);
        padding: 2rem 2.2rem; border-radius: 20px; color: white; margin-bottom: 1.4rem;
        box-shadow: 0 12px 32px rgba(30, 58, 95, 0.25); position: relative; overflow: hidden;
    }
    .hero-card::before { content: '🏘️'; position: absolute; right: 2rem; top: 50%; transform: translateY(-50%); font-size: 6rem; opacity: 0.12; }
    .hero-title { font-family: 'DM Serif Display', serif; font-size: 2.1rem; font-weight: 400; margin-bottom: 0.4rem; letter-spacing: -0.01em; }
    .hero-subtitle { font-size: 0.98rem; opacity: 0.9; line-height: 1.6; max-width: 680px; }
    .metric-card { background: white; padding: 1.1rem 1.3rem; border-radius: 16px; border: 1px solid #e5e7eb; box-shadow: 0 4px 12px rgba(0,0,0,0.05); margin-bottom: 1rem; }
    .metric-label { font-size: 0.82rem; font-weight: 600; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
    .metric-value { font-family: 'DM Serif Display', serif; font-size: 2rem; color: #1e3a5f; line-height: 1; }
    .metric-sub { font-size: 0.78rem; color: #9ca3af; margin-top: 0.3rem; }
    .section-card { background: white; padding: 1.2rem 1.3rem 1rem 1.3rem; border-radius: 16px; border: 1px solid #e5e7eb; box-shadow: 0 2px 10px rgba(0,0,0,0.04); margin-bottom: 1.2rem; }
    .narrative-box { background: #f0fdf4; border: 1px solid #bbf7d0; border-left: 5px solid #86C5A4; padding: 1rem 1.2rem; border-radius: 12px; margin-bottom: 1rem; font-size: 0.94rem; line-height: 1.7; color: #1f2937; }
    .narrative-box-blue { background: #eff6ff; border: 1px solid #bfdbfe; border-left: 5px solid #60a5fa; padding: 1rem 1.2rem; border-radius: 12px; margin-bottom: 1rem; font-size: 0.94rem; line-height: 1.7; color: #1f2937; }
    .narrative-neighborhood { font-family: 'DM Serif Display', serif; font-size: 1.1rem; color: #1e3a5f; margin-bottom: 0.4rem; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { height: 46px; padding-left: 18px; padding-right: 18px; border-radius: 12px; background-color: #f8fafc; font-family: 'DM Sans', sans-serif; }
    .stTabs [aria-selected="true"] { background-color: #e0f2fe !important; color: #0c4a6e !important; font-weight: 700; }
    .tier-badge { display: inline-block; padding: 0.2rem 0.6rem; border-radius: 999px; font-size: 0.78rem; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADERS — CRIME
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data
def load_crime_forecasts():
    df = session.sql("SELECT * FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_FORECAST ORDER BY NEIGHBORHOOD_NAME, FORECAST_MONTH").to_pandas()
    df.columns = [c.upper() for c in df.columns]; return df

@st.cache_data
def load_crime_hotspots():
    df = session.sql("SELECT * FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_HOTSPOT_CLUSTERS ORDER BY HOTSPOT_CRIME_SHARE_PCT DESC").to_pandas()
    df.columns = [c.upper() for c in df.columns]; return df

@st.cache_data
def load_crime_narratives():
    df = session.sql("SELECT * FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_SAFETY_NARRATIVE ORDER BY NEIGHBORHOOD_NAME").to_pandas()
    df.columns = [c.upper() for c in df.columns]; return df

@st.cache_data
def load_crime_historical():
    df = session.sql("""
        SELECT NEIGHBORHOOD_NAME, DATE_TRUNC('month', OCCURRED_ON_DATE) AS YEAR_MONTH, COUNT(*) AS CRIME_COUNT
        FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_CRIME
        WHERE VALID_LOCATION = TRUE AND OCCURRED_ON_DATE IS NOT NULL AND YEAR(OCCURRED_ON_DATE) >= 2023
          AND NEIGHBORHOOD_NAME IS NOT NULL AND DATE_TRUNC('month', OCCURRED_ON_DATE) < DATE_TRUNC('month', CURRENT_DATE())
        GROUP BY 1, 2 ORDER BY 1, 2
    """).to_pandas()
    df.columns = [c.upper() for c in df.columns]; return df

@st.cache_data
def load_neighborhood_boundary(nbhd):
    df = session.sql(f"""
        WITH geojson AS (SELECT ST_ASGEOJSON(GEOMETRY)::VARIANT AS geo FROM NEIGHBOURWISE_DOMAINS.MARTS.MASTER_LOCATION WHERE NEIGHBORHOOD_NAME = '{nbhd}'),
        coord_ring AS (SELECT CASE WHEN geo:type::STRING = 'MultiPolygon' THEN geo:coordinates[0][0] ELSE geo:coordinates[0] END AS ring FROM geojson)
        SELECT pt.value[0]::FLOAT AS LONG, pt.value[1]::FLOAT AS LAT FROM coord_ring, LATERAL FLATTEN(input => ring) pt ORDER BY pt.index
    """).to_pandas()
    df.columns = [c.upper() for c in df.columns]; return df

@st.cache_data
def load_cluster_points(nbhd):
    df = session.sql(f"""
        SELECT cp.LAT, cp.LONG, cp.CLUSTER_ID, cp.IS_NOISE, cp.CRIME_DATE, m.STREET
        FROM NEIGHBOURWISE_DOMAINS.CRIME_ANALYSIS.CA_CRIME_CLUSTER_POINTS cp
        LEFT JOIN NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_CRIME m ON cp.LAT = m.LAT AND cp.LONG = m.LONG AND cp.CRIME_DATE = m.OCCURRED_ON_DATE::DATE::VARCHAR
        WHERE cp.NEIGHBORHOOD_NAME = '{nbhd}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.LAT, cp.LONG, cp.CRIME_DATE ORDER BY m.LOAD_TIMESTAMP DESC) = 1
        ORDER BY cp.CRIME_DATE DESC
    """).to_pandas()
    df.columns = [c.upper() for c in df.columns]; return df

@st.cache_data
def load_crime_type_breakdown(nbhd):
    df = session.sql(f"""
        SELECT OFFENSE_DESCRIPTION, COUNT(*) AS CRIME_COUNT,
            CASE WHEN IS_VIOLENT_CRIME THEN 'Violent' WHEN IS_PROPERTY_CRIME THEN 'Property' END AS CRIME_CATEGORY
        FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_CRIME
        WHERE NEIGHBORHOOD_NAME = '{nbhd}' AND VALID_LOCATION = TRUE AND OCCURRED_ON_DATE >= DATEADD('month', -12, CURRENT_DATE())
          AND (IS_VIOLENT_CRIME = TRUE OR IS_PROPERTY_CRIME = TRUE)
        GROUP BY 1, 3 ORDER BY 2 DESC LIMIT 5
    """).to_pandas()
    df.columns = [c.upper() for c in df.columns]; return df

@st.cache_data
def load_safety_choropleth():
    df = session.sql("""
        SELECT ml.NAME AS NEIGHBORHOOD_NAME, ml.CENTROID_LAT, ml.CENTROID_LONG,
            ST_ASGEOJSON(TO_GEOGRAPHY(ml.GEOMETRY_WKT))::VARCHAR AS GEOJSON,
            ns.SAFETY_SCORE, ns.SAFETY_GRADE, ns.AVG_MONTHLY_INCIDENTS
        FROM NEIGHBOURWISE_DOMAINS.STAGE.STG_MASTER_LOCATION ml
        INNER JOIN NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY ns ON UPPER(ml.NAME) = UPPER(ns.NEIGHBORHOOD_NAME)
        WHERE ml.CITY IN ('BOSTON','CAMBRIDGE') AND ml.GRANULARITY = 'NEIGHBORHOOD'
          AND ns.INSUFFICIENT_DATA = FALSE AND ns.SAFETY_GRADE != 'INSUFFICIENT DATA'
          AND ml.GEOMETRY_WKT IS NOT NULL AND ml.CENTROID_LAT IS NOT NULL
    """).to_pandas()
    df.columns = [c.upper() for c in df.columns]; return df

@st.cache_data
def load_adjacency():
    df = session.sql("""
        SELECT a.NEIGHBORHOOD_NAME AS NEIGHBORHOOD, b.NEIGHBORHOOD_NAME AS NEIGHBOR
        FROM NEIGHBOURWISE_DOMAINS.MARTS.MASTER_LOCATION a
        JOIN NEIGHBOURWISE_DOMAINS.MARTS.MASTER_LOCATION b ON ST_INTERSECTS(a.GEOMETRY, b.GEOMETRY) AND a.LOCATION_ID != b.LOCATION_ID
        WHERE (a.IS_BOSTON = TRUE OR a.IS_CAMBRIDGE = TRUE) AND (b.IS_BOSTON = TRUE OR b.IS_CAMBRIDGE = TRUE)
        ORDER BY 1, 2
    """).to_pandas()
    df.columns = [c.upper() for c in df.columns]
    return df.groupby("NEIGHBORHOOD")["NEIGHBOR"].apply(list).to_dict()

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADERS — GROCERY
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data
def load_grocery_clusters():
    return session.sql("""
        SELECT NEIGHBORHOOD_NAME, CITY, TOTAL_STORES, ESSENTIAL_STORE_COUNT, ESSENTIAL_STORE_PCT,
            SUPERMARKET_COUNT, CONVENIENCE_STORE_COUNT, SPECIALTY_STORE_COUNT, PHARMACY_COUNT,
            FARMERS_MARKET_COUNT, N_STORE_CLUSTERS, CLUSTERED_STORE_SHARE_PCT, ISOLATED_STORE_PCT,
            ACCESS_TIER, LOAD_TIMESTAMP
        FROM NEIGHBOURWISE_DOMAINS.GROCERY_ANALYSIS.GA_GROCERY_HOTSPOT_CLUSTERS ORDER BY ESSENTIAL_STORE_COUNT DESC
    """).to_pandas()

@st.cache_data
def load_grocery_narratives():
    return session.sql("""
        SELECT NEIGHBORHOOD_NAME, CITY, ACCESS_TIER, TOTAL_STORES, ESSENTIAL_STORE_COUNT,
            ESSENTIAL_STORE_PCT, N_STORE_CLUSTERS, DATA_YEAR, FOOD_ACCESS_NARRATIVE, RELIABILITY_FLAG
        FROM NEIGHBOURWISE_DOMAINS.GROCERY_ANALYSIS.GA_GROCERY_NARRATIVE ORDER BY ESSENTIAL_STORE_COUNT DESC
    """).to_pandas()

@st.cache_data
def load_grocery_map_data():
    return session.sql("""
        SELECT c.NEIGHBORHOOD_NAME, c.CITY, c.TOTAL_STORES, c.ESSENTIAL_STORE_COUNT,
            c.ESSENTIAL_STORE_PCT, c.SUPERMARKET_COUNT, c.ACCESS_TIER, c.N_STORE_CLUSTERS,
            ml.LOCATION_ID, ml.CENTROID_LAT, ml.CENTROID_LONG,
            ST_ASGEOJSON(TO_GEOGRAPHY(ml.GEOMETRY_WKT)) AS GEOJSON
        FROM NEIGHBOURWISE_DOMAINS.GROCERY_ANALYSIS.GA_GROCERY_HOTSPOT_CLUSTERS c
        LEFT JOIN NEIGHBOURWISE_DOMAINS.STAGE.STG_MASTER_LOCATION ml ON UPPER(c.NEIGHBORHOOD_NAME) = UPPER(ml.NAME)
        WHERE ml.GEOMETRY_WKT IS NOT NULL AND ml.CENTROID_LAT IS NOT NULL
    """).to_pandas()

@st.cache_data
def load_grocery_stores():
    return session.sql("""
        SELECT STORE_NAME, STREET_ADDRESS, ZIP_CODE, STORE_TYPE, NAICS_CATEGORY,
            IS_ESSENTIAL_FOOD_SOURCE, IS_LARGE_FORMAT, IS_SPECIALTY_STORE, IS_CONVENIENCE_TYPE,
            DATA_YEAR, LAT, LONG, NEIGHBORHOOD_NAME, CITY
        FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_GROCERY_STORES WHERE HAS_VALID_LOCATION = TRUE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(STORE_NAME)), UPPER(TRIM(STREET_ADDRESS)), NEIGHBORHOOD_NAME ORDER BY DATA_YEAR DESC) = 1
        ORDER BY NEIGHBORHOOD_NAME, STORE_TYPE
    """).to_pandas()

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADERS — HEALTHCARE
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data
def load_healthcare_data():
    df = session.sql("""
        SELECT
            ap.NEIGHBORHOOD_NAME,
            ap.CITY,
            ap.HEALTHCARE_SCORE,
            ap.HEALTHCARE_GRADE,
            ap.TOTAL_FACILITIES,
            ap.HOSPITAL_COUNT,
            ap.CLINIC_COUNT,
            ap.FACILITIES_PER_SQMILE,
            ap.PCT_CORE_CARE,
            ap.PCT_VALID_PHONE,
            ap.DENSITY_SCORE,
            ap.CORE_CARE_SCORE,
            ap.CONTACT_QUALITY_SCORE,
            ap.DIVERSITY_SCORE,
            COALESCE(hc.N_HEALTHCARE_CLUSTERS, 0) AS N_HEALTHCARE_CLUSTERS,
            COALESCE(hc.CLUSTERED_FACILITY_SHARE_PCT, 0) AS CLUSTERED_FACILITY_SHARE_PCT,
            hn.HEALTHCARE_NARRATIVE
        FROM NEIGHBOURWISE_DOMAINS.HEALTHCARE_ANALYSIS.HA_HEALTHCARE_ACCESS_PROFILE ap
        LEFT JOIN NEIGHBOURWISE_DOMAINS.HEALTHCARE_ANALYSIS.HA_HEALTHCARE_HOTSPOT_CLUSTERS hc
            ON UPPER(ap.NEIGHBORHOOD_NAME) = UPPER(hc.NEIGHBORHOOD_NAME)
        LEFT JOIN NEIGHBOURWISE_DOMAINS.HEALTHCARE_ANALYSIS.HA_HEALTHCARE_NARRATIVE hn
            ON UPPER(ap.NEIGHBORHOOD_NAME) = UPPER(hn.NEIGHBORHOOD_NAME)
        ORDER BY ap.HEALTHCARE_SCORE DESC
    """).to_pandas()
    df.columns = [c.upper() for c in df.columns]
    return df

@st.cache_data
def load_healthcare_map_data():
    df = session.sql("""
        SELECT
            ap.NEIGHBORHOOD_NAME,
            ap.CITY,
            ap.HEALTHCARE_SCORE,
            ap.HEALTHCARE_GRADE,
            ap.TOTAL_FACILITIES,
            ap.HOSPITAL_COUNT,
            ap.CLINIC_COUNT,
            ap.FACILITIES_PER_SQMILE,
            ap.PCT_CORE_CARE,
            ap.PCT_VALID_PHONE,
            COALESCE(hc.N_HEALTHCARE_CLUSTERS, 0) AS N_HEALTHCARE_CLUSTERS,
            COALESCE(hc.CLUSTERED_FACILITY_SHARE_PCT, 0) AS CLUSTERED_FACILITY_SHARE_PCT,
            ml.CENTROID_LAT,
            ml.CENTROID_LONG,
            ST_ASGEOJSON(ml.GEOMETRY) AS GEOJSON
        FROM NEIGHBOURWISE_DOMAINS.HEALTHCARE_ANALYSIS.HA_HEALTHCARE_ACCESS_PROFILE ap
        LEFT JOIN NEIGHBOURWISE_DOMAINS.HEALTHCARE_ANALYSIS.HA_HEALTHCARE_HOTSPOT_CLUSTERS hc
            ON ap.LOCATION_ID = hc.LOCATION_ID
        LEFT JOIN NEIGHBOURWISE_DOMAINS.MARTS.MASTER_LOCATION ml
            ON ap.LOCATION_ID = ml.LOCATION_ID
        WHERE ml.GEOMETRY IS NOT NULL
          AND ml.CENTROID_LAT IS NOT NULL
          AND ml.CENTROID_LONG IS NOT NULL
    """).to_pandas()
    df.columns = [c.upper() for c in df.columns]
    return df

# ══════════════════════════════════════════════════════════════════════════════
# LOAD ALL DATA
# ══════════════════════════════════════════════════════════════════════════════
crime_fc_df   = load_crime_forecasts()
crime_hs_df   = load_crime_hotspots()
crime_nar_df  = load_crime_narratives()
crime_hist_df = load_crime_historical()
adjacency     = load_adjacency()
grocery_clusters_df  = load_grocery_clusters()
grocery_narrative_df = load_grocery_narratives()
grocery_map_df       = load_grocery_map_data()
grocery_stores_df    = load_grocery_stores()
healthcare_df = load_healthcare_data()
healthcare_map_df = load_healthcare_map_data()
crime_neighborhoods = sorted(crime_nar_df["NEIGHBORHOOD_NAME"].unique().tolist())

# Grocery helpers
TIER_ORDER = ["HIGH_ACCESS","GOOD_ACCESS","FAIR_ACCESS","LOW_ACCESS"]
TIER_COLORS = {"HIGH_ACCESS":[134,197,164,180],"GOOD_ACCESS":[134,197,164,120],"FAIR_ACCESS":[237,201,128,170],"LOW_ACCESS":[219,148,140,180]}
TIER_HEX = {"HIGH_ACCESS":"#86C5A4","GOOD_ACCESS":"#B0D9C1","FAIR_ACCESS":"#EDC980","LOW_ACCESS":"#DB948C"}
def tier_color(t): return TIER_COLORS.get(t, [100,100,100,130])
def tier_label(t): return t.replace("_"," ").title() if t else "Unknown"
def build_grocery_geojson(df, sel_nbhd=None):
    feats = []
    for _, r in df.iterrows():
        try: geom = json.loads(r["GEOJSON"])
        except: continue
        is_selected = (r["NEIGHBORHOOD_NAME"] == sel_nbhd) if sel_nbhd else False
        feats.append({"type":"Feature","geometry":geom,"properties":{
            "NEIGHBORHOOD_NAME":r["NEIGHBORHOOD_NAME"],"CITY":r["CITY"],
            "TOTAL_STORES":int(r["TOTAL_STORES"] or 0),"ESSENTIAL_STORE_COUNT":int(r["ESSENTIAL_STORE_COUNT"] or 0),
            "ESSENTIAL_STORE_PCT":float(r["ESSENTIAL_STORE_PCT"] or 0),"SUPERMARKET_COUNT":int(r["SUPERMARKET_COUNT"] or 0),
            "ACCESS_TIER":r["ACCESS_TIER"],"ACCESS_TIER_LABEL":tier_label(r["ACCESS_TIER"]),
            "N_STORE_CLUSTERS":int(r["N_STORE_CLUSTERS"] or 0),"fill_color":tier_color(r["ACCESS_TIER"]),
            "is_selected":is_selected}})
    return {"type":"FeatureCollection","features":feats}

PRIMARY = "#1e3a5f"; SUCCESS = "#86C5A4"; INFO = "#60a5fa"
SAFETY_COLORS = {"EXCELLENT":[30,132,73,180],"GOOD":[130,224,170,180],"MODERATE":[241,196,15,180],"HIGH CONCERN":[192,57,43,180]}

def build_safety_features(choro_df, sel_nbhd=None):
    feats = []
    for _, r in choro_df.iterrows():
        try: geom = json.loads(r["GEOJSON"])
        except: continue
        grade = r["SAFETY_GRADE"] if pd.notna(r["SAFETY_GRADE"]) else "N/A"
        score = round(float(r["SAFETY_SCORE"]),1) if pd.notna(r["SAFETY_SCORE"]) else "N/A"
        avg = round(float(r["AVG_MONTHLY_INCIDENTS"]),1) if pd.notna(r["AVG_MONTHLY_INCIDENTS"]) else "N/A"
        feats.append({"type":"Feature","geometry":geom,"properties":{
            "NEIGHBORHOOD_NAME":r["NEIGHBORHOOD_NAME"],"SAFETY_SCORE":score,"SAFETY_GRADE":grade,"AVG_MONTHLY":avg,
            "fill_color":SAFETY_COLORS.get(str(grade).strip().upper(),[160,160,160,140]),
            "is_selected": r["NEIGHBORHOOD_NAME"] == sel_nbhd if sel_nbhd else False}})
    return feats

def render_safety_choropleth(choro_df, sel_nbhd=None, height=450):
    feats = build_safety_features(choro_df, sel_nbhd)
    gj = {"type":"FeatureCollection","features":feats}
    layers = [pdk.Layer("GeoJsonLayer",data=gj,filled=True,stroked=True,pickable=True,auto_highlight=True,
        get_fill_color="properties.fill_color",get_line_color=[255,255,255,180],line_width_min_pixels=1)]
    if sel_nbhd:
        sel_feats = [f for f in feats if f["properties"]["is_selected"]]
        if sel_feats:
            layers.append(pdk.Layer("GeoJsonLayer",data={"type":"FeatureCollection","features":sel_feats},
                filled=False,stroked=True,get_line_color=[255,255,255,255],line_width_min_pixels=3))
    deck = pdk.Deck(layers=layers,
        initial_view_state=pdk.ViewState(latitude=float(choro_df["CENTROID_LAT"].mean()),
            longitude=float(choro_df["CENTROID_LONG"].mean()),zoom=10.8,pitch=0),
        tooltip={"html":"<b>{NEIGHBORHOOD_NAME}</b><br/>Score: <b>{SAFETY_SCORE}</b>/100<br/>Grade: <b>{SAFETY_GRADE}</b><br/>Avg Monthly: <b>{AVG_MONTHLY}</b>",
            "style":{"backgroundColor":"white","color":"#0f172a","fontSize":"12px","borderRadius":"8px","padding":"8px"}},
        map_style="mapbox://styles/mapbox/light-v10")
    st.pydeck_chart(deck, use_container_width=True, height=height)
    l1,l2,l3,l4 = st.columns(4)
    l1.markdown('<span style="color:#1E8449;">■</span> **Excellent (≥75)**',unsafe_allow_html=True)
    l2.markdown('<span style="color:#82E0AA;">■</span> **Good (50–74)**',unsafe_allow_html=True)
    l3.markdown('<span style="color:#F1C40F;">■</span> **Moderate (25–49)**',unsafe_allow_html=True)
    l4.markdown('<span style="color:#C0392B;">■</span> **High Concern (<25)**',unsafe_allow_html=True)

def render_metric_cards(items):
    cols = st.columns(len(items))
    for col, (label, value, sub) in zip(cols, items):
        col.markdown(f"""<div class="metric-card"><div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div><div class="metric-sub">{sub}</div></div>""", unsafe_allow_html=True)

# Healthcare helpers
def healthcare_score_to_color(score):
    if score >= 75:
        return [22, 163, 74, 160]     # green
    elif score >= 50:
        return [37, 99, 235, 150]     # blue
    elif score >= 25:
        return [245, 158, 11, 150]    # amber
    else:
        return [220, 38, 38, 160]     # red

def build_healthcare_geojson_feature_collection(df, sel_nbhd=None):
    features = []

    for _, row in df.iterrows():
        try:
            geometry = json.loads(row["GEOJSON"])
        except Exception:
            continue

        is_selected = (row["NEIGHBORHOOD_NAME"] == sel_nbhd) if sel_nbhd else False

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "NEIGHBORHOOD_NAME": row["NEIGHBORHOOD_NAME"],
                "CITY": row["CITY"],
                "TOTAL_FACILITIES": int(row["TOTAL_FACILITIES"]) if pd.notnull(row["TOTAL_FACILITIES"]) else 0,
                "HOSPITAL_COUNT": int(row["HOSPITAL_COUNT"]) if pd.notnull(row["HOSPITAL_COUNT"]) else 0,
                "CLINIC_COUNT": int(row["CLINIC_COUNT"]) if pd.notnull(row["CLINIC_COUNT"]) else 0,
                "FACILITIES_PER_SQMILE": float(row["FACILITIES_PER_SQMILE"]) if pd.notnull(row["FACILITIES_PER_SQMILE"]) else 0,
                "HEALTHCARE_SCORE": float(row["HEALTHCARE_SCORE"]) if pd.notnull(row["HEALTHCARE_SCORE"]) else 0,
                "HEALTHCARE_GRADE": row["HEALTHCARE_GRADE"],
                "N_HEALTHCARE_CLUSTERS": int(row["N_HEALTHCARE_CLUSTERS"]) if pd.notnull(row["N_HEALTHCARE_CLUSTERS"]) else 0,
                "CLUSTERED_FACILITY_SHARE_PCT": float(row["CLUSTERED_FACILITY_SHARE_PCT"]) if pd.notnull(row["CLUSTERED_FACILITY_SHARE_PCT"]) else 0,
                "fill_color": healthcare_score_to_color(float(row["HEALTHCARE_SCORE"]) if pd.notnull(row["HEALTHCARE_SCORE"]) else 0),
                "is_selected": is_selected
            }
        }
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features
    }

# ══════════════════════════════════════════════════════════════════════════════
# HERO
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="hero-card">
    <div class="hero-title">NeighbourWise AI — Neighborhood Intelligence</div>
    <div class="hero-subtitle">
        Crime safety forecasting, grocery access analytics, and healthcare facility
        intelligence across Boston & Cambridge — powered by ARIMA, DBSCAN, and Snowflake Cortex.
    </div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("NeighbourWise AI")
st.sidebar.markdown("Neighborhood intelligence for Greater Boston.")
selected_nbhd = st.sidebar.selectbox("Select Neighborhood", ["All"] + crime_neighborhoods)

st.sidebar.markdown("---")
st.sidebar.markdown("**Crime Filters**")
show_ci = st.sidebar.checkbox("Show forecast confidence interval", value=True)
trend_filter = st.sidebar.multiselect("Filter by trend", options=["increasing","stable","decreasing"], default=["increasing","stable","decreasing"])
reliability_filter = st.sidebar.multiselect("Forecast reliability", options=["HIGH","LOW"], default=["HIGH"], help="LOW = MAPE > 50% or fewer than 500 total crimes.")

# ── Apply shared neighborhood filter to grocery data ──
f_groc_clusters = grocery_clusters_df.copy()
f_groc_narrative = grocery_narrative_df.copy()
f_groc_map = grocery_map_df.copy()
f_groc_stores = grocery_stores_df.copy()
if selected_nbhd != "All":
    f_groc_clusters = f_groc_clusters[f_groc_clusters["NEIGHBORHOOD_NAME"] == selected_nbhd]
    f_groc_narrative = f_groc_narrative[f_groc_narrative["NEIGHBORHOOD_NAME"] == selected_nbhd]
    f_groc_stores = f_groc_stores[f_groc_stores["NEIGHBORHOOD_NAME"] == selected_nbhd]
    # f_groc_map is NOT filtered — show all polygons but highlight selected

# ── Apply shared neighborhood filter to healthcare data ──
f_hc = healthcare_df.copy()
f_hc_map = healthcare_map_df.copy()

if selected_nbhd != "All":
    f_hc = f_hc[f_hc["NEIGHBORHOOD_NAME"] == selected_nbhd]
    # keep map data unfiltered so all polygons remain visible, but selected one can be highlighted

# ══════════════════════════════════════════════════════════════════════════════
# MAIN TABS
# ══════════════════════════════════════════════════════════════════════════════
tab_crime, tab_grocery, tab_healthcare = st.tabs(["🔴 Crime & Safety", "🛒 Grocery & Food Access", "🏥 Healthcare"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CRIME & SAFETY  (unchanged from document 7)
# ══════════════════════════════════════════════════════════════════════════════
with tab_crime:
    if selected_nbhd == "All":
        total_crime_nbhds = crime_nar_df["NEIGHBORHOOD_NAME"].nunique()
        avg_monthly_all = round(crime_nar_df["RECENT_AVG_MONTHLY"].mean(), 0)
        increasing_cnt = int((crime_nar_df["RECENT_TREND"]=="increasing").sum())
        decreasing_cnt = int((crime_nar_df["RECENT_TREND"]=="decreasing").sum())
        render_metric_cards([("Neighborhoods", total_crime_nbhds, "Boston & Cambridge"),("Avg Monthly Crimes", f"{avg_monthly_all:.0f}", "Per neighborhood"),("Trends Increasing", increasing_cnt, "Neighborhoods worsening"),("Trends Decreasing", decreasing_cnt, "Neighborhoods improving")])
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown("**Neighborhood Safety Score — Boston & Cambridge**"); st.caption("Green = safer, red = higher concern.")
        choro_df = load_safety_choropleth()
        if not choro_df.empty: render_safety_choropleth(choro_df, height=500)
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-card">', unsafe_allow_html=True); st.markdown("**All Neighborhoods — Forecasted Crime Count**")
        filtered_nar = crime_nar_df[(crime_nar_df["RECENT_TREND"].isin(trend_filter)) & (crime_nar_df["RELIABILITY_FLAG"].isin(reliability_filter))].copy()
        if not filtered_nar.empty:
            bar = alt.Chart(filtered_nar.sort_values("FORECASTED_COUNT",ascending=False)).mark_bar(cornerRadiusTopLeft=5,cornerRadiusTopRight=5).encode(x=alt.X("FORECASTED_COUNT:Q",title="Forecasted Crimes"),y=alt.Y("NEIGHBORHOOD_NAME:N",sort="-x",title=""),color=alt.Color("RECENT_TREND:N",scale=alt.Scale(domain=["increasing","stable","decreasing"],range=["#E45756","#F58518","#54A24B"])),tooltip=["NEIGHBORHOOD_NAME","FORECASTED_COUNT","RECENT_TREND","TRAIN_MAPE"]).properties(height=500)
            st.altair_chart(bar, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-card">', unsafe_allow_html=True); st.markdown("**Ask Cortex About Crime Trends — All Neighborhoods**")
        user_q = st.text_input("Your question", placeholder="e.g. Which neighborhoods are the safest in Boston?", key="crime_q")
        if user_q:
            ctx = []
            for _, r in crime_nar_df.iterrows():
                mp = f"{r['TRAIN_MAPE']:.1f}" if pd.notna(r.get("TRAIN_MAPE")) else "N/A"
                ctx.append(f"--- {r['NEIGHBORHOOD_NAME']} ---\n  Trend: {r['RECENT_TREND']} | Avg: {r['RECENT_AVG_MONTHLY']:.0f}/mo | Forecast: {r['FORECASTED_COUNT']} | MAPE: {mp}% | Reliability: {r.get('RELIABILITY_FLAG','N/A')}\n  Summary: {r.get('SAFETY_NARRATIVE','N/A')}")
            fc = "\n\n".join(ctx)
            if len(fc) > 10000: fc = fc[:10000]
            prompt = f"You are a friendly neighborhood safety guide for Boston/Cambridge.\nViewing: ALL neighborhoods.\n\nHOW TO ANSWER:\n- Warm tone. Interpret numbers plainly. Rank using data. 5-8 sentences.\n\n{fc}\n\nQuestion: {user_q}"
            with st.spinner("Cortex is thinking..."):
                try: res = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', ?)", params=[prompt]).collect()[0][0]; st.markdown(f"**Cortex:** {res.strip()}")
                except Exception as e: st.error(f"Cortex error: {e}")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        nbhd_nar_rows = crime_nar_df[crime_nar_df["NEIGHBORHOOD_NAME"]==selected_nbhd]; nbhd_fc_rows = crime_fc_df[crime_fc_df["NEIGHBORHOOD_NAME"]==selected_nbhd]
        if nbhd_nar_rows.empty: st.warning(f"No data for {selected_nbhd}.")
        else:
            nbhd_nar = nbhd_nar_rows.iloc[0]; nbhd_fc = nbhd_fc_rows
            cluster_pts_all = load_cluster_points(selected_nbhd); map_cluster_count = 0; map_hotspot_pct = 0.0
            if not cluster_pts_all.empty:
                vc = [c for c in cluster_pts_all["CLUSTER_ID"].unique() if c != -1]; map_cluster_count = len(vc)
                nhp = int((~cluster_pts_all["IS_NOISE"]).sum()); nt = len(cluster_pts_all); map_hotspot_pct = round(nhp/nt*100,1) if nt > 0 else 0.0
            trend_emoji = {"increasing":"📈","decreasing":"📉","stable":"➡️"}; trend = nbhd_nar["RECENT_TREND"]
            fv = nbhd_nar["FORECASTED_COUNT"]; ra = nbhd_nar["RECENT_AVG_MONTHLY"]; fd = "rising" if fv > ra*1.05 else ("falling" if fv < ra*0.95 else "flat")
            render_metric_cards([("Recent Monthly Avg", f"{ra:.0f}", "crimes/month"),("Next Month Forecast", f"{fv}", f"{fv-ra:.0f} vs recent avg"),("Recent Trend", f"{trend_emoji.get(trend,'')} {trend.capitalize()}", "Last 3 months"),("Hotspot Clusters", f"{map_cluster_count}", "DBSCAN dense zones")])
            if (trend=="decreasing" and fd=="rising") or (trend=="increasing" and fd=="falling"): st.caption(f"Note: Recent trend is **{trend}**, but forecast projects **{fd}** — potential trend reversal.")
            nt_text = nbhd_nar["SAFETY_NARRATIVE"]
            nt_text = re.sub(r'\d+\s+(?:[A-Za-z\-]+\s+){0,4}(?:clusters?|hotspots?)',f"{map_cluster_count} hotspot cluster{'s' if map_cluster_count!=1 else ''}", nt_text, flags=re.IGNORECASE)
            nt_text = re.sub(r'[\d.]+%\s+of\s+(?:[A-Za-z\-]+\s+){0,3}crimes\s+\w+',f"{map_hotspot_pct}% of crimes occur", nt_text, flags=re.IGNORECASE)
            st.markdown(f'<div class="narrative-box-blue"><div class="narrative-neighborhood">{selected_nbhd} — AI Safety Summary</div>{nt_text}</div>', unsafe_allow_html=True)
            ao = nbhd_fc["ARIMA_ORDER"].iloc[0] if not nbhd_fc.empty else "N/A"; mv = f"{nbhd_nar['TRAIN_MAPE']}%" if pd.notna(nbhd_nar["TRAIN_MAPE"]) else "N/A"
            st.caption(f"Model accuracy (MAPE): {mv} | ARIMA order: {ao}")
            col_scatter, col_choro = st.columns(2)
            with col_scatter:
                st.markdown('<div class="section-card">', unsafe_allow_html=True); st.markdown(f"**Crime Incidents — {selected_nbhd}**"); st.caption("Each point is a crime incident (last 12 months).")
                if not cluster_pts_all.empty:
                    mdf = cluster_pts_all.rename(columns={"LAT":"latitude","LONG":"longitude"}).copy(); mdf["POINT_TYPE"] = mdf["IS_NOISE"].map({True:"Dispersed",False:"Hotspot"})
                    scatter = alt.Chart(mdf).mark_circle(opacity=0.5).encode(longitude="longitude:Q",latitude="latitude:Q",color=alt.Color("POINT_TYPE:N",scale=alt.Scale(domain=["Hotspot","Dispersed"],range=["#E45756","#AAAAAA"]),legend=alt.Legend(title="Type")),size=alt.condition(alt.datum.POINT_TYPE=="Dispersed",alt.value(10),alt.value(18)),tooltip=[alt.Tooltip("STREET:N",title="Street"),alt.Tooltip("POINT_TYPE:N",title="Zone"),alt.Tooltip("CRIME_DATE:N",title="Date")]).properties(width=480,height=420)
                    bdf = load_neighborhood_boundary(selected_nbhd)
                    if not bdf.empty:
                        bnd = bdf.rename(columns={"LAT":"latitude","LONG":"longitude"}); bnd = pd.concat([bnd, bnd.iloc[[0]]], ignore_index=True); bnd["order"] = range(len(bnd))
                        bl = alt.Chart(bnd).mark_line(color="#FFFFFF",strokeWidth=2,opacity=0.6).encode(longitude="longitude:Q",latitude="latitude:Q",order="order:O"); st.altair_chart(bl+scatter, use_container_width=True)
                    else: st.altair_chart(scatter, use_container_width=True)
                    nhp2 = int((~mdf["IS_NOISE"]).sum()); nns = int(mdf["IS_NOISE"].sum()); st.caption(f"**{nhp2:,}** hotspot ({nhp2/len(mdf)*100:.1f}%) across **{map_cluster_count}** clusters | **{nns:,}** dispersed")
                else: st.info("No crime point data.")
                st.markdown('</div>', unsafe_allow_html=True)
            with col_choro:
                st.markdown('<div class="section-card">', unsafe_allow_html=True); st.markdown("**Neighborhood Safety Score — Boston & Cambridge**"); st.caption("Green = safer, red = higher concern.")
                choro_df = load_safety_choropleth()
                if not choro_df.empty: render_safety_choropleth(choro_df, sel_nbhd=selected_nbhd, height=420)
                st.markdown('</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-card">', unsafe_allow_html=True); st.markdown(f"**Crime Type Breakdown — {selected_nbhd}**"); st.caption("Violent & property crimes only (last 12 months).")
            ct = load_crime_type_breakdown(selected_nbhd)
            if not ct.empty:
                pie = alt.Chart(ct).mark_arc().encode(theta=alt.Theta("CRIME_COUNT:Q",stack=True),color=alt.Color("OFFENSE_DESCRIPTION:N",scale=alt.Scale(range=["#7BA7C9","#A3C4BC","#D4B896","#B8A9C9","#C9A9A6"]),legend=alt.Legend(title="Crime Type",labelLimit=180,labelFontSize=11,titleFontSize=12,symbolSize=80,columns=1)),tooltip=[alt.Tooltip("OFFENSE_DESCRIPTION:N",title="Crime"),alt.Tooltip("CRIME_COUNT:Q",title="Incidents"),alt.Tooltip("CRIME_CATEGORY:N",title="Category")]).properties(height=350)
                st.altair_chart(pie, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-card">', unsafe_allow_html=True); st.markdown(f"**Historical Crime + 6-Month Forecast — {selected_nbhd}**")
            hn = crime_hist_df[crime_hist_df["NEIGHBORHOOD_NAME"]==selected_nbhd].copy(); hn["year_month"] = pd.to_datetime(hn["YEAR_MONTH"]); hn["type"] = "Historical"; hn = hn.rename(columns={"CRIME_COUNT":"crime_count"})
            fcx = nbhd_fc.copy(); fcx["year_month"] = pd.to_datetime(fcx["FORECAST_MONTH"]); fcx["crime_count"] = fcx["FORECASTED_COUNT"]; fcx["type"] = "Forecast"
            comb = pd.concat([hn[["year_month","crime_count","type"]], fcx[["year_month","crime_count","type"]]])
            if not comb.empty:
                line = alt.Chart(comb).mark_line(point=True).encode(x=alt.X("year_month:T",title="Month"),y=alt.Y("crime_count:Q",title="Crime Count"),color=alt.Color("type:N",scale=alt.Scale(domain=["Historical","Forecast"],range=["#4C78A8","#F58518"])),tooltip=["year_month:T","crime_count:Q","type:N"])
                if show_ci and not fcx.empty: ci = alt.Chart(fcx).mark_area(opacity=0.2,color="#F58518").encode(x="year_month:T",y="LOWER_CI:Q",y2="UPPER_CI:Q"); ch = (ci+line).properties(height=350)
                else: ch = line.properties(height=350)
                st.altair_chart(ch, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-card">', unsafe_allow_html=True); st.markdown("**All Neighborhoods — Forecasted Crime Count**")
            fn = crime_nar_df[(crime_nar_df["RECENT_TREND"].isin(trend_filter))&(crime_nar_df["RELIABILITY_FLAG"].isin(reliability_filter))].copy()
            fn = fn.merge(crime_hs_df[["NEIGHBORHOOD_NAME","N_HOTSPOT_CLUSTERS","HOTSPOT_CRIME_SHARE_PCT"]],on="NEIGHBORHOOD_NAME",how="left")
            if not fn.empty:
                bar = alt.Chart(fn.sort_values("FORECASTED_COUNT",ascending=False)).mark_bar(cornerRadiusTopLeft=5,cornerRadiusTopRight=5).encode(x=alt.X("FORECASTED_COUNT:Q",title="Forecasted Crimes"),y=alt.Y("NEIGHBORHOOD_NAME:N",sort="-x",title=""),color=alt.Color("RECENT_TREND:N",scale=alt.Scale(domain=["increasing","stable","decreasing"],range=["#E45756","#F58518","#54A24B"])),tooltip=["NEIGHBORHOOD_NAME","FORECASTED_COUNT","RECENT_TREND","TRAIN_MAPE"]).properties(height=500)
                st.altair_chart(bar, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-card">', unsafe_allow_html=True); st.markdown("**Ask Cortex About Crime Trends**")
            st.caption(f"Focused on **{selected_nbhd}** + adjacent neighborhoods. Questions about other areas will be answered in context of {selected_nbhd}.")
            user_q = st.text_input("Your question", placeholder=f"e.g. Is {selected_nbhd} safe? Nearby safer options?", key="crime_q")
            if user_q:
                adj_names = adjacency.get(selected_nbhd, []); rel_set = set([selected_nbhd] + adj_names); rows = []
                for _, r in crime_nar_df.iterrows():
                    if r["NEIGHBORHOOD_NAME"] not in rel_set: continue
                    mp = f"{r['TRAIN_MAPE']:.1f}" if pd.notna(r.get("TRAIN_MAPE")) else "N/A"; narr = r.get("SAFETY_NARRATIVE","N/A")
                    if r["NEIGHBORHOOD_NAME"] == selected_nbhd:
                        narr = re.sub(r'\d+\s+(?:[A-Za-z\-]+\s+){0,4}(?:clusters?|hotspots?)',f"{map_cluster_count} hotspot cluster{'s' if map_cluster_count!=1 else ''}",narr,flags=re.IGNORECASE)
                        narr = re.sub(r'[\d.]+%\s+of\s+(?:[A-Za-z\-]+\s+){0,3}crimes\s+\w+',f"{map_hotspot_pct}% of crimes occur",narr,flags=re.IGNORECASE)
                    isl = "⭐ SELECTED" if r["NEIGHBORHOOD_NAME"]==selected_nbhd else "ADJACENT"
                    rows.append(f"--- {r['NEIGHBORHOOD_NAME']} [{isl}] ---\n  Trend: {r['RECENT_TREND']} | Avg: {r['RECENT_AVG_MONTHLY']:.0f}/mo | Forecast: {r['FORECASTED_COUNT']} | MAPE: {mp}% | Reliability: {r.get('RELIABILITY_FLAG','N/A')}\n  Summary: {narr}")
                als = ", ".join(adj_names) if adj_names else "None"; fc = "\n\n".join(rows) + f"\n\nADJACENCY:\n{selected_nbhd} borders: {als}\n\nIMPORTANT: ONLY mention neighborhoods listed above."
                if len(fc) > 10000: fc = fc[:10000]
                prompt = f"You are a friendly neighborhood safety guide for Boston/Cambridge.\nViewing: {selected_nbhd}\n\nHOW TO ANSWER:\n- The user has selected {selected_nbhd}. ALWAYS start your answer with {selected_nbhd}'s data first.\n- If the user asks about a different neighborhood, first answer about {selected_nbhd}, then compare with the asked neighborhood ONLY if it appears in the data below.\n- If the asked neighborhood is not in the data below, say you only have data for {selected_nbhd} and its adjacent areas.\n- Warm tone. Interpret numbers plainly. ONLY mention neighborhoods listed below. 5-8 sentences.\n- If Reliability=LOW, note forecast is less reliable.\n\n{fc}\n\nQuestion: {user_q}"
                with st.spinner("Cortex is thinking..."):
                    try: res = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', ?)",params=[prompt]).collect()[0][0]; st.markdown(f"**Cortex:** {res.strip()}")
                    except Exception as e: st.error(f"Cortex error: {e}")
            st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — GROCERY & FOOD ACCESS  (PATCHED: bubble chart removed, store type mix full-width)
# ══════════════════════════════════════════════════════════════════════════════
with tab_grocery:
    total_nbhds = f_groc_clusters["NEIGHBORHOOD_NAME"].nunique()
    total_stores = int(f_groc_clusters["TOTAL_STORES"].sum())
    low_cnt = int((f_groc_clusters["ACCESS_TIER"]=="LOW_ACCESS").sum())
    avg_ess = round(f_groc_clusters["ESSENTIAL_STORE_PCT"].mean(),1) if not f_groc_clusters.empty else 0

    render_metric_cards([
        ("Neighborhoods", total_nbhds, "In current filter"),
        ("Total Stores", total_stores, "Unique deduplicated"),
        ("Low Access Zones", low_cnt, "≤ 2 essential stores"),
        ("Avg Essential %", f"{avg_ess}%", "Supermarkets + produce"),
    ])

    gtab1, gtab2, gtab3 = st.tabs(["📊 Analytics", "🗺️ Map", "🤖 AI Chat"])

    with gtab1:
        c1, c2 = st.columns([1.2, 1])
        with c1:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Essential Store Count by Neighborhood")
            ch = alt.Chart(f_groc_clusters.head(12)).mark_bar(cornerRadiusTopLeft=5,cornerRadiusTopRight=5,color="#86C5A4").encode(
                x=alt.X("ESSENTIAL_STORE_COUNT:Q",title="Essential Stores"),y=alt.Y("NEIGHBORHOOD_NAME:N",sort="-x",title=""),
                tooltip=["NEIGHBORHOOD_NAME","ESSENTIAL_STORE_COUNT","TOTAL_STORES","ACCESS_TIER"]).properties(height=380)
            st.altair_chart(ch, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        with c2:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Access Tier Distribution")
            td = f_groc_clusters.groupby("ACCESS_TIER").size().reset_index(name="COUNT")
            tc = alt.Chart(td).mark_bar(cornerRadiusTopLeft=5,cornerRadiusTopRight=5).encode(
                x=alt.X("ACCESS_TIER:N",title="Access Tier",sort=TIER_ORDER),y=alt.Y("COUNT:Q",title="Neighborhoods"),
                color=alt.Color("ACCESS_TIER:N",scale=alt.Scale(domain=TIER_ORDER,range=[TIER_HEX[t] for t in TIER_ORDER]),legend=None),
                tooltip=["ACCESS_TIER","COUNT"]).properties(height=380)
            st.altair_chart(tc, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # ── PATCHED: Full-width Store Type Mix (bubble chart removed) ──
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Store Type Mix — Top 12 Neighborhoods")
        mx = f_groc_clusters[["NEIGHBORHOOD_NAME","SUPERMARKET_COUNT","CONVENIENCE_STORE_COUNT","SPECIALTY_STORE_COUNT","PHARMACY_COUNT","FARMERS_MARKET_COUNT"]].head(12).melt(id_vars="NEIGHBORHOOD_NAME",var_name="TYPE",value_name="COUNT")
        mc = alt.Chart(mx).mark_bar().encode(x=alt.X("COUNT:Q",title="Store Count"),y=alt.Y("NEIGHBORHOOD_NAME:N",sort="-x",title=""),
            color=alt.Color("TYPE:N",scale=alt.Scale(range=["#86C5A4","#EDC980","#A8C4E0","#C4A8D0","#DB948C"]),title="Store Type"),
            tooltip=["NEIGHBORHOOD_NAME","TYPE","COUNT"]).properties(height=420)
        st.altair_chart(mc, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Neighborhood Food Access Scorecard")
        st.dataframe(f_groc_clusters[["NEIGHBORHOOD_NAME","CITY","TOTAL_STORES","ESSENTIAL_STORE_COUNT","ESSENTIAL_STORE_PCT","SUPERMARKET_COUNT","CONVENIENCE_STORE_COUNT","SPECIALTY_STORE_COUNT","N_STORE_CLUSTERS","ACCESS_TIER"]].sort_values("ESSENTIAL_STORE_COUNT",ascending=False), use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with gtab2:
        active_groc_nbhd = selected_nbhd if selected_nbhd != "All" else None
        col_map, col_detail = st.columns([1.6, 1])

        with col_map:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Boston Food Access Map")
            st.caption(
                f"Viewing: **{selected_nbhd}** (highlighted with white border)"
                if active_groc_nbhd
                else "Polygons shaded by access tier — green = high access, sand = fair, coral = low access."
            )
            if not f_groc_map.empty and "GEOJSON" in f_groc_map.columns:
                gj = build_grocery_geojson(f_groc_map, sel_nbhd=active_groc_nbhd)

                # Auto-zoom on selected neighborhood
                if active_groc_nbhd:
                    sel_row = f_groc_map[f_groc_map["NEIGHBORHOOD_NAME"]==active_groc_nbhd]
                    if not sel_row.empty:
                        center_lat = float(sel_row["CENTROID_LAT"].iloc[0])
                        center_lng = float(sel_row["CENTROID_LONG"].iloc[0])
                        zoom = 12.5
                    else:
                        center_lat = float(f_groc_map["CENTROID_LAT"].mean())
                        center_lng = float(f_groc_map["CENTROID_LONG"].mean())
                        zoom = 10.8
                else:
                    center_lat = float(f_groc_map["CENTROID_LAT"].mean())
                    center_lng = float(f_groc_map["CENTROID_LONG"].mean())
                    zoom = 10.8

                # Polygon layer
                gl = pdk.Layer("GeoJsonLayer",data=gj,filled=True,stroked=True,pickable=True,auto_highlight=True,
                    get_fill_color="properties.fill_color",get_line_color=[255,255,255,180],line_width_min_pixels=1.5)
                layers = [gl]

                # Highlight border for selected neighborhood
                if active_groc_nbhd:
                    sel_feats = [f for f in gj["features"] if f["properties"].get("is_selected")]
                    if sel_feats:
                        layers.append(pdk.Layer("GeoJsonLayer",
                            data={"type":"FeatureCollection","features":sel_feats},
                            filled=False,stroked=True,get_line_color=[255,255,255,255],line_width_min_pixels=3))

                # Store scatter — essential in sage, others in grey
                sdf = f_groc_stores[["LAT","LONG","STORE_NAME","STORE_TYPE","IS_ESSENTIAL_FOOD_SOURCE","NEIGHBORHOOD_NAME"]].dropna().copy()
                sdf["FILL_R"] = sdf["IS_ESSENTIAL_FOOD_SOURCE"].apply(lambda v: 134 if str(v).lower()=="true" else 200)
                sdf["FILL_G"] = sdf["IS_ESSENTIAL_FOOD_SOURCE"].apply(lambda v: 197 if str(v).lower()=="true" else 200)
                sdf["FILL_B"] = sdf["IS_ESSENTIAL_FOOD_SOURCE"].apply(lambda v: 164 if str(v).lower()=="true" else 200)
                sdf["FILL_A"] = 180; sdf["RADIUS"] = 60
                if active_groc_nbhd:
                    is_sel = sdf["NEIGHBORHOOD_NAME"]==active_groc_nbhd
                    sdf.loc[is_sel,"FILL_A"] = 230; sdf.loc[is_sel,"RADIUS"] = 85
                    sdf.loc[~is_sel,"FILL_A"] = 70; sdf.loc[~is_sel,"RADIUS"] = 40
                layers.append(pdk.Layer("ScatterplotLayer",data=sdf,get_position=["LONG","LAT"],get_radius="RADIUS",
                    get_fill_color=["FILL_R","FILL_G","FILL_B","FILL_A"],pickable=True,opacity=0.85))

                dk = pdk.Deck(layers=layers,
                    initial_view_state=pdk.ViewState(latitude=center_lat,longitude=center_lng,zoom=zoom,pitch=0),
                    tooltip={"html":"<b>{NEIGHBORHOOD_NAME}</b><br/>🏪 {TOTAL_STORES}<br/>🥦 {ESSENTIAL_STORE_COUNT}<br/>📊 {ACCESS_TIER_LABEL}",
                        "style":{"backgroundColor":"white","color":"#0f172a","fontSize":"13px","borderRadius":"10px","padding":"10px"}},
                    map_style="mapbox://styles/mapbox/light-v10")
                st.pydeck_chart(dk, use_container_width=True)
                l1,l2,l3,l4 = st.columns(4)
                l1.markdown('<span style="color:#86C5A4;">■</span> **High Access**',unsafe_allow_html=True)
                l2.markdown('<span style="color:#B0D9C1;">■</span> **Good Access**',unsafe_allow_html=True)
                l3.markdown('<span style="color:#EDC980;">■</span> **Fair Access**',unsafe_allow_html=True)
                l4.markdown('<span style="color:#DB948C;">■</span> **Low Access**',unsafe_allow_html=True)
            else: st.warning("No map data available.")
            st.markdown('</div>', unsafe_allow_html=True)

        # ── Right panel: neighborhood snapshot or top/bottom summary ──
        with col_detail:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            if active_groc_nbhd:
                st.subheader(f"{selected_nbhd} — Snapshot")
                nbhd_row = grocery_clusters_df[grocery_clusters_df["NEIGHBORHOOD_NAME"]==selected_nbhd]
                if not nbhd_row.empty:
                    r = nbhd_row.iloc[0]
                    st.markdown(f"""
**Access Tier:** {tier_label(r['ACCESS_TIER'])}

**Total Stores:** {int(r['TOTAL_STORES'])}
**Essential Stores:** {int(r['ESSENTIAL_STORE_COUNT'])} ({r['ESSENTIAL_STORE_PCT']}%)
**Supermarkets:** {int(r['SUPERMARKET_COUNT'])}
**Convenience:** {int(r['CONVENIENCE_STORE_COUNT'])}
**Specialty:** {int(r['SPECIALTY_STORE_COUNT'])}
**Pharmacies:** {int(r['PHARMACY_COUNT'])}
**Farmers Markets:** {int(r['FARMERS_MARKET_COUNT'])}

**DBSCAN Clusters:** {int(r['N_STORE_CLUSTERS'])}
**Clustered Share:** {r['CLUSTERED_STORE_SHARE_PCT']}%
**Isolated Stores:** {r['ISOLATED_STORE_PCT']}%
                    """)

                # AI Narrative
                nbhd_narr = grocery_narrative_df[grocery_narrative_df["NEIGHBORHOOD_NAME"]==selected_nbhd]
                if not nbhd_narr.empty:
                    nr = nbhd_narr.iloc[0]
                    st.markdown(f'<div class="narrative-box"><div class="narrative-neighborhood">AI Narrative</div>{nr["FOOD_ACCESS_NARRATIVE"]}<div style="margin-top:0.5rem;font-size:0.75rem;color:#9ca3af;">Data year: {nr["DATA_YEAR"]} · Reliability: {nr["RELIABILITY_FLAG"]}</div></div>', unsafe_allow_html=True)

                # Adjacent neighborhoods comparison
                adj_names = adjacency.get(selected_nbhd, [])
                if adj_names:
                    adj_data = grocery_clusters_df[grocery_clusters_df["NEIGHBORHOOD_NAME"].isin(adj_names)]
                    if not adj_data.empty:
                        st.markdown("**Neighboring Areas**")
                        st.dataframe(adj_data[["NEIGHBORHOOD_NAME","ESSENTIAL_STORE_COUNT","TOTAL_STORES","ACCESS_TIER"]].sort_values("ESSENTIAL_STORE_COUNT",ascending=False), use_container_width=True, hide_index=True)
            else:
                st.subheader("Neighborhood Detail")
                st.caption("Select a neighborhood from the sidebar to see its snapshot, AI narrative, and neighboring area comparison.")
                st.markdown("**Top 5 — Highest Essential Store Count**")
                st.dataframe(grocery_clusters_df.head(5)[["NEIGHBORHOOD_NAME","ESSENTIAL_STORE_COUNT","ACCESS_TIER"]], use_container_width=True, hide_index=True)
                st.markdown("**Bottom 5 — Lowest Essential Store Count**")
                st.dataframe(grocery_clusters_df.tail(5)[["NEIGHBORHOOD_NAME","ESSENTIAL_STORE_COUNT","ACCESS_TIER"]], use_container_width=True, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with gtab3:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("AI Food Access Assistant")
        st.caption("Ask about store access, comparisons, or grocery patterns.")
        if "grocery_chat_history" not in st.session_state: st.session_state.grocery_chat_history = []
        cc = f_groc_clusters[["NEIGHBORHOOD_NAME","CITY","TOTAL_STORES","ESSENTIAL_STORE_COUNT","ESSENTIAL_STORE_PCT","SUPERMARKET_COUNT","ACCESS_TIER","N_STORE_CLUSTERS"]].head(20).to_csv(index=False)
        cn = f_groc_narrative[["NEIGHBORHOOD_NAME","ACCESS_TIER","FOOD_ACCESS_NARRATIVE"]].head(10).to_csv(index=False)
        for m in st.session_state.grocery_chat_history:
            with st.chat_message(m["role"]): st.markdown(m["content"])
        gq = st.chat_input("Ask about food access...", key="grocery_chat")
        if gq:
            st.session_state.grocery_chat_history.append({"role":"user","content":gq})
            with st.chat_message("user"): st.markdown(gq)
            pr = f"You are a food access analyst for NeighbourWise AI covering Boston.\nAnswer using only data below. Be concise, analytical, factual.\n\nCluster data:\n{cc}\n\nNarratives:\n{cn}\n\nQuestion: {gq}"
            try:
                res = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', $$ {pr} $$) AS RESPONSE").to_pandas()
                ai = res.iloc[0]["RESPONSE"]
            except Exception as e: ai = f"Error: {e}"
            with st.chat_message("assistant"): st.markdown(ai)
            st.session_state.grocery_chat_history.append({"role":"assistant","content":ai})
        st.markdown("**Suggested Questions**")
        st.caption("Which neighborhoods have low food access? · Compare Dorchester and Roxbury · Where is specialty food strongest?")
        st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — HEALTHCARE
# ══════════════════════════════════════════════════════════════════════════════
with tab_healthcare:
    if healthcare_df.empty:
        st.warning("No healthcare data available.")
    else:
        active_hc_nbhd = selected_nbhd if selected_nbhd != "All" else None
        filtered_hc = f_hc.copy()
        filtered_hc_map = f_hc_map.copy()

        avg_sc = round(filtered_hc["HEALTHCARE_SCORE"].mean(), 1) if not filtered_hc.empty else 0
        tot_fac = int(filtered_hc["TOTAL_FACILITIES"].sum()) if not filtered_hc.empty else 0
        avg_den = round(filtered_hc["FACILITIES_PER_SQMILE"].mean(), 1) if not filtered_hc.empty else 0
        tot_hosp = int(filtered_hc["HOSPITAL_COUNT"].sum()) if not filtered_hc.empty else 0

        render_metric_cards([
            ("Avg Healthcare Score", avg_sc, "Composite access performance"),
            ("Total Facilities", tot_fac, "Hospitals + clinics"),
            ("Avg Facilities / Sq Mile", avg_den, "Spatial density"),
            ("Total Hospitals", tot_hosp, "Major medical centers"),
        ])

        htab1, htab2, htab3 = st.tabs(["📊 Healthcare Analytics", "🗺️ Maps", "🤖 AI Assistant"])

        # ──────────────────────────────────────────────────────────────────────
        # SUBTAB 1 — HEALTHCARE ANALYTICS
        # ──────────────────────────────────────────────────────────────────────
        with htab1:
            if active_hc_nbhd and not filtered_hc.empty:
                r = filtered_hc.iloc[0]
                st.markdown(
                    f"""
                    <div class="narrative-box-blue">
                        <div class="narrative-neighborhood">{active_hc_nbhd} — Healthcare Snapshot</div>
                        <b>Score:</b> {round(float(r["HEALTHCARE_SCORE"]),1) if pd.notna(r["HEALTHCARE_SCORE"]) else "N/A"} |
                        <b>Grade:</b> {r.get("HEALTHCARE_GRADE","N/A")} |
                        <b>Facilities:</b> {int(r["TOTAL_FACILITIES"]) if pd.notna(r["TOTAL_FACILITIES"]) else 0} |
                        <b>Hospitals:</b> {int(r["HOSPITAL_COUNT"]) if pd.notna(r["HOSPITAL_COUNT"]) else 0} |
                        <b>Clinics:</b> {int(r["CLINIC_COUNT"]) if pd.notna(r["CLINIC_COUNT"]) else 0}<br>
                        <b>Density:</b> {round(float(r["FACILITIES_PER_SQMILE"]),2) if pd.notna(r["FACILITIES_PER_SQMILE"]) else "N/A"} / sq mi |
                        <b>Core Care:</b> {round(float(r["PCT_CORE_CARE"]),1) if pd.notna(r["PCT_CORE_CARE"]) else "N/A"}% |
                        <b>Valid Phone:</b> {round(float(r["PCT_VALID_PHONE"]),1) if pd.notna(r["PCT_VALID_PHONE"]) else "N/A"}%
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                top3 = healthcare_df.sort_values(["HEALTHCARE_SCORE", "TOTAL_FACILITIES"], ascending=[False, False]).head(3)
                bottom3 = healthcare_df.sort_values(["HEALTHCARE_SCORE", "TOTAL_FACILITIES"], ascending=[True, False]).head(3)

                hot_txt = ", ".join([f"{r['NEIGHBORHOOD_NAME']} ({r['HEALTHCARE_SCORE']:.1f})" for _, r in top3.iterrows()]) if not top3.empty else "No data"
                concern_txt = ", ".join([f"{r['NEIGHBORHOOD_NAME']} ({r['HEALTHCARE_SCORE']:.1f})" for _, r in bottom3.iterrows()]) if not bottom3.empty else "No data"

                st.markdown(
                    f"""
                    <div class="narrative-box-blue">
                        <div class="narrative-neighborhood">Citywide Healthcare Snapshot</div>
                        <b>Healthcare hotspots:</b> {hot_txt}<br>
                        <b>Needs attention:</b> {concern_txt}<br>
                        Compare spatial density, hospital presence, clinics, core care coverage, and quality indicators across neighborhoods.
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            c1, c2 = st.columns([1.2, 1])

            with c1:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.subheader("Top Neighborhoods by Healthcare Score")

                score_source = healthcare_df.copy() if selected_nbhd == "All" else filtered_hc.copy()
                top_score_df = score_source[
                    ["NEIGHBORHOOD_NAME", "HEALTHCARE_SCORE", "TOTAL_FACILITIES", "HEALTHCARE_GRADE"]
                ].sort_values(["HEALTHCARE_SCORE", "TOTAL_FACILITIES"], ascending=[False, False]).head(12)

                score_chart = alt.Chart(top_score_df).mark_bar(
                    cornerRadiusTopLeft=6,
                    cornerRadiusTopRight=6
                ).encode(
                    x=alt.X("HEALTHCARE_SCORE:Q", title="Healthcare Score"),
                    y=alt.Y("NEIGHBORHOOD_NAME:N", sort="-x", title="Neighborhood"),
                    color=alt.Color("HEALTHCARE_GRADE:N", legend=alt.Legend(title="Grade")),
                    tooltip=["NEIGHBORHOOD_NAME", "HEALTHCARE_SCORE", "TOTAL_FACILITIES", "HEALTHCARE_GRADE"]
                ).properties(height=380)

                st.altair_chart(score_chart, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            with c2:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.subheader("Healthcare Grade Distribution")

                grade_source = healthcare_df.copy() if selected_nbhd == "All" else filtered_hc.copy()
                grade_dist = grade_source.groupby("HEALTHCARE_GRADE").size().reset_index(name="COUNT")

                grade_chart = alt.Chart(grade_dist).mark_bar(
                    cornerRadiusTopLeft=6,
                    cornerRadiusTopRight=6
                ).encode(
                    x=alt.X("HEALTHCARE_GRADE:N", title="Grade"),
                    y=alt.Y("COUNT:Q", title="Neighborhood Count"),
                    color=alt.Color("HEALTHCARE_GRADE:N", legend=None),
                    tooltip=["HEALTHCARE_GRADE", "COUNT"]
                ).properties(height=380)

                st.altair_chart(grade_chart, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            c3, c4 = st.columns(2)

            with c3:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.subheader("Healthcare Score Components")

                comp_source = healthcare_df.copy() if selected_nbhd == "All" else filtered_hc.copy()
                component_df = comp_source[
                    [
                        "NEIGHBORHOOD_NAME",
                        "DENSITY_SCORE",
                        "CORE_CARE_SCORE",
                        "CONTACT_QUALITY_SCORE",
                        "DIVERSITY_SCORE"
                    ]
                ].head(12)

                if not component_df.empty:
                    component_long = component_df.melt(
                        id_vars="NEIGHBORHOOD_NAME",
                        var_name="COMPONENT",
                        value_name="VALUE"
                    )

                    component_chart = alt.Chart(component_long).mark_bar().encode(
                        x=alt.X("VALUE:Q", title="Score Contribution"),
                        y=alt.Y("NEIGHBORHOOD_NAME:N", sort="-x", title="Neighborhood"),
                        color=alt.Color("COMPONENT:N", title="Component"),
                        tooltip=["NEIGHBORHOOD_NAME", "COMPONENT", "VALUE"]
                    ).properties(height=420)

                    st.altair_chart(component_chart, use_container_width=True)
                else:
                    st.info("No component data available.")
                st.markdown('</div>', unsafe_allow_html=True)

            with c4:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.subheader("Hotspot Concentration")

                hotspot_source = healthcare_df.copy() if selected_nbhd == "All" else filtered_hc.copy()
                hotspot_df = hotspot_source[
                    ["NEIGHBORHOOD_NAME", "N_HEALTHCARE_CLUSTERS", "CLUSTERED_FACILITY_SHARE_PCT", "HEALTHCARE_SCORE"]
                ].sort_values(["N_HEALTHCARE_CLUSTERS", "CLUSTERED_FACILITY_SHARE_PCT"], ascending=[False, False]).head(12)

                if not hotspot_df.empty:
                    hotspot_chart = alt.Chart(hotspot_df).mark_bar(
                        cornerRadiusTopLeft=6,
                        cornerRadiusTopRight=6
                    ).encode(
                        x=alt.X("CLUSTERED_FACILITY_SHARE_PCT:Q", title="Clustered Facility Share (%)"),
                        y=alt.Y("NEIGHBORHOOD_NAME:N", sort="-x", title="Neighborhood"),
                        color=alt.Color("HEALTHCARE_SCORE:Q", scale=alt.Scale(scheme="blues"), legend=alt.Legend(title="Score")),
                        tooltip=["NEIGHBORHOOD_NAME", "N_HEALTHCARE_CLUSTERS", "CLUSTERED_FACILITY_SHARE_PCT", "HEALTHCARE_SCORE"]
                    ).properties(height=420)

                    st.altair_chart(hotspot_chart, use_container_width=True)
                else:
                    st.info("No hotspot data available.")
                st.markdown('</div>', unsafe_allow_html=True)

            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Healthcare Neighborhood Table")

            table_source = healthcare_df.copy() if selected_nbhd == "All" else filtered_hc.copy()
            scorecard_df = table_source[
                [
                    "NEIGHBORHOOD_NAME",
                    "CITY",
                    "TOTAL_FACILITIES",
                    "HOSPITAL_COUNT",
                    "CLINIC_COUNT",
                    "FACILITIES_PER_SQMILE",
                    "PCT_CORE_CARE",
                    "PCT_VALID_PHONE",
                    "HEALTHCARE_SCORE",
                    "HEALTHCARE_GRADE",
                    "N_HEALTHCARE_CLUSTERS",
                    "CLUSTERED_FACILITY_SHARE_PCT"
                ]
            ].sort_values(["HEALTHCARE_SCORE", "TOTAL_FACILITIES"], ascending=[False, False])

            st.dataframe(scorecard_df, use_container_width=True, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # ──────────────────────────────────────────────────────────────────────
        # SUBTAB 2 — MAPS
        # ──────────────────────────────────────────────────────────────────────
        with htab2:
            col_map, col_detail = st.columns([1.6, 1])

            with col_map:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                st.subheader("Boston Neighborhood Healthcare Map")
                st.caption(
                    f"Viewing: **{selected_nbhd}** (highlighted with white border)"
                    if active_hc_nbhd
                    else "Neighborhood polygons are shaded by healthcare access score."
                )

                if filtered_hc_map.empty:
                    st.warning("No map geometry data available.")
                else:
                    geojson_data = build_healthcare_geojson_feature_collection(
                        filtered_hc_map,
                        sel_nbhd=active_hc_nbhd
                    )

                    if active_hc_nbhd:
                        sel_row = filtered_hc_map[filtered_hc_map["NEIGHBORHOOD_NAME"] == active_hc_nbhd]
                        if not sel_row.empty:
                            center_lat = float(sel_row["CENTROID_LAT"].iloc[0])
                            center_lng = float(sel_row["CENTROID_LONG"].iloc[0])
                            zoom = 12.2
                        else:
                            center_lat = float(filtered_hc_map["CENTROID_LAT"].mean())
                            center_lng = float(filtered_hc_map["CENTROID_LONG"].mean())
                            zoom = 10.8
                    else:
                        center_lat = float(filtered_hc_map["CENTROID_LAT"].mean())
                        center_lng = float(filtered_hc_map["CENTROID_LONG"].mean())
                        zoom = 10.8

                    layers = [
                        pdk.Layer(
                            "GeoJsonLayer",
                            data=geojson_data,
                            filled=True,
                            stroked=True,
                            pickable=True,
                            auto_highlight=True,
                            get_fill_color="properties.fill_color",
                            get_line_color=[255, 255, 255, 200],
                            line_width_min_pixels=1.5
                        )
                    ]

                    if active_hc_nbhd:
                        sel_feats = [f for f in geojson_data["features"] if f["properties"].get("is_selected")]
                        if sel_feats:
                            layers.append(
                                pdk.Layer(
                                    "GeoJsonLayer",
                                    data={"type": "FeatureCollection", "features": sel_feats},
                                    filled=False,
                                    stroked=True,
                                    get_line_color=[255, 255, 255, 255],
                                    line_width_min_pixels=3
                                )
                            )

                    deck = pdk.Deck(
                        layers=layers,
                        initial_view_state=pdk.ViewState(
                            latitude=center_lat,
                            longitude=center_lng,
                            zoom=zoom,
                            pitch=0
                        ),
                        tooltip={
                            "html": """
                                <b>{NEIGHBORHOOD_NAME}</b><br/>
                                City: {CITY}<br/>
                                Healthcare Score: {HEALTHCARE_SCORE}<br/>
                                Grade: {HEALTHCARE_GRADE}<br/>
                                Total Facilities: {TOTAL_FACILITIES}<br/>
                                Hospitals: {HOSPITAL_COUNT}<br/>
                                Clinics: {CLINIC_COUNT}<br/>
                                Facilities / Sq Mile: {FACILITIES_PER_SQMILE}
                            """,
                            "style": {
                                "backgroundColor": "white",
                                "color": "#0f172a",
                                "fontSize": "13px"
                            }
                        },
                        map_style="mapbox://styles/mapbox/light-v10"
                    )

                    st.pydeck_chart(deck, use_container_width=True)

                    l1, l2, l3, l4 = st.columns(4)
                    l1.markdown("🟩 **Excellent**")
                    l2.markdown("🟦 **Good**")
                    l3.markdown("🟨 **Moderate**")
                    l4.markdown("🟥 **Limited**")

                st.markdown('</div>', unsafe_allow_html=True)

            with col_detail:
                st.markdown('<div class="section-card">', unsafe_allow_html=True)
                if active_hc_nbhd:
                    st.subheader(f"{active_hc_nbhd} — Snapshot")
                    nb = healthcare_df[healthcare_df["NEIGHBORHOOD_NAME"] == active_hc_nbhd]
                    if not nb.empty:
                        r = nb.iloc[0]
                        st.markdown(f"""
**Healthcare Score:** {round(float(r['HEALTHCARE_SCORE']),1) if pd.notna(r['HEALTHCARE_SCORE']) else "N/A"}
**Grade:** {r.get('HEALTHCARE_GRADE',"N/A")}

**Total Facilities:** {int(r['TOTAL_FACILITIES']) if pd.notna(r['TOTAL_FACILITIES']) else 0}
**Hospitals:** {int(r['HOSPITAL_COUNT']) if pd.notna(r['HOSPITAL_COUNT']) else 0}
**Clinics:** {int(r['CLINIC_COUNT']) if pd.notna(r['CLINIC_COUNT']) else 0}
**Facilities / Sq Mile:** {round(float(r['FACILITIES_PER_SQMILE']),2) if pd.notna(r['FACILITIES_PER_SQMILE']) else "N/A"}
**Core Care %:** {round(float(r['PCT_CORE_CARE']),1) if pd.notna(r['PCT_CORE_CARE']) else "N/A"}
**Valid Phone %:** {round(float(r['PCT_VALID_PHONE']),1) if pd.notna(r['PCT_VALID_PHONE']) else "N/A"}
**Healthcare Clusters:** {int(r['N_HEALTHCARE_CLUSTERS']) if pd.notna(r['N_HEALTHCARE_CLUSTERS']) else 0}
**Clustered Share %:** {round(float(r['CLUSTERED_FACILITY_SHARE_PCT']),1) if pd.notna(r['CLUSTERED_FACILITY_SHARE_PCT']) else "N/A"}
                        """)

                        narr = r.get("HEALTHCARE_NARRATIVE", None)
                        if pd.notna(narr):
                            st.markdown(
                                f'<div class="narrative-box"><div class="narrative-neighborhood">AI Narrative</div>{narr}</div>',
                                unsafe_allow_html=True
                            )
                else:
                    st.subheader("Healthcare Summary")
                    st.markdown("**Top 5 — Highest Healthcare Score**")
                    top5 = healthcare_df.sort_values("HEALTHCARE_SCORE", ascending=False).head(5)
                    st.dataframe(
                        top5[["NEIGHBORHOOD_NAME", "HEALTHCARE_SCORE", "HEALTHCARE_GRADE", "TOTAL_FACILITIES"]],
                        use_container_width=True,
                        hide_index=True
                    )

                    st.markdown("**Bottom 5 — Lowest Healthcare Score**")
                    bottom5 = healthcare_df.sort_values("HEALTHCARE_SCORE", ascending=True).head(5)
                    st.dataframe(
                        bottom5[["NEIGHBORHOOD_NAME", "HEALTHCARE_SCORE", "HEALTHCARE_GRADE", "TOTAL_FACILITIES"]],
                        use_container_width=True,
                        hide_index=True
                    )
                st.markdown('</div>', unsafe_allow_html=True)

        # ──────────────────────────────────────────────────────────────────────
        # SUBTAB 3 — AI ASSISTANT
        # ──────────────────────────────────────────────────────────────────────
        with htab3:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("AI Healthcare Assistant")
            st.caption("Ask questions about neighborhood healthcare access, rankings, hotspots, gaps, or comparisons.")

            if "healthcare_chat_history" not in st.session_state:
                st.session_state.healthcare_chat_history = []

            selected_context = filtered_hc[
                [
                    "NEIGHBORHOOD_NAME",
                    "CITY",
                    "TOTAL_FACILITIES",
                    "HOSPITAL_COUNT",
                    "CLINIC_COUNT",
                    "FACILITIES_PER_SQMILE",
                    "PCT_CORE_CARE",
                    "PCT_VALID_PHONE",
                    "HEALTHCARE_SCORE",
                    "HEALTHCARE_GRADE",
                    "N_HEALTHCARE_CLUSTERS",
                    "CLUSTERED_FACILITY_SHARE_PCT",
                    "HEALTHCARE_NARRATIVE"
                ]
            ].head(20)

            context_text = selected_context.to_csv(index=False)

            for message in st.session_state.healthcare_chat_history:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

            user_question = st.chat_input(
                "Ask about healthcare rankings, hotspots, comparisons, or access gaps...",
                key="healthcare_chat_input"
            )

            if user_question:
                st.session_state.healthcare_chat_history.append({"role": "user", "content": user_question})

                with st.chat_message("user"):
                    st.markdown(user_question)

                prompt = f"""
You are a healthcare access analyst for the NeighbourWise AI platform.

Answer the user's question using only the healthcare context below.
Be smart, polished, insight-rich, and easy to read.
Do not invent facts.
If a conclusion is uncertain, say so clearly.

Output style:
- Start with a bold one-line takeaway.
- Then use exactly these 3 short sections:
  1. What the data shows
  2. Why it matters
  3. Best next interpretation / recommendation

Healthcare context:
{context_text}

User question:
{user_question}
"""

                try:
                    result = session.sql(f"""
                        SELECT SNOWFLAKE.CORTEX.COMPLETE(
                            'mistral-large',
                            $$ {prompt} $$
                        ) AS RESPONSE
                    """).to_pandas()
                    ai_response = result.iloc[0]["RESPONSE"]
                except Exception as e:
                    ai_response = f"AI response could not be generated. Error: {e}"

                with st.chat_message("assistant"):
                    st.markdown(ai_response)

                st.session_state.healthcare_chat_history.append({"role": "assistant", "content": ai_response})

            st.markdown("### Suggested Questions")
            st.markdown("""
- Which Boston neighborhoods have the best healthcare access?
- Compare Back Bay and Allston on healthcare access.
- Which areas have low healthcare scores but still have multiple facilities?
- Which neighborhoods look like healthcare hotspots?
- Where is contact quality weaker across facilities?
            """)
            st.markdown('</div>', unsafe_allow_html=True)