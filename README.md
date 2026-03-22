# NeighborWise AI — GenAI DBSCAN & SARIMAX 📍

## 🏙️ About NeighborWise AI

NeighborWise AI is a **neighborhood intelligence platform** for the Greater Boston metro area. It helps people making relocation decisions by scoring and comparing 51 neighborhoods and cities across multiple livability dimensions — crime & safety, grocery access, healthcare availability, transit, schools, restaurants, and more.

The platform ingests data from public APIs and open data portals, transforms it through a Snowflake + dbt pipeline, and uses GenAI (Snowflake Cortex) to generate explainable, plain-English neighborhood recommendations.

## 🔬 What This Repository Covers

This repo contains the **advanced analytics layer** — the spatial clustering and time-series forecasting that sit on top of the core data pipeline. Specifically, it applies **DBSCAN hotspot detection** and **SARIMAX forecasting** across three domains:

| Domain | DBSCAN | SARIMAX | Cortex Narrative |
|--------|--------|---------|------------------|
| **Crime** | ✅ Two-pass clustering | ✅ 12-month forecast | ✅ Safety narratives |
| **Grocery** | ✅ Store access clustering | — | ✅ Food access narratives |
| **Healthcare** | ✅ Facility density clustering | — | ✅ Healthcare access narratives |

These results feed into neighborhood scoring tables that rate all 51 locations on a 0–100 scale, with LLM-generated narrative summaries for each.

---

## 🧠 How It Works

### DBSCAN — Spatial Hotspot Detection (All 3 Domains)

Traditional grid-based or zip-code-level analysis hides block-level patterns. DBSCAN (Density-Based Spatial Clustering of Applications with Noise) discovers **arbitrarily shaped clusters** without requiring a predefined number of clusters — ideal for finding organic hotspots.

For crime, a **two-pass approach** is used:
- **Pass 1** (eps ≈ 200m, min_samples = 50) → Broad hotspot regions
- **Pass 2** (eps ≈ 75m, min_samples = 15) → Concentrated micro-clusters within hotspot regions

Grocery uses single-pass DBSCAN with haversine distance (eps = 200m, min_samples = 3) — wider parameters than crime since stores are sparser than individual incidents.

Healthcare uses spatial binning on lat/long grid cells to identify concentrated facility clusters vs. isolated facilities.

### SARIMAX — Time-Series Forecasting (Crime)

```
SARIMAX(1,1,1) × (1,1,1,12)
```

Captures both **trend** (year-over-year normalization) and **seasonality** (summer peaks, winter dips) in Boston crime data. Produces 12-month rolling forecasts with 95% confidence intervals.

---

## 🔴 Crime Scoring — 6-Signal Weighted Formula

```
Score = 100
  - (40% × violent_rate)
  - (20% × crime_density)
  - (15% × property_rate)
  - (10% × high_severity)
  - (10% × yoy_trend)
  - ( 5% × night_crime)
```

Grades: **EXCELLENT** ≥75 · **GOOD** ≥50 · **MODERATE** ≥25 · **HIGH CONCERN** <25

**Output Tables (CRIME_ANALYSIS schema):**
- `CA_CRIME_HOTSPOT_CLUSTERS` — Cluster centroids, density stats, top offenses
- `CA_CRIME_CLUSTER_POINTS` — Individual incidents with cluster assignments
- `CA_CRIME_FORECAST` — Monthly forecast with 95% CI
- `CA_CRIME_SAFETY_NARRATIVE` — LLM-generated safety narrative per neighborhood

---

## 🟢 Grocery Scoring — 4-Tier Access Classification

DBSCAN identifies dense store zones vs. isolated stores per neighborhood. Each neighborhood is classified into an access tier based on essential food source count (supermarkets, produce markets, meat/fish markets):

- **HIGH_ACCESS** — 10+ essential food sources
- **GOOD_ACCESS** — 6–10 essential food sources
- **FAIR_ACCESS** — 3–5 essential food sources
- **LOW_ACCESS** — ≤2 essential food sources

Metrics include store type breakdown (supermarkets, convenience, specialty, pharmacies, farmers markets), clustered vs. isolated store percentages, and essential food source share.

**Output Tables (GROCERY_ANALYSIS schema):**
- `GA_GROCERY_HOTSPOT_CLUSTERS` — DBSCAN cluster stats + store type breakdown per neighborhood
- `GA_GROCERY_NARRATIVE` — LLM-generated food access narrative per neighborhood

---

## 🔵 Healthcare Scoring — 4-Component Formula

| Component | Max Points | What It Measures |
|-----------|-----------|------------------|
| Density Score | 35 | Facilities per square mile |
| Core Care Score | 30 | Hospital + clinic share of total facilities |
| Contact Quality Score | 20 | Percentage with valid phone numbers |
| Diversity Score | 15 | Number of distinct facility type groups (out of 4) |

Grades: **EXCELLENT** ≥75 · **GOOD** ≥50 · **MODERATE** ≥25 · **LIMITED** <25

Facility types tracked: Inpatient/Hospital, Outpatient/Clinic, Public Health/Community, and Specialty/Other.

**Output Tables (HEALTHCARE_ANALYSIS schema):**
- `HA_HEALTHCARE_ACCESS_PROFILE` — Per-neighborhood scoring with facility breakdowns
- `HA_HEALTHCARE_HOTSPOT_CLUSTERS` — Clustered vs. isolated facility percentages
- `HA_HEALTHCARE_NARRATIVE` — LLM-generated healthcare access narrative per neighborhood

---

## 🏗️ Architecture

![System Architecture](Architectures/DBSCAN%20+%20SARIMAX%20Architecture.png)

### Pipeline Flow

Snowflake Marts → Python Scripts / dbt Models (DBSCAN / SARIMAX) → Analysis Schemas → Cortex LLM Narratives → Streamlit Dashboard

---

## 📁 Repository Structure

```
GenAI_DBSCAN_SARIMAX/
├── airflow/dags/
│   ├── boston_api_to_s3.py
│   ├── cambridge_api_to_s3_to_snowflake.py
│   ├── district_mapping_to_s3_to_snowflake.py
│   ├── grocery_unstructured_scrape_dag.py
│   ├── healthcare_dataload_dag.py
│   └── master_location_to_s3_to_snowflake.py
│
├── dbt/models/
│   ├── intermediate/
│   │   ├── INT_BOSTON_CRIME.sql
│   │   ├── INT_BOSTON_GROCERY_STORES.sql
│   │   └── INT_BOSTON_HEALTHCARE.sql
│   └── marts/
│       ├── MRT_BOSTON_CRIME.sql
│       ├── MRT_BOSTON_GROCERY_STORES.sql
│       └── MRT_BOSTON_HEALTHCARE.sql
│
├── scripts/
│   ├── crime_hotspot_analysis.py       # Two-pass DBSCAN + SARIMAX + Cortex narratives
│   ├── Grocery_analysis.py             # DBSCAN grocery clustering + Cortex narratives
│   └── healthcare_analysis.sql         # Healthcare scoring + clustering + Cortex narratives
│
├── streamlit/
│   ├── streamlit_app.py                # Unified dashboard (Crime + Grocery + Healthcare)
│   └── environment.yml
│
├── LICENSE
└── README.md
```

---

## ⚙️ Tech Stack

- **Snowflake** — Data warehouse + Cortex LLM (mistral-large)
- **Apache Airflow** — Orchestration & scheduling (Docker)
- **dbt** — Data transformations (STG → INT → MRT)
- **Amazon S3** — Object storage
- **scikit-learn** — DBSCAN spatial clustering
- **statsmodels** — SARIMAX time-series forecasting
- **Nominatim** — Geocoding (OpenStreetMap)
- **Streamlit** — Interactive dashboard
- **Python 3.10+**

---

## 🔄 Key Engineering Highlights

- Implemented **two-pass DBSCAN** for multi-resolution crime hotspot detection (macro regions → micro-clusters)
- Applied **DBSCAN with haversine distance** to grocery store locations for food desert identification
- Built **spatial binning clustering** for healthcare facility density mapping
- Designed **SARIMAX forecasting** with seasonal differencing (D=1, s=12) for crime trend prediction
- Created **two-track crime scoring** — incident-level for Boston/Cambridge/Somerville, FBI aggregates for 11 Greater Boston cities — with redistributed signal weights
- Built **4-component healthcare scoring** (density, core care, contact quality, diversity)
- Built **4-tier grocery access classification** based on essential food source counts
- Generated **LLM-powered narratives** via Snowflake Cortex across all three domains with city-specific prompts

---

## 📊 Data Coverage

| Region | Locations | Crime Source |
|--------|-----------|-------------|
| Boston | 26 neighborhoods | Analyze Boston API (~251K incidents) |
| Cambridge | 13 neighborhoods | Cambridge Open Data (~20K incidents) |
| Somerville | 1 city | Somerville Socrata API (~7.8K incidents) |
| Greater Boston | 11 cities | FBI Crime Data Explorer (annual aggregates) |

**Greater Boston cities:** Arlington · Brookline · Newton · Watertown · Medford · Malden · Revere · Chelsea · Everett · Salem · Quincy

**Total:** 51 locations scored across all domains

---

## 🎯 Goal

To add spatial intelligence and temporal forecasting to the NeighborWise AI platform — transforming raw location data into actionable, explainable neighborhood insights powered by DBSCAN clustering, SARIMAX forecasting, and GenAI narratives.

---

## 📜 License

[MIT](LICENSE)
