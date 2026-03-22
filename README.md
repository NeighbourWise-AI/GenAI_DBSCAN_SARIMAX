# NeighbourWise AI — GenAI DBSCAN & SARIMAX 📍

## 🏙️ About NeighbourWise AI

NeighbourWise AI is a **neighborhood intelligence platform** for the Greater Boston metro area. It helps people making relocation decisions by scoring and comparing 51 neighborhoods and cities across multiple livability dimensions — crime & safety, grocery access, healthcare availability, transit, schools, restaurants, and more.

The platform ingests data from public APIs and open data portals, transforms it through a Snowflake + dbt pipeline, and uses GenAI (Snowflake Cortex) to generate explainable, plain-English neighborhood recommendations.

## 🔬 What This Repository Covers

This repo contains the **advanced analytics layer** — the spatial clustering and time-series forecasting that sit on top of the core data pipeline. Specifically, it applies **DBSCAN hotspot detection** and **SARIMAX forecasting** across three domains:

| Domain | DBSCAN | SARIMAX | Cortex Narrative |
|--------|--------|---------|------------------|
| **Crime** | ✅ Two-pass clustering | ✅ 12-month forecast | ✅ Safety narratives |
| **Grocery** | ✅ Store access clustering | — | ✅ Access narratives |
| **Healthcare** | ✅ Facility access clustering | — | ✅ Access narratives |

These results feed into neighborhood scoring tables that rate all 51 locations on a 0–100 scale, with LLM-generated narrative summaries for each.

---

## 🧠 How It Works

### DBSCAN — Spatial Hotspot Detection (All 3 Domains)

Traditional grid-based or zip-code-level analysis hides block-level patterns. DBSCAN (Density-Based Spatial Clustering of Applications with Noise) discovers **arbitrarily shaped clusters** without requiring a predefined number of clusters — ideal for finding organic hotspots.

For crime, a **two-pass approach** is used:
- **Pass 1** (eps ≈ 200m, min_samples = 50) → Broad hotspot regions
- **Pass 2** (eps ≈ 75m, min_samples = 15) → Concentrated micro-clusters within hotspot regions

Grocery and Healthcare use single-pass DBSCAN to map facility density and identify access gaps.

### SARIMAX — Time-Series Forecasting (Crime)

```
SARIMAX(1,1,1) × (1,1,1,12)
```

Captures both **trend** (year-over-year normalization) and **seasonality** (summer peaks, winter dips) in Boston crime data. Produces 12-month rolling forecasts with 95% confidence intervals.

### Safety Scoring — 6-Signal Weighted Formula

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

---

## 🏗️ Architecture


### Pipeline Flow

Snowflake Marts → Python Scripts (DBSCAN / SARIMAX) → Snowflake CRIME_ANALYSIS Schema → Cortex LLM Narratives → Streamlit Dashboard

---

## 📁 Repository Structure

```
GenAI_DBSCAN_SARIMAX/
├── airflow/       # Airflow DAGs — data ingestion for 4 crime sources
├── dbt/           # dbt models — STG → INT → MRT transformation layers
├── scripts/       # Python scripts — DBSCAN, SARIMAX, Cortex narratives
├── streamlit/     # Unified Streamlit dashboard (Crime + Grocery + Healthcare)
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

- Implemented **two-pass DBSCAN** for multi-resolution hotspot detection (macro regions → micro-clusters)
- Built **SARIMAX forecasting** with seasonal differencing (D=1, s=12) capturing Boston's annual crime patterns
- Designed **two-track scoring architecture** — incident-level stats for Boston/Cambridge/Somerville, aggregate stats for 11 FBI cities — with proportionally redistributed signal weights
- Solved **apples-to-apples comparison** by excluding LOW severity service calls from scoring denominators
- Integrated **4 distinct crime data sources** (Analyze Boston, Cambridge Open Data, Somerville Socrata, FBI CDE API) into a unified pipeline
- Generated **LLM-powered neighborhood narratives** via Snowflake Cortex with city-specific prompt templates

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

To add spatial intelligence and temporal forecasting to the NeighbourWise AI platform — transforming raw location data into actionable, explainable neighborhood insights powered by DBSCAN clustering, SARIMAX forecasting, and GenAI narratives.

---

## 📜 License

[MIT](LICENSE)
