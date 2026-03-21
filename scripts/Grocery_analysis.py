"""
NeighbourWise AI — Grocery Access Analysis
Methodology adapted from Singh et al. (2025) Crime Pulse and Cesario (2023).

Applies spatial clustering (DBSCAN) and Cortex narrative generation to
Boston-area grocery store data from MRT_BOSTON_GROCERY_STORES.

Two output tables in GROCERY_ANALYSIS schema:
  - GA_GROCERY_HOTSPOT_CLUSTERS   : DBSCAN-derived food access cluster stats per neighborhood
  - GA_GROCERY_NARRATIVE          : Cortex-generated food access narrative per neighborhood

Run on Mac directly (NOT inside Docker).
"""

import os
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sklearn.cluster import DBSCAN
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

warnings.filterwarnings("ignore")

# ── CONFIG ────────────────────────────────────────────────────────────────────
SF_CONFIG = {
    "account":       "pgb87192",
    "user":          os.environ["SNOWFLAKE_USER"],
    "password":      os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse":     "NEIGHBOURWISE_AI",
    "database":      "NEIGHBOURWISE_DOMAINS",
    "schema":        "GROCERY_ANALYSIS",
    "role":          "TRAINING_ROLE",
    "insecure_mode": True,
}

# DBSCAN params — 200m radius, min 3 stores to form a cluster
# Wider than crime (75m) because stores are sparser than individual incidents
EPS_KM      = 0.20   # 200m radius
MIN_SAMPLES = 3


# ── SNOWFLAKE CONNECTION ──────────────────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(**SF_CONFIG)


# ── STEP 1: PULL GROCERY DATA ─────────────────────────────────────────────────
def load_grocery_data(conn) -> pd.DataFrame:
    """
    Pull from MRT_BOSTON_GROCERY_STORES — already neighborhood-resolved.
    Uses 2021 data where available, falls back to 2017 for neighborhoods
    not covered in 2021 survey.
    """
    query = """
    SELECT
        OBJECTID,
        STORE_NAME,
        STREET_ADDRESS,
        ZIP_CODE,
        DATA_YEAR,
        DATA_VINTAGE,
        STORE_TYPE,
        NAICS_CATEGORY,
        IS_ESSENTIAL_FOOD_SOURCE,
        IS_PHARMACY_OR_DRUG_STORE,
        IS_SPECIALTY_STORE,
        IS_LARGE_FORMAT,
        IS_CONVENIENCE_TYPE,
        LAT,
        LONG,
        LOCATION_ID,
        NEIGHBORHOOD_NAME,
        CITY
    FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_GROCERY_STORES
    WHERE HAS_VALID_LOCATION = TRUE
      AND NEIGHBORHOOD_NAME IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY UPPER(TRIM(STORE_NAME)), UPPER(TRIM(STREET_ADDRESS)), NEIGHBORHOOD_NAME
        ORDER BY DATA_YEAR DESC
    ) = 1
    """
    print("Loading grocery data from MRT_BOSTON_GROCERY_STORES...")
    df = pd.read_sql(query, conn)
    df.columns = [c.upper() for c in df.columns]
    print(f"  {len(df):,} stores loaded across {df['NEIGHBORHOOD_NAME'].nunique()} neighborhoods")
    return df


# ── STEP 2: DBSCAN CLUSTER ANALYSIS ──────────────────────────────────────────
def dbscan_grocery_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    DBSCAN spatial clustering per neighborhood to identify food access clusters.

    Metrics computed per neighborhood:
      - Total stores and breakdown by type
      - Essential food source count and share
      - Number of geographic clusters (dense store zones)
      - Isolated stores (noise) — stores with no nearby neighbors
      - Access tier based on essential store density

    Adapted from Singh et al. (2025) haversine DBSCAN methodology.
    """
    results = []
    eps_rad = EPS_KM / 6371.0

    for nbhd, grp in df.groupby("NEIGHBORHOOD_NAME"):
        coords = grp[["LAT", "LONG"]].dropna().values
        total_stores = len(coords)

        if total_stores < MIN_SAMPLES:
            # Too few stores to cluster — still record summary stats
            n_clusters       = 0
            clustered_share  = 0.0
            isolated_pct     = 100.0
        else:
            coords_rad = np.radians(coords)
            db         = DBSCAN(
                eps=eps_rad, min_samples=MIN_SAMPLES, metric="haversine"
            ).fit(coords_rad)
            labels = db.labels_

            n_clusters      = len(set(labels)) - (1 if -1 in labels else 0)
            noise_count     = (labels == -1).sum()
            isolated_pct    = round(noise_count / len(labels) * 100, 1)
            clustered_share = round(100 - isolated_pct, 1)

        # Store type breakdown
        type_counts = grp["STORE_TYPE"].value_counts().to_dict()
        essential   = grp["IS_ESSENTIAL_FOOD_SOURCE"].astype(str).str.lower() == "true"
        essential_n = essential.sum()
        essential_pct = round(essential_n / total_stores * 100, 1) if total_stores > 0 else 0.0

        supermarkets     = type_counts.get("SUPERMARKET", 0)
        convenience      = type_counts.get("CONVENIENCE_STORE", 0)
        specialty        = (
            type_counts.get("FRUIT_VEG_MARKET", 0) +
            type_counts.get("MEAT_MARKET", 0) +
            type_counts.get("FISH_SEAFOOD_MARKET", 0) +
            type_counts.get("SPECIALTY_FOOD_STORE", 0)
        )
        pharmacies       = type_counts.get("PHARMACY", 0)
        farmers_markets  = (
            type_counts.get("FARMERS_MARKET", 0) +
            type_counts.get("WINTER_MARKET", 0)
        )

        # Access tier — 4-tier system based on essential store count
        if essential_n <= 2:
            access_tier = "LOW_ACCESS"
        elif essential_n <= 5:
            access_tier = "FAIR_ACCESS"
        elif essential_n <= 10:
            access_tier = "GOOD_ACCESS"
        else:
            access_tier = "HIGH_ACCESS"

        results.append({
            "NEIGHBORHOOD_NAME":         nbhd,
            "CITY":                      grp["CITY"].iloc[0],
            "TOTAL_STORES":              total_stores,
            "ESSENTIAL_STORE_COUNT":     int(essential_n),
            "ESSENTIAL_STORE_PCT":       essential_pct,
            "SUPERMARKET_COUNT":         int(supermarkets),
            "CONVENIENCE_STORE_COUNT":   int(convenience),
            "SPECIALTY_STORE_COUNT":     int(specialty),
            "PHARMACY_COUNT":            int(pharmacies),
            "FARMERS_MARKET_COUNT":      int(farmers_markets),
            "N_STORE_CLUSTERS":          int(n_clusters),
            "CLUSTERED_STORE_SHARE_PCT": clustered_share,
            "ISOLATED_STORE_PCT":        isolated_pct,
            "ACCESS_TIER":               access_tier,
        })

    df_out = pd.DataFrame(results).sort_values(
        "ESSENTIAL_STORE_COUNT", ascending=False
    ).reset_index(drop=True)

    print(f"  DBSCAN clustering completed for {len(df_out)} neighborhoods")
    tier_counts = df_out["ACCESS_TIER"].value_counts().to_dict()
    print(f"  Tier distribution: {tier_counts}")
    return df_out


# ── STEP 3: CORTEX NARRATIVE GENERATION ──────────────────────────────────────
def generate_cortex_narratives(conn, cluster_df: pd.DataFrame,
                                df: pd.DataFrame) -> pd.DataFrame:
    """
    Snowflake Cortex COMPLETE — food access narrative per neighborhood.
    Combines DBSCAN cluster stats, store type mix, and access tier.
    """
    narratives = []
    cluster_lookup = cluster_df.set_index("NEIGHBORHOOD_NAME").to_dict("index")

    # Most recent data year per neighborhood
    latest_year = (
        df.sort_values("DATA_YEAR", ascending=False)
          .groupby("NEIGHBORHOOD_NAME")["DATA_YEAR"]
          .first()
          .to_dict()
    )

    for nbhd, stats in cluster_lookup.items():
        data_year = latest_year.get(nbhd, "unknown")

        # Human-readable tier label for the prompt
        tier_label = stats["ACCESS_TIER"].replace("_", " ").lower()

        prompt = (
            f"You are a neighborhood food access analyst writing for a Boston/Cambridge, "
            f"Massachusetts neighborhood guide. "
            f"Write a 2-3 sentence food access summary for the {nbhd} neighborhood "
            f"in the Greater Boston area. "
            f"IMPORTANT: {nbhd} is a neighborhood in the Boston/Cambridge, Massachusetts area. "
            f"Do not confuse it with any place of the same name elsewhere.\n\n"
            f"Data (from {data_year} Massachusetts grocery licensing survey):\n"
            f"- Total grocery and food retail stores: {stats['TOTAL_STORES']}\n"
            f"- Essential food sources (supermarkets, produce, meat/fish markets): "
            f"{stats['ESSENTIAL_STORE_COUNT']} ({stats['ESSENTIAL_STORE_PCT']}% of stores)\n"
            f"- Supermarkets: {stats['SUPERMARKET_COUNT']}\n"
            f"- Convenience stores: {stats['CONVENIENCE_STORE_COUNT']}\n"
            f"- Specialty food stores: {stats['SPECIALTY_STORE_COUNT']}\n"
            f"- Pharmacies: {stats['PHARMACY_COUNT']}\n"
            f"- Farmers/winter markets: {stats['FARMERS_MARKET_COUNT']}\n"
            f"- Geographic food clusters (dense store zones): {stats['N_STORE_CLUSTERS']}\n"
            f"- Share of stores in clusters: {stats['CLUSTERED_STORE_SHARE_PCT']}%\n"
            f"- Food access tier: {tier_label}\n\n"
            f"Write a clear, factual, non-alarmist summary suitable for a neighborhood "
            f"recommendation app. Mention the key food access strengths or gaps. "
            f"Do not use bullet points. Keep it under 70 words."
        )

        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', %s)", (prompt,)
            )
            narrative = cur.fetchone()[0].strip()
            cur.close()
        except Exception as e:
            narrative = (
                f"{nbhd} has {stats['ESSENTIAL_STORE_COUNT']} essential food sources "
                f"out of {stats['TOTAL_STORES']} total stores, "
                f"classified as {tier_label}."
            )
            print(f"  Cortex error for {nbhd}: {e}")

        narratives.append({
            "NEIGHBORHOOD_NAME":         nbhd,
            "CITY":                      stats["CITY"],
            "ACCESS_TIER":               stats["ACCESS_TIER"],
            "TOTAL_STORES":              int(stats["TOTAL_STORES"]),
            "ESSENTIAL_STORE_COUNT":     int(stats["ESSENTIAL_STORE_COUNT"]),
            "ESSENTIAL_STORE_PCT":       float(stats["ESSENTIAL_STORE_PCT"]),
            "N_STORE_CLUSTERS":          int(stats["N_STORE_CLUSTERS"]),
            "DATA_YEAR":                 int(data_year) if data_year != "unknown" else None,
            "FOOD_ACCESS_NARRATIVE":     narrative,
            "RELIABILITY_FLAG":          (
                "LOW" if stats["TOTAL_STORES"] < 5 else "HIGH"
            ),
        })

    df_out = pd.DataFrame(narratives)
    print(f"  Generated {len(df_out)} Cortex narratives")
    return df_out


# ── STEP 4: WRITE RESULTS TO SNOWFLAKE ───────────────────────────────────────
def _create_schema_and_tables(cur):
    """Create GROCERY_ANALYSIS schema and both output tables."""
    cur.execute(
        "CREATE SCHEMA IF NOT EXISTS NEIGHBOURWISE_DOMAINS.GROCERY_ANALYSIS"
    )

    cur.execute("""
    CREATE OR REPLACE TABLE NEIGHBOURWISE_DOMAINS.GROCERY_ANALYSIS.GA_GROCERY_HOTSPOT_CLUSTERS (
        NEIGHBORHOOD_NAME          VARCHAR(100),
        CITY                       VARCHAR(100),
        TOTAL_STORES               INTEGER,
        ESSENTIAL_STORE_COUNT      INTEGER,
        ESSENTIAL_STORE_PCT        FLOAT,
        SUPERMARKET_COUNT          INTEGER,
        CONVENIENCE_STORE_COUNT    INTEGER,
        SPECIALTY_STORE_COUNT      INTEGER,
        PHARMACY_COUNT             INTEGER,
        FARMERS_MARKET_COUNT       INTEGER,
        N_STORE_CLUSTERS           INTEGER,
        CLUSTERED_STORE_SHARE_PCT  FLOAT,
        ISOLATED_STORE_PCT         FLOAT,
        ACCESS_TIER                VARCHAR(20),
        LOAD_TIMESTAMP             TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )""")

    cur.execute("""
    CREATE OR REPLACE TABLE NEIGHBOURWISE_DOMAINS.GROCERY_ANALYSIS.GA_GROCERY_NARRATIVE (
        NEIGHBORHOOD_NAME       VARCHAR(100),
        CITY                    VARCHAR(100),
        ACCESS_TIER             VARCHAR(20),
        TOTAL_STORES            INTEGER,
        ESSENTIAL_STORE_COUNT   INTEGER,
        ESSENTIAL_STORE_PCT     FLOAT,
        N_STORE_CLUSTERS        INTEGER,
        DATA_YEAR               INTEGER,
        FOOD_ACCESS_NARRATIVE   VARCHAR(2000),
        RELIABILITY_FLAG        VARCHAR(10),
        LOAD_TIMESTAMP          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )""")


def write_results(conn, cluster_df: pd.DataFrame, narrative_df: pd.DataFrame):
    """Write both output tables to Snowflake GROCERY_ANALYSIS schema."""
    cur = conn.cursor()
    _create_schema_and_tables(cur)
    cur.close()

    def _upload(df, table):
        success, nchunks, nrows, _ = write_pandas(
            conn, df, table,
            database="NEIGHBOURWISE_DOMAINS",
            schema="GROCERY_ANALYSIS",
            auto_create_table=False,
            overwrite=False,
        )
        print(f"  Wrote {nrows} rows to {table} ({nchunks} chunk(s))")

    _upload(cluster_df,   "GA_GROCERY_HOTSPOT_CLUSTERS")
    _upload(narrative_df, "GA_GROCERY_NARRATIVE")

    conn.commit()


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print("NeighbourWise Grocery Access Analysis")
    print(f"Run started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    conn = get_conn()

    try:
        print("STEP 1 — Loading grocery data from MRT_BOSTON_GROCERY_STORES")
        df = load_grocery_data(conn)

        print("\nSTEP 2 — DBSCAN spatial clustering per neighborhood")
        cluster_df = dbscan_grocery_analysis(df)

        print("\nSTEP 3 — Generating Cortex food access narratives")
        narrative_df = generate_cortex_narratives(conn, cluster_df, df)

        print("\nSTEP 4 — Writing results to Snowflake GROCERY_ANALYSIS")
        write_results(conn, cluster_df, narrative_df)

        print(f"\n{'='*60}")
        print("SAMPLE OUTPUT — Food Access Narratives")
        print("="*60)
        for _, r in narrative_df.head(5).iterrows():
            print(f"\n{r['NEIGHBORHOOD_NAME']} ({r['ACCESS_TIER']})")
            print(f"  Essential stores: {r['ESSENTIAL_STORE_COUNT']} / {r['TOTAL_STORES']} total")
            print(f"  Narrative: {r['FOOD_ACCESS_NARRATIVE']}")

        print(f"\n{'='*60}")
        print("Run completed successfully.")
        print("New tables in GROCERY_ANALYSIS schema:")
        print("  GA_GROCERY_HOTSPOT_CLUSTERS")
        print("  GA_GROCERY_NARRATIVE")

    finally:
        conn.close()


if __name__ == "__main__":
    main()