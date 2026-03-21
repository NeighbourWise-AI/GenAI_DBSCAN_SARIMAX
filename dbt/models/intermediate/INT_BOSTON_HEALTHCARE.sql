{{
    config(
        materialized='table'
    )
}}
WITH source AS (
    SELECT *
    FROM {{ source('healthcare_stage', 'STG_BOSTON_HEALTHCARE') }}
),

/* ------------------------------------------------------------
   Keep only successfully geocoded healthcare facilities
------------------------------------------------------------- */
geocoded AS (
    SELECT
        DPH_FACILITY_ID,
        LAT,
        LONG
    FROM {{ source('healthcare_stage', 'STG_BOSTON_HEALTHCARE_GEOCODED') }}
    WHERE GEOCODE_STATUS = 'GEOCODED'
),

/* ------------------------------------------------------------
   Clean + standardize + enrich
------------------------------------------------------------- */
cleaned AS (
    SELECT
        /* ---------- business key ---------- */
        s.DPH_FACILITY_ID AS facility_id,

        /* ---------- facility name ---------- */
        COALESCE(
            NULLIF(REGEXP_REPLACE(TRIM(UPPER(s.FACILITY_NAME)), '\\s+', ' '), ''),
            'UNKNOWN'
        ) AS facility_name,

        /* ---------- facility type ---------- */
        COALESCE(
            NULLIF(TRIM(UPPER(s.FACILITY_TYPE)), ''),
            'UNKNOWN'
        ) AS facility_type,

        CASE
            WHEN TRIM(s.FACILITY_TYPE) ILIKE 'Ambulatory Surgical Ctr.%'         THEN 'AMBULATORY SURGICAL CENTER'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE 'Outpt.%Phys.%Ther%/Speech%Path.%'  THEN 'OUTPATIENT PT / SPEECH PATHOLOGY'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE 'Renal Dialysis (ESRD)%'            THEN 'RENAL DIALYSIS (ESRD)'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE 'Temp.%Nursing Agency%'             THEN 'TEMPORARY NURSING AGENCY'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE 'Mob/Port Hospital Satellite%'      THEN 'MOBILE / PORTABLE HOSPITAL SATELLITE'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE 'Mobile/Portable Clinic Satellite%' THEN 'MOBILE / PORTABLE CLINIC SATELLITE'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE 'Mobile/Portable Clinic%'           THEN 'MOBILE / PORTABLE CLINIC'
            WHEN NULLIF(TRIM(s.FACILITY_TYPE), '') IS NULL                       THEN 'UNKNOWN'
            ELSE UPPER(TRIM(s.FACILITY_TYPE))
        END AS facility_type_canonical,

        CASE
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Hospital%'
              OR TRIM(s.FACILITY_TYPE) ILIKE '%Inpatient%'
                THEN 'INPATIENT / HOSPITAL'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Clinic%'
              OR TRIM(s.FACILITY_TYPE) ILIKE '%Ambulatory%'
                THEN 'OUTPATIENT / CLINIC'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Home Health%'
                THEN 'HOME HEALTH'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Hospice%'
                THEN 'HOSPICE'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Nursing Home%'
              OR TRIM(s.FACILITY_TYPE) ILIKE '%Rest Home%'
                THEN 'LONG-TERM CARE'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Adult Day Health%'
                THEN 'ADULT DAY HEALTH'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Renal Dialysis%'
                THEN 'DIALYSIS'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Agency%'
                THEN 'AGENCY / STAFFING'
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Research%'
                THEN 'RESEARCH'
            ELSE 'OTHER'
        END AS facility_type_group,

        /* ---------- address ---------- */
        COALESCE(
            NULLIF(REGEXP_REPLACE(TRIM(UPPER(s.STREET)), '\\s+', ' '), ''),
            'UNKNOWN'
        ) AS street,

        COALESCE(
            NULLIF(REGEXP_REPLACE(TRIM(UPPER(s.CITY_TOWN)), '\\s+', ' '), ''),
            'UNKNOWN'
        ) AS city,

        COALESCE(
            NULLIF(
                LPAD(
                    REGEXP_REPLACE(COALESCE(CAST(s.ZIP_CODE AS VARCHAR), ''), '[^0-9]', ''),
                    5,
                    '0'
                ),
                ''
            ),
            'UNKNOWN'
        ) AS zip_code,

        /* ---------- phone ---------- */
        COALESCE(
            NULLIF(REGEXP_REPLACE(TRIM(COALESCE(s.TELEPHONE, '')), '[^0-9]', ''), ''),
            'UNKNOWN'
        ) AS phone,

        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(s.TELEPHONE, ''), '[^0-9]', '')) >= 10 THEN TRUE
            ELSE FALSE
        END AS has_valid_phone,

        /* ---------- capacity / counts ---------- */
        COALESCE(
            CASE
                WHEN s.BED_COUNT IS NULL THEN NULL
                WHEN UPPER(TRIM(s.BED_COUNT)) IN ('NOT APPLICABLE', 'N/A', '') THEN NULL
                ELSE TRY_TO_NUMBER(s.BED_COUNT)
            END,
            -99
        ) AS bed_count,

        COALESCE(
            CASE
                WHEN s.ADULT_DAY_HEALTH_CAPACITY IS NULL THEN NULL
                WHEN UPPER(TRIM(s.ADULT_DAY_HEALTH_CAPACITY)) IN ('NOT APPLICABLE', 'N/A', '') THEN NULL
                ELSE TRY_TO_NUMBER(s.ADULT_DAY_HEALTH_CAPACITY)
            END,
            -99
        ) AS adult_day_health_capacity,

        /* ---------- simple Boston-area flag ---------- */
        CASE
            WHEN UPPER(TRIM(COALESCE(s.CITY_TOWN, ''))) IN (
                'BOSTON','CAMBRIDGE','SOMERVILLE','BROOKLINE','NEWTON','ARLINGTON',
                'MEDFORD','MALDEN','QUINCY','REVERE','CHELSEA','EVERETT','WATERTOWN',
                'BELMONT','WALTHAM','MILTON','DEDHAM',
                'ALLSTON','BRIGHTON','CHARLESTOWN','DORCHESTER','EAST BOSTON',
                'HYDE PARK','JAMAICA PLAIN','MATTAPAN','MISSION HILL','NORTH END',
                'ROSLINDALE','ROXBURY','SOUTH BOSTON','SOUTH END','WEST ROXBURY',
                'FENWAY','BACK BAY','BEACON HILL'
            ) THEN TRUE
            ELSE FALSE
        END AS is_boston_area,

        /* ---------- derived type flags ---------- */
        CASE
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Hospital%'
              OR TRIM(s.FACILITY_TYPE) ILIKE '%Inpatient%'
            THEN TRUE ELSE FALSE
        END AS is_hospital,

        CASE
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Clinic%'
              OR TRIM(s.FACILITY_TYPE) ILIKE '%Ambulatory%'
            THEN TRUE ELSE FALSE
        END AS is_clinic,

        CASE
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Adult Day Health%'
            THEN TRUE ELSE FALSE
        END AS is_adult_day_health,

        CASE
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Renal Dialysis%'
            THEN TRUE ELSE FALSE
        END AS is_dialysis,

        CASE
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Hospice%'
            THEN TRUE ELSE FALSE
        END AS is_hospice,

        CASE
            WHEN TRIM(s.FACILITY_TYPE) ILIKE '%Home Health%'
            THEN TRUE ELSE FALSE
        END AS is_home_health,

        /* ---------- geocoded coordinates ---------- */
        COALESCE(g.LAT, -999.0)  AS lat,
        COALESCE(g.LONG, -999.0) AS long,

        CASE
            WHEN g.LAT IS NOT NULL AND g.LONG IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_valid_location,

        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp

    FROM source s
    INNER JOIN geocoded g
        ON s.DPH_FACILITY_ID = g.DPH_FACILITY_ID
)

SELECT
facility_id,
facility_name,
facility_type,
facility_type_group,
street,
city,
zip_code,
phone,
has_valid_phone,
bed_count,
adult_day_health_capacity,
is_boston_area,
is_hospital,
is_clinic,
lat,
long,
has_valid_location,
load_timestamp
FROM cleaned
WHERE facility_id IS NOT NULL
