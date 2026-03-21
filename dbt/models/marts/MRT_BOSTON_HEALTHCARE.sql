{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH int_healthcare AS (
    SELECT *
    FROM {{ ref('int_boston_healthcare') }}
),

/* ------------------------------------------------------------
   Master location tables
------------------------------------------------------------- */
master_location_spatial AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        geometry
    FROM MARTS.MASTER_LOCATION
    WHERE has_geometry = TRUE
),

master_location_city AS (
    SELECT
        location_id,
        neighborhood_name,
        city
    FROM MARTS.MASTER_LOCATION
    WHERE UPPER(granularity) = 'CITY'
),

/* ------------------------------------------------------------
   First try spatial join
------------------------------------------------------------- */
with_spatial AS (
    SELECT
        h.*,
        ml.location_id AS spatial_location_id,
        ml.neighborhood_name AS spatial_neighborhood_name,
        ml.city AS spatial_city
    FROM int_healthcare h
    LEFT JOIN master_location_spatial ml
        ON h.has_valid_location = TRUE
        AND ST_CONTAINS(
            ml.geometry,
            ST_MAKEPOINT(
                CASE WHEN h.has_valid_location = TRUE THEN h.long ELSE 0 END,
                CASE WHEN h.has_valid_location = TRUE THEN h.lat ELSE 0 END
            )
        )
),

/* ------------------------------------------------------------
   If spatial fails, try city-level fallback from MASTER_LOCATION
------------------------------------------------------------- */
with_city_fallback AS (
    SELECT
        s.*,
        mc.location_id AS city_location_id,
        mc.neighborhood_name AS city_neighborhood_name,
        mc.city AS city_match_name
    FROM with_spatial s
    LEFT JOIN master_location_city mc
        ON UPPER(TRIM(s.city)) = UPPER(TRIM(mc.city))
),

/* ------------------------------------------------------------
   Final resolved location columns
------------------------------------------------------------- */
with_resolved_location AS (
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

        COALESCE(spatial_location_id, city_location_id) AS location_id,

        COALESCE(
            spatial_neighborhood_name,
            city_neighborhood_name,
            city
        ) AS neighborhood_name,

        COALESCE(
            spatial_city,
            city_match_name,
            city
        ) AS city_name,

        CASE
            WHEN spatial_location_id IS NOT NULL THEN 'SPATIAL'
            WHEN city_location_id IS NOT NULL THEN 'CITY_MASTER'
            ELSE 'CITY'
        END AS resolution_method

    FROM with_city_fallback
),

with_description AS (
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
        location_id,
        neighborhood_name,
        city_name,
        resolution_method,

        facility_name || ' is a ' ||
        LOWER(COALESCE(facility_type, 'healthcare facility')) ||
        ' located in ' ||
        COALESCE(neighborhood_name, 'an unknown neighborhood') || ', ' ||
        COALESCE(city_name, city, 'Boston') || '.' ||

        CASE
            WHEN facility_type_group IS NOT NULL
                THEN ' It belongs to the ' || LOWER(facility_type_group) || ' category.'
            ELSE ''
        END ||

        CASE
            WHEN bed_count IS NOT NULL AND bed_count <> -99
                THEN ' It has a bed count of ' || bed_count || '.'
            ELSE ''
        END ||

        CASE
            WHEN adult_day_health_capacity IS NOT NULL AND adult_day_health_capacity <> -99
                THEN ' It has an adult day health capacity of ' || adult_day_health_capacity || '.'
            ELSE ''
        END ||

        CASE WHEN is_hospital THEN ' It is classified as a hospital.' ELSE '' END ||
        CASE WHEN is_clinic THEN ' It is classified as a clinic.' ELSE '' END ||
        CASE WHEN has_valid_phone THEN ' A valid phone number is available for this facility.' ELSE '' END ||
        CASE WHEN is_boston_area THEN ' It is located within the Boston area.' ELSE '' END
        AS row_description,

        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS mart_load_timestamp

    FROM with_resolved_location
)

SELECT *
FROM with_description
WHERE facility_id IS NOT NULL
