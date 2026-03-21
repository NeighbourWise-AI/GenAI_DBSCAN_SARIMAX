##HEALTHCARE ACCESS PROFILE##

{{
    config(
        materialized='table',
        schema='healthcare_analysis',
        alias='HA_HEALTHCARE_ACCESS_PROFILE'
    )
}}

WITH healthcare_agg AS (

    SELECT
        ml.location_id,
        ml.neighborhood_name,
        ml.city,
        ml.sqmiles,

        COUNT(h.facility_id) AS total_facilities,

        SUM(CASE WHEN h.is_hospital THEN 1 ELSE 0 END) AS hospital_count,
        SUM(CASE WHEN h.is_clinic THEN 1 ELSE 0 END) AS clinic_count,
        SUM(CASE WHEN h.has_valid_phone THEN 1 ELSE 0 END) AS valid_phone_count,

        SUM(CASE WHEN h.facility_type_group = 'INPATIENT / HOSPITAL' THEN 1 ELSE 0 END) AS inpatient_hospital_count,
        SUM(CASE WHEN h.facility_type_group = 'OUTPATIENT / CLINIC' THEN 1 ELSE 0 END) AS outpatient_clinic_count,
        SUM(CASE WHEN h.facility_type_group = 'PUBLIC HEALTH / COMMUNITY' THEN 1 ELSE 0 END) AS public_health_count,
        SUM(CASE WHEN h.facility_type_group = 'SPECIALTY / OTHER' THEN 1 ELSE 0 END) AS specialty_other_count,

        COUNT(DISTINCT h.facility_type_group) AS facility_type_diversity

    FROM {{ source('marts', 'MASTER_LOCATION') }} ml
    LEFT JOIN {{ source('marts', 'MRT_BOSTON_HEALTHCARE') }} h
        ON h.location_id = ml.location_id

    GROUP BY
        ml.location_id,
        ml.neighborhood_name,
        ml.city,
        ml.sqmiles
),

scored AS (

    SELECT
        location_id,
        neighborhood_name,
        city,
        sqmiles,

        COALESCE(total_facilities, 0) AS total_facilities,
        COALESCE(hospital_count, 0) AS hospital_count,
        COALESCE(clinic_count, 0) AS clinic_count,
        COALESCE(valid_phone_count, 0) AS valid_phone_count,
        COALESCE(inpatient_hospital_count, 0) AS inpatient_hospital_count,
        COALESCE(outpatient_clinic_count, 0) AS outpatient_clinic_count,
        COALESCE(public_health_count, 0) AS public_health_count,
        COALESCE(specialty_other_count, 0) AS specialty_other_count,
        COALESCE(facility_type_diversity, 0) AS facility_type_diversity,

        CASE
            WHEN sqmiles > 0 THEN ROUND(COALESCE(total_facilities, 0) / sqmiles, 2)
            ELSE 0
        END AS facilities_per_sqmile,

        CASE
            WHEN COALESCE(total_facilities, 0) > 0
            THEN ROUND((COALESCE(hospital_count, 0) + COALESCE(clinic_count, 0)) * 100.0 / total_facilities, 1)
            ELSE 0
        END AS pct_core_care,

        CASE
            WHEN COALESCE(total_facilities, 0) > 0
            THEN ROUND(COALESCE(valid_phone_count, 0) * 100.0 / total_facilities, 1)
            ELSE 0
        END AS pct_valid_phone,

        CASE
            WHEN COALESCE(total_facilities, 0) > 0
            THEN ROUND(COALESCE(inpatient_hospital_count, 0) * 100.0 / total_facilities, 1)
            ELSE 0
        END AS pct_inpatient_hospital,

        CASE
            WHEN COALESCE(total_facilities, 0) > 0
            THEN ROUND(COALESCE(outpatient_clinic_count, 0) * 100.0 / total_facilities, 1)
            ELSE 0
        END AS pct_outpatient_clinic,

        CASE
            WHEN COALESCE(total_facilities, 0) > 0
            THEN ROUND(COALESCE(public_health_count, 0) * 100.0 / total_facilities, 1)
            ELSE 0
        END AS pct_public_health,

        CASE
            WHEN COALESCE(total_facilities, 0) > 0
            THEN ROUND(COALESCE(specialty_other_count, 0) * 100.0 / total_facilities, 1)
            ELSE 0
        END AS pct_specialty_other,

        LEAST(
            CASE
                WHEN sqmiles > 0
                THEN ROUND((COALESCE(total_facilities, 0) / sqmiles) / 12.0 * 35, 1)
                ELSE 0
            END,
            35
        ) AS density_score,

        CASE
            WHEN COALESCE(total_facilities, 0) > 0
            THEN ROUND((COALESCE(hospital_count, 0) + COALESCE(clinic_count, 0)) * 1.0 / total_facilities * 30, 1)
            ELSE 0
        END AS core_care_score,

        CASE
            WHEN COALESCE(total_facilities, 0) > 0
            THEN ROUND(COALESCE(valid_phone_count, 0) * 1.0 / total_facilities * 20, 1)
            ELSE 0
        END AS contact_quality_score,

        LEAST(ROUND(COALESCE(facility_type_diversity, 0) / 4.0 * 15, 1), 15) AS diversity_score

    FROM healthcare_agg
)

SELECT
    location_id,
    neighborhood_name,
    city,
    sqmiles,
    total_facilities,
    hospital_count,
    clinic_count,
    valid_phone_count,
    inpatient_hospital_count,
    outpatient_clinic_count,
    public_health_count,
    specialty_other_count,
    facility_type_diversity,
    facilities_per_sqmile,
    pct_core_care,
    pct_valid_phone,
    pct_inpatient_hospital,
    pct_outpatient_clinic,
    pct_public_health,
    pct_specialty_other,
    density_score,
    core_care_score,
    contact_quality_score,
    diversity_score,
    LEAST(
        ROUND(
            density_score +
            core_care_score +
            contact_quality_score +
            diversity_score,
            1
        ),
        100
    ) AS healthcare_score,
    CASE
        WHEN LEAST(
            ROUND(
                density_score +
                core_care_score +
                contact_quality_score +
                diversity_score,
                1
            ),
            100
        ) >= 75 THEN 'EXCELLENT'
        WHEN LEAST(
            ROUND(
                density_score +
                core_care_score +
                contact_quality_score +
                diversity_score,
                1
            ),
            100
        ) >= 50 THEN 'GOOD'
        WHEN LEAST(
            ROUND(
                density_score +
                core_care_score +
                contact_quality_score +
                diversity_score,
                1
            ),
            100
        ) >= 25 THEN 'MODERATE'
        ELSE 'LIMITED'
    END AS healthcare_grade,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM scored

##HEALTHCARE HOTSPOT CLUSTER##

{{
    config(
        materialized='table',
        schema='healthcare_analysis',
        alias='HA_HEALTHCARE_HOTSPOT_CLUSTERS'
    )
}}

WITH base AS (

    SELECT
        location_id,
        neighborhood_name,
        city,
        facility_id,
        lat,
        long,

        /* SQL-only spatial binning proxy for local concentration */
        ROUND(lat, 2) AS lat_bin,
        ROUND(long, 2) AS long_bin

    FROM {{ source('marts', 'MRT_BOSTON_HEALTHCARE') }}
    WHERE location_id IS NOT NULL
      AND neighborhood_name IS NOT NULL
      AND lat IS NOT NULL
      AND long IS NOT NULL
),

cell_counts AS (

    SELECT
        location_id,
        neighborhood_name,
        city,
        lat_bin,
        long_bin,
        COUNT(*) AS facilities_in_bin
    FROM base
    GROUP BY
        location_id,
        neighborhood_name,
        city,
        lat_bin,
        long_bin
),

summary AS (

    SELECT
        location_id,
        neighborhood_name,
        city,
        COUNT(*) AS total_bins,
        SUM(facilities_in_bin) AS total_facilities,
        SUM(CASE WHEN facilities_in_bin >= 2 THEN 1 ELSE 0 END) AS n_healthcare_clusters,
        SUM(CASE WHEN facilities_in_bin >= 2 THEN facilities_in_bin ELSE 0 END) AS clustered_facilities,
        SUM(CASE WHEN facilities_in_bin = 1 THEN facilities_in_bin ELSE 0 END) AS isolated_facilities
    FROM cell_counts
    GROUP BY
        location_id,
        neighborhood_name,
        city
)

SELECT
    location_id,
    neighborhood_name,
    city,
    total_facilities,
    n_healthcare_clusters,
    CASE
        WHEN total_facilities > 0
        THEN ROUND(clustered_facilities * 100.0 / total_facilities, 1)
        ELSE 0
    END AS clustered_facility_share_pct,
    CASE
        WHEN total_facilities > 0
        THEN ROUND(isolated_facilities * 100.0 / total_facilities, 1)
        ELSE 0
    END AS isolated_facility_pct,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM summary


##HEALTHCARE NARRATIVE##

{{
    config(
        materialized='table',
        schema='healthcare_analysis',
        alias='HA_HEALTHCARE_NARRATIVE'
    )
}}

WITH profile AS (

    SELECT *
    FROM {{ ref('ha_healthcare_access_profile') }}

),

clusters AS (

    SELECT
        location_id,
        neighborhood_name,
        city,
        total_facilities,
        n_healthcare_clusters,
        clustered_facility_share_pct,
        isolated_facility_pct
    FROM {{ ref('ha_healthcare_hotspot_clusters') }}

),

merged AS (

    SELECT
        p.location_id,
        p.neighborhood_name,
        p.city,
        p.total_facilities,
        p.hospital_count,
        p.clinic_count,
        p.facility_type_diversity,
        p.facilities_per_sqmile,
        p.pct_core_care,
        p.pct_valid_phone,
        p.healthcare_score,
        p.healthcare_grade,
        COALESCE(c.n_healthcare_clusters, 0) AS n_healthcare_clusters,
        COALESCE(c.clustered_facility_share_pct, 0) AS clustered_facility_share_pct,
        COALESCE(c.isolated_facility_pct, 0) AS isolated_facility_pct
    FROM profile p
    LEFT JOIN clusters c
        ON p.location_id = c.location_id
)

SELECT
    location_id,
    neighborhood_name,
    city,
    total_facilities,
    hospital_count,
    clinic_count,
    facility_type_diversity,
    facilities_per_sqmile,
    pct_core_care,
    pct_valid_phone,
    n_healthcare_clusters,
    clustered_facility_share_pct,
    isolated_facility_pct,
    healthcare_score,
    healthcare_grade,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'You are a healthcare access analyst writing for a Boston/Cambridge area neighborhood guide. ' ||
        'Write a factual 2-3 sentence healthcare access summary for ' ||
        neighborhood_name || ', located in ' || city || ', Massachusetts. ' ||
        'It has ' || total_facilities || ' healthcare facilities, including ' ||
        hospital_count || ' hospitals and ' || clinic_count || ' clinics. ' ||
        'Healthcare score: ' || healthcare_score::VARCHAR || '/100 (' || healthcare_grade || '). ' ||
        'Facility density: ' || facilities_per_sqmile::VARCHAR || ' facilities per square mile. ' ||
        'Core care coverage: ' || pct_core_care::VARCHAR || '%. ' ||
        'Valid phone coverage: ' || pct_valid_phone::VARCHAR || '%. ' ||
        'Healthcare hotspots: ' || n_healthcare_clusters::VARCHAR || ' concentrated clusters, ' ||
        clustered_facility_share_pct::VARCHAR || '% clustered facilities, and ' ||
        isolated_facility_pct::VARCHAR || '% isolated facilities. ' ||
        'IMPORTANT: ' || neighborhood_name || ' is in Massachusetts, USA. Do not confuse it with any other place. ' ||
        'Be concise, factual, and do not invent data.'
    ) AS healthcare_narrative,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM merged

