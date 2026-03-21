{{
    config(
        materialized = 'table',
        schema       = 'INTERMEDIATE',
        tags         = ['grocery', 'intermediate']
    )
}}

/*
================================================================================
INT_BOSTON_GROCERY_STORES
--------------------------------------------------------------------------------
Sources: STAGE.STG_BOSTON_GROCERY_STORES
         STAGE.STG_BOSTON_GROCERY_GEOCODE  (lat/long via Nominatim)
Layer  : Intermediate
--------------------------------------------------------------------------------
Transformations applied:
  1. Data type casting         — objectid → INTEGER, year → INTEGER, zip cleaned
  2. NULL / empty handling     — sentinels: 'UNKNOWN' for strings, -99 for ints
  3. Standardization           — UPPER(), TRIM(), store_type mapped to clean labels (raw not exposed)
  4. Derived columns           — store_type, naics_category, is_essential,
                                  is_pharmacy, is_specialty, data_vintage,
                                  zip_valid, naics_major_group
  5. Deduplication             — on (coname, staddr, stcity, zip, year)
                                  keeps row with lowest objectid
  6. Geocode join              — INNER JOIN on OBJECTID to STG_BOSTON_GROCERY_GEOCODE
                                  only GEOCODED rows retained (FAILED excluded)
  7. Shape column dropped      — proprietary Esri binary, not decodable
================================================================================
*/

with

source as (
    select * from {{ source('STAGE', 'STG_BOSTON_GROCERY_STORES') }}
),

-- Only bring in rows that were successfully geocoded within Greater Boston bbox
geocoded as (
    select
        objectid,
        lat,
        long
    from {{ source('STAGE', 'STG_BOSTON_GROCERY_GEOCODED') }}
    where geocode_status = 'GEOCODED'
),

-- ─────────────────────────────────────────────
-- STEP 1: Cast, clean, and sentinel-fill
-- INNER JOIN geocoded: only stores with valid coordinates are retained
-- ─────────────────────────────────────────────
casted as (
    select
        -- Primary key
        try_cast(s.objectid as integer)                                 as objectid,

        -- Store identity
        upper(trim(s.coname))                                           as store_name,
        upper(trim(s.staddr))                                           as street_address,
        upper(trim(s.stcity))                                           as city,

        -- ZIP: strip whitespace, validate 5-digit format
        case
            when regexp_like(trim(s.zip), '^[0-9]{5}$')
                and trim(s.zip) != '00000'
            then trim(s.zip)
            else 'UNKNOWN'
        end                                                             as zip_code,

        -- Store type — raw (kept for reference)
        upper(trim(s.store_type))                                       as store_type_src,

        -- Year as integer
        try_cast(s.year as integer)                                     as data_year,

        -- Geocoordinates from STG_BOSTON_GROCERY_GEOCODE
        coalesce(g.lat,  -999.0)                                        as lat,
        coalesce(g.long, -999.0)                                        as long,
        case
            when g.lat is not null then true
            else false
        end                                                             as has_valid_location

    from source s
    inner join geocoded g
        on try_cast(s.objectid as integer) = g.objectid
    where s.objectid is not null
),

-- ─────────────────────────────────────────────
-- STEP 2: Standardize store_type labels
-- (collapse duplicates and fix inconsistent naming)
-- ─────────────────────────────────────────────
standardized as (
    select
        *,
        case
            when store_type_src in (
                'SUPERMARKET OR OTHER GROCERY',
                'SUPERMARKETS/OTHER GROCERY (EXC CONVENIENCE) STRS'
            )                                               then 'SUPERMARKET'
            when store_type_src in (
                'CONVENIENCE STORES',
                'CONVENIENCE STORES, PHARMACIES, AND DRUG STORES'
            )                                               then 'CONVENIENCE_STORE'
            when store_type_src in (
                'MEAT MARKETS, FISH AND SEAFOOD MARKETS, AND ALL OTHER SPECIALTY FOOD STORES',
                'MEAT MARKETS'
            )                                               then 'MEAT_MARKET'
            when store_type_src in (
                'FISH & SEAFOOD MARKETS'
            )                                               then 'FISH_SEAFOOD_MARKET'
            when store_type_src in (
                'FRUIT AND VEGETABLE MARKETS',
                'FRUIT & VEGETABLE MARKETS'
            )                                               then 'FRUIT_VEG_MARKET'
            when store_type_src in (
                'ALL OTHER SPECIALTY FOOD STORES'
            )                                               then 'SPECIALTY_FOOD_STORE'
            when store_type_src in (
                'PHARMACIES & DRUG STORES'
            )                                               then 'PHARMACY'
            when store_type_src in (
                'WAREHOUSE CLUBS & SUPERCENTERS',
                'WAREHOUSE CLUBS AND SUPERCENTERS'
            )                                               then 'WAREHOUSE_CLUB'
            when store_type_src = 'FARMERS MARKETS'         then 'FARMERS_MARKET'
            when store_type_src = 'WINTER MARKETS'          then 'WINTER_MARKET'
            when store_type_src = 'DOLLAR STORE'            then 'DOLLAR_STORE'
            when store_type_src = 'DEPARTMENT STORES (EXCEPT DISCOUNT DEPT STORES)'
                                                            then 'DEPARTMENT_STORE'
            else 'OTHER'
        end                                                             as store_type,

        -- NAICS-style category derived from store_type for classification
        case
            when store_type = 'SUPERMARKET'           then 'Supermarkets & Grocery'
            when store_type = 'CONVENIENCE_STORE'     then 'Convenience Stores'
            when store_type = 'MEAT_MARKET'           then 'Meat Markets'
            when store_type = 'FISH_SEAFOOD_MARKET'   then 'Fish & Seafood Markets'
            when store_type = 'FRUIT_VEG_MARKET'      then 'Fruit & Vegetable Markets'
            when store_type = 'SPECIALTY_FOOD_STORE'  then 'Other Specialty Food'
            when store_type = 'PHARMACY'              then 'Pharmacies & Drug Stores'
            when store_type in ('WAREHOUSE_CLUB', 'DEPARTMENT_STORE') then 'Warehouse & Department Stores'
            when store_type = 'FARMERS_MARKET'        then 'Farmers Markets'
            when store_type = 'WINTER_MARKET'         then 'Winter Markets'
            when store_type = 'DOLLAR_STORE'          then 'Dollar Stores'
            else 'Other Retail'
        end                                                             as naics_category

    from casted
),

-- ─────────────────────────────────────────────
-- STEP 3: Deduplication
-- Keep lowest objectid per (store_name, street_address, city, zip_code, data_year)
-- ─────────────────────────────────────────────
deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by store_name, street_address, city, zip_code, data_year
                order by objectid asc
            ) as _row_num
        from standardized
    )
    where _row_num = 1
),

-- ─────────────────────────────────────────────
-- STEP 4: Derived / business logic columns
-- ─────────────────────────────────────────────
final as (
    select
        -- ── Keys ──────────────────────────────
        objectid,

        -- ── Store identity ────────────────────
        store_name,
        street_address,
        city,
        zip_code,
        -- ── Classification ────────────────────
        store_type,
        naics_category,

        -- ── Data vintage ──────────────────────
        data_year,
        case
            when data_year = 2021 then '2021 Survey'
            when data_year = 2017 then '2017 Survey'
            else 'UNKNOWN'
        end                                                             as data_vintage,

        -- ── Derived flags ─────────────────────

        -- Is this a primary grocery source (food security relevance)?
        case
            when store_type in (
                'SUPERMARKET', 'FRUIT_VEG_MARKET', 'MEAT_MARKET',
                'FISH_SEAFOOD_MARKET', 'WAREHOUSE_CLUB', 'FARMERS_MARKET',
                'WINTER_MARKET', 'SPECIALTY_FOOD_STORE'
            ) then true
            else false
        end                                                             as is_essential_food_source,

        -- Is this a pharmacy / drug store?
        case
            when store_type in ('PHARMACY', 'CONVENIENCE_STORE')
                or naics_category = 'Pharmacies & Drug Stores'
            then true else false
        end                                                             as is_pharmacy_or_drug_store,

        -- Is this a specialty / niche food store?
        case
            when store_type in (
                'FRUIT_VEG_MARKET', 'MEAT_MARKET', 'FISH_SEAFOOD_MARKET',
                'SPECIALTY_FOOD_STORE', 'FARMERS_MARKET', 'WINTER_MARKET'
            ) then true else false
        end                                                             as is_specialty_store,

        -- Is this a large-format store?
        case
            when store_type in ('SUPERMARKET', 'WAREHOUSE_CLUB', 'DEPARTMENT_STORE')
            then true else false
        end                                                             as is_large_format,

        -- Is this a convenience / quick-stop store?
        case
            when store_type in ('CONVENIENCE_STORE', 'DOLLAR_STORE')
            then true else false
        end                                                             as is_convenience_type,

        -- ZIP validity flag
        case
            when zip_code = 'UNKNOWN' then false else true
        end                                                             as is_zip_valid,

        -- ── Location coordinates ──────────────
        lat,
        long,
        has_valid_location,

        -- ── Metadata ──────────────────────────
        current_timestamp()                                             as dbt_loaded_at

    from deduped
)

select * from final
