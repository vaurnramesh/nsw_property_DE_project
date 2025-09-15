{{ config(materialized='incremental', unique_key='property_sk') }}

with source AS (
    SELECT *
    FROM {{ ref ('stg_google_nswprop_data_2001_2024') }}
),

-- generate surrogate key
with_hash AS (
    SELECT
        *,
        TO_HEX(MD5(
            CONCAT(
                COALESCE(UPPER(TRIM(property_id)), ''),
                '|', COALESCE(UPPER(TRIM(unit_number)), ''),
                '|', COALESCE(UPPER(TRIM(street_number)), ''),
                '|', COALESCE(UPPER(TRIM(street_name)), ''),
                '|', COALESCE(UPPER(TRIM(suburb)), ''),
                '|', COALESCE(UPPER(TRIM(postcode)), ''),
                '|', COALESCE(UPPER(TRIM(lga_code)), '')
            )
        )) AS property_sk
    FROM source
),

-- pick the latest snapshot
deduped AS (
    SELECT *
    FROM (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY property_sk
            ORDER BY processed_datetime DESC NULLS LAST
        ) AS rn
        FROM with_hash
    )
    WHERE rn = 1
)

-- Only latest (SCD 1 overwrite) 
SELECT
    property_sk,                                -- surrogate key for star joins
    property_id AS property_source_id,          -- original ID from staging
    building_name,
    unit_number,
    street_number,
    street_name,
    suburb,
    postcode,
    land_area_sqm,
    zoning,
    property_description,
    current_timestamp() AS created_at,
    processed_datetime AS last_seen             -- tells you when this record was last updated in staging (audit)
FROM deduped
WHERE street_name IS NOT NULL
AND suburb IS NOT NULL
AND postcode IS NOT NULL