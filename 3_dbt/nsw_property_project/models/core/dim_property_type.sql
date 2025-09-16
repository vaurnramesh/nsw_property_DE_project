{{ config(materialized='table') }}

with src as (
    SELECT
      property_type,
      NULLIF(UPPER(TRIM(property_type_flag)), '') AS property_type_flag,
      CASE
            WHEN UPPER(TRIM(property_category)) = 'R' then 'Residential'
            WHEN UPPER(TRIM(property_category)) = 'V' then 'Vacant'
            ELSE 'Other'
        END AS property_category_name
    from {{ ref('stg_google_nswprop_data_2001_2024') }}
),

-- cannonicalise type and category, trim/ upper for consistency
canon as (
    SELECT DISTINCT
        CASE
            when regexp_contains(upper(trim(property_type)), r'TOWN\s?HOUSE') then 'TOWN HOUSE'
            WHEN REGEXP_CONTAINS(UPPER(TRIM(property_type)), r'HOUSE') then 'HOUSE'
            when regexp_contains(upper(trim(property_type)), r'UNIT/APARTMENT') then 'UNIT/APARTMENT'
            when regexp_contains(upper(trim(property_type)), r'VACANT LAND') then 'VACANT LAND'
            else UPPER(TRIM(property_type))
        END AS property_type_name,
        property_type_flag,
        property_category_name
    FROM src
)

SELECT 
    TO_HEX(MD5(CONCAT(
        UPPER(TRIM(property_type_name)), '|',
        UPPER(TRIM(property_category_name)), '|',
        COALESCE(property_type_flag, '')
    ))) as property_type_sk,

    property_type_name as property_type,
    property_category_name as property_category,
    property_type_flag,

    -- Booleans for slicing
    CASE 
        WHEN property_category_name = 'Residential' THEN true
        WHEN property_type_name in ('HOUSE', 'UNIT/APARTMENT', 'TOWN HOUSE') THEN true
        ELSE false 
    END AS is_residential,

    CASE
        WHEN property_category_name = 'VACANT' OR property_type_name = 'VACANT LAND' THEN true
        ELSE false 
    END AS is_vacant_land,

    current_timestamp() as created_at

FROM canon