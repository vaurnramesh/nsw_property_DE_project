/**
Following operations are generally performed in the staging layer 

- Safe Type casting
- Generate surrgate keys: unique_row_id is already handled while ingestion in airflow
- Deduplication: Covered during ingestion by airflow with the help of temp tables
- Field conversions
- Macros if needed
**/

{{ config(materialized='view') }}

with raw as (
    SELECT *
    FROM {{ source('staging', 'final_table') }}
),

dedup_exact as (
    SELECT
        property_id,
        section_no,
        street_no,
        street_name,
        locality,
        contract_date,
        settlement_date,
        sale_price,

        -- choose one representative row
        max(processed_datetime) as processed_datetime,   -- keep latest snapshot
        any_value(unique_row_id) as sample_row_id,
        any_value(building_name) as building_name,
        any_value(land_area_sqm) as land_area_sqm,
        any_value(area_type) as area_type,
        any_value(zoning) as zoning,
        any_value(property_category) as property_category,
        any_value(property_description) as property_description,
        any_value(lga_code) as lga_code,
        any_value(postcode) as postcode
    FROM raw
    GROUP BY
        property_id,
        section_no,
        street_no,
        street_name,
        locality,
        contract_date,
        settlement_date,
        sale_price
),


-- Tier 2: Resolve near-duplicate (choose latest snapsot by processed_datetime)
deduped as (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY property_id, section_no, street_no, locality, contract_date, settlement_date
            ORDER BY processed_datetime DESC
        ) as rn
    FROM dedup_exact
)

select
    -- identifiers
    sample_row_id as unique_row_id,
    {{ dbt.safe_cast("lga_code", api.Column.translate_type("string")) }} as lga_code,  -- now present
    {{ dbt.safe_cast("property_id", api.Column.translate_type("string")) }} as property_id,

    -- timestamps
    cast(processed_datetime as timestamp) as processed_datetime,
    cast(contract_date as date) as contract_date,
    cast(settlement_date as date) as settlement_date,

    -- address fields
    nullif(trim(building_name), '') as building_name,
    nullif(trim(section_no), '') as unit_number,
    nullif(trim(street_no), '') as street_number,
    upper(trim(street_name)) as street_name,
    upper(trim(locality)) as suburb,
    cast(postcode as string) as postcode,

    -- land + zoning
    {{ dbt.safe_cast("land_area_sqm", api.Column.translate_type("integer")) }} as land_area_sqm,
    upper(trim(area_type)) as area_type,
    upper(trim(zoning)) as zoning,

    -- transaction
    {{ dbt.safe_cast("sale_price", api.Column.translate_type("integer")) }} as sale_price,
    upper(trim(property_category)) as property_category,
    upper(trim(property_description)) as property_description,

    -- Property type classification using macro
    {{ classify_property_type('section_no', 'street_no', 'property_category') }} as property_type

from deduped
where rn = 1