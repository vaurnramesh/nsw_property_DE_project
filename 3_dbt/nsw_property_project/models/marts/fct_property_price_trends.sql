{{ config(
    materialized='table',
    schema='analytics',
    alias='fct_property_price_trends'
) }}

WITH base AS (
    SELECT
        f.sale_price,
        f.contract_date_id,
        pt.property_type,
        l.suburb,
        l.gccsa_name AS region_category
    FROM {{ ref("fact_sales") }} f
    LEFT JOIN {{ ref("dim_property_type") }} pt
        ON f.property_type_sk = pt.property_type_sk
    LEFT JOIN {{ ref("dim_location_cleaned") }} l
        ON f.location_id = l.location_id
    WHERE f.sale_price IS NOT NULL
        AND f.contract_date_id IS NOT NULL
        AND l.gccsa_name IN ('Greater Sydney', 'Rest of NSW')
),

date_parsed AS (
    SELECT
        sale_price,
        PARSE_DATE('%Y%m%d', CAST(contract_date_id AS STRING)) AS contract_date,
        property_type,
        suburb,
        region_category
    FROM base
    WHERE contract_date_id IS NOT NULL
),

yearly AS (
    SELECT
        EXTRACT(YEAR FROM contract_date) AS sale_year,
        region_category,
        property_type,
        APPROX_QUANTILES(sale_price, 100)[OFFSET(50)] AS median_sale_price
    FROM date_parsed
    GROUP BY sale_year, region_category, property_type
    HAVING sale_year >= 2001
),

SELECT
    sale_year,
    region_category,
    property_type,
    sale_price_distribution[OFFSET(50)] AS median_sale_price
FROM quantiles
WHERE sale_year >= 2001
ORDER BY sale_year, sale_quarter, region_category, property_type
