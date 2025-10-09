{{ config(
    materialized='table',
    schema='analytics',
    alias='fct_sales_volume'
) }}

WITH sales_data AS (
    SELECT
        f.unique_row_id,
        f.sale_price,
        f.settlement_date_id,
        
        -- Date dimensions
        d.full_date,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        CONCAT(CAST(d.year AS STRING), '-Q', CAST(d.quarter AS STRING)) AS year_quarter,
        CONCAT(CAST(d.year AS STRING), '-', LPAD(CAST(d.month AS STRING), 2, '0')) AS year_month,

        -- first day of year & month for YoY comparisons
        DATE(d.year, 1, 1) AS year_start_date,
        DATE(d.year, d.month, 1) AS month_start_date,
        
        -- Location dimensions
        l.suburb,
        l.postcode,
        l.lga_code AS lga,
        l.gccsa_name AS region,
        
        -- Property dimensions
        pt.property_type,
        pt.property_category
        
    FROM {{ ref("fact_sales") }} f
    LEFT JOIN {{ ref("dim_dates") }} d
        ON f.settlement_date_id = d.date_id
    LEFT JOIN {{ ref("dim_location_cleaned") }} l
        ON TRIM(CAST(f.location_id AS STRING)) = TRIM(CAST(l.location_id AS STRING))
    LEFT JOIN {{ ref("dim_property_type") }} pt
        ON f.property_type_sk = pt.property_type_sk
    WHERE f.sale_price IS NOT NULL
        AND f.sale_price BETWEEN 100000 AND 20000000
        AND d.year IS NOT NULL
        AND d.year > 2000
)

SELECT
    -- Time dimensions
    year,
    quarter,
    month,
    month_name,
    year_quarter,
    year_month,
    year_start_date,
    month_start_date,
    
    -- Location dimensions
    region,
    lga,
    suburb,
    postcode,
    
    -- Property dimensions
    property_type,
    property_category,
    
    -- Volume metrics
    COUNT(DISTINCT unique_row_id) AS total_sales,
    
    -- Price metrics (useful context)
    approx_quantiles(sale_price, 100)[OFFSET(50)] AS median_price,
    AVG(sale_price) AS avg_price,
    MIN(sale_price) AS min_price,
    MAX(sale_price) AS max_price,
    
    -- Total value
    SUM(sale_price) AS total_value

FROM sales_data
GROUP BY
    year,
    quarter,
    month,
    month_name,
    year_quarter,
    year_month,
    year_start_date,
    month_start_date,
    region,
    lga,
    suburb,
    postcode,
    property_type,
    property_category

ORDER BY year DESC, quarter DESC, month DESC