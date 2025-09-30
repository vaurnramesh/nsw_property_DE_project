{{ config(
    materialized='table',
    schema='analytics',
    alias='fct_suburb_price_trends_2'
) }}

WITH base AS (
    SELECT
        d.year,
        pt.property_type,
        l.suburb,
        l.gccsa_name AS region_category,
        approx_quantiles(f.sale_price, 100)[OFFSET(50)] AS median_price,
        count(*) AS sales_count
    FROM {{ ref("fact_sales") }} f
    LEFT JOIN {{ ref("dim_property_type") }} pt
        ON f.property_type_sk = pt.property_type_sk
    LEFT JOIN {{ ref("dim_location_cleaned") }} l
        ON f.location_id = l.location_id
    LEFT JOIN {{ ref("dim_dates")}} d
        ON  f.settlement_date_id = d.date_id
    WHERE f.sale_price IS NOT NULL
        AND f.sale_price BETWEEN 100000 AND 20000000
        AND d.year IS NOT NULL
        AND d.year > 2000
        AND l.gccsa_name IN ('Greater Sydney', 'Rest of NSW')
        GROUP BY l.suburb, pt.property_type, l.gccsa_name, d.year
),

growth AS (
    SELECT
        suburb,
        property_type,
        region_category,
        year,
        median_price,
        sales_count,
        LAG(median_price) OVER (
            PARTITION BY suburb, property_type, region_category
            ORDER BY year
        ) AS prev_median_price
    FROM base
),


yoy AS (SELECT
    suburb,
    property_type,
    region_category,
    year,
    median_price,
    sales_count,
    CASE 
      WHEN prev_median_price IS NOT NULL 
           AND sales_count >= 5
      THEN SAFE_DIVIDE(median_price - prev_median_price, prev_median_price) * 100
      ELSE NULL
    END AS yoy_median_growth_pct
FROM growth
),

filtered_years AS (
    SELECT *
    FROM yoy
    WHERE sales_count >= 5
),


final_growth AS (
    SELECT DISTINCT
        suburb,
        property_type,
        region_category,
        FIRST_VALUE(year) OVER (
            PARTITION BY suburb, property_type, region_category
            ORDER BY year ASC
        ) AS start_year,
        FIRST_VALUE(year) OVER (
            PARTITION BY suburb, property_type, region_category
            ORDER BY year DESC
        ) AS end_year,
        FIRST_VALUE(median_price) OVER (
            PARTITION BY suburb, property_type, region_category
            ORDER BY year ASC
        ) AS start_price,
        FIRST_VALUE(median_price) OVER (
            PARTITION BY suburb, property_type, region_category
            ORDER BY year DESC
        ) AS end_price
    FROM filtered_years
)

SELECT
    y.suburb,
    y.property_type,
    y.region_category,
    y.year,
    y.median_price,
    y.sales_count,
    y.yoy_median_growth_pct,
    f.start_year,
    f.end_year,
    f.start_price,
    f.end_price,
    SAFE_DIVIDE(f.end_price - f.start_price, f.start_price) * 100 AS total_growth_pct
FROM yoy y
JOIN final_growth f
  USING (suburb, property_type, region_category)
ORDER BY suburb, property_type, region_category, year
