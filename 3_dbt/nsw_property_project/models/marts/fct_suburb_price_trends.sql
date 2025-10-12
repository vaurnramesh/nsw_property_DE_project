{{ config(
    materialized='table',
    schema='analytics',
    alias='fct_suburb_price_trends'
) }}

-- @Unused Mart

WITH base AS (
    SELECT
        d.year,
        pt.property_type,
        l.suburb,
        l.gccsa_name AS region_category,
        f.sale_price  -- Keep individual prices for outlier detection
    FROM {{ ref("fact_sales") }} f
    LEFT JOIN {{ ref("dim_property_type") }} pt
        ON f.property_type_sk = pt.property_type_sk
    LEFT JOIN {{ ref("dim_location_cleaned") }} l
        ON f.location_id = l.location_id
    LEFT JOIN {{ ref("dim_dates")}} d
        ON f.settlement_date_id = d.date_id
    WHERE f.sale_price IS NOT NULL
        AND f.sale_price BETWEEN 100000 AND 20000000
        AND d.year IS NOT NULL
        AND d.year > 2000
        AND l.gccsa_name IN ('Greater Sydney', 'Rest of NSW')
),

-- Add outlier detection using IQR method
quartiles AS (
    SELECT
        suburb,
        property_type,
        region_category,
        year,
        approx_quantiles(sale_price, 100)[OFFSET(25)] AS q1,
        approx_quantiles(sale_price, 100)[OFFSET(75)] AS q3
    FROM base
    GROUP BY suburb, property_type, region_category, year
),

-- Remove outliers before calculating medians
clean_base AS (
    SELECT
        b.suburb,
        b.property_type,
        b.region_category,
        b.year,
        b.sale_price
    FROM base b
    INNER JOIN quartiles q
        ON b.suburb = q.suburb
        AND b.property_type = q.property_type
        AND b.region_category = q.region_category
        AND b.year = q.year
    WHERE b.sale_price >= q.q1 - 1.5 * (q.q3 - q.q1)
        AND b.sale_price <= q.q3 + 1.5 * (q.q3 - q.q1)
),

-- Calculate medians on cleaned data
aggregated AS (
    SELECT
        suburb,
        property_type,
        region_category,
        year,
        approx_quantiles(sale_price, 100)[OFFSET(50)] AS median_price,
        COUNT(*) AS sales_count
    FROM clean_base
    GROUP BY suburb, property_type, region_category, year
),

-- More stringent filtering - require minimum sales
filtered_years AS (
    SELECT *
    FROM aggregated
    WHERE sales_count >= 5
),

-- Only use filtered years for growth calculations
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
    FROM filtered_years
),

yoy AS (
    SELECT
        suburb,
        property_type,
        region_category,
        year,
        median_price,
        sales_count,
        CASE 
            WHEN prev_median_price IS NOT NULL 
            THEN SAFE_DIVIDE(median_price - prev_median_price, prev_median_price) * 100
            ELSE NULL
        END AS yoy_median_growth_pct
    FROM growth
),

-- Add data quality requirements for total growth calculations
suburb_quality AS (
    SELECT
        suburb,
        property_type,
        region_category,
        COUNT(DISTINCT year) AS years_with_data,
        SUM(sales_count) AS total_sales,
        MIN(year) AS min_year,
        MAX(year) AS max_year
    FROM filtered_years
    GROUP BY suburb, property_type, region_category
),

-- Only calculate total growth for suburbs with sufficient data
final_growth AS (
    SELECT DISTINCT
        fy.suburb,
        fy.property_type,
        fy.region_category,
        FIRST_VALUE(fy.year) OVER (
            PARTITION BY fy.suburb, fy.property_type, fy.region_category
            ORDER BY fy.year ASC
        ) AS start_year,
        FIRST_VALUE(fy.year) OVER (
            PARTITION BY fy.suburb, fy.property_type, fy.region_category
            ORDER BY fy.year DESC
        ) AS end_year,
        FIRST_VALUE(fy.median_price) OVER (
            PARTITION BY fy.suburb, fy.property_type, fy.region_category
            ORDER BY fy.year ASC
        ) AS start_price,
        FIRST_VALUE(fy.median_price) OVER (
            PARTITION BY fy.suburb, fy.property_type, fy.region_category
            ORDER BY fy.year DESC
        ) AS end_price
    FROM filtered_years fy
    INNER JOIN suburb_quality sq
        ON fy.suburb = sq.suburb
        AND fy.property_type = sq.property_type
        AND fy.region_category = sq.region_category
    WHERE sq.years_with_data >= 5  -- Require at least 5 years of data
        AND sq.total_sales >= 50    -- Require at least 50 total sales
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
    -- Cap extreme growth percentages for visualization
    CASE 
        WHEN f.start_price IS NOT NULL 
        THEN SAFE_DIVIDE(f.end_price - f.start_price, f.start_price) * 100
        ELSE NULL
    END AS total_growth_pct,
    -- Data quality flags
    sq.years_with_data,
    sq.total_sales,
    CASE 
        WHEN sq.years_with_data >= 5 AND sq.total_sales >= 50 
        THEN TRUE 
        ELSE FALSE 
    END AS is_reliable
FROM yoy y
LEFT JOIN final_growth f
    USING (suburb, property_type, region_category)
LEFT JOIN suburb_quality sq
    ON y.suburb = sq.suburb
    AND y.property_type = sq.property_type
    AND y.region_category = sq.region_category
ORDER BY suburb, property_type, region_category, year