{{ config(
    materialized='table',
    schema='analytics',
    alias='fct_suburb_price_periods'
) }}

WITH base AS (
    SELECT
        d.year,
        pt.property_type,
        l.suburb,
        l.gccsa_name AS region_category,
        f.sale_price
    FROM {{ ref("fact_sales") }} f
    LEFT JOIN {{ ref("dim_property_type") }} pt
        ON f.property_type_sk = pt.property_type_sk
    LEFT JOIN {{ ref("dim_location_cleaned") }} l
        ON f.location_id = l.location_id
    LEFT JOIN {{ ref("dim_dates") }} d
        ON f.settlement_date_id = d.date_id
    WHERE f.sale_price IS NOT NULL
        AND f.sale_price BETWEEN 100000 AND 20000000
        AND d.year IS NOT NULL
        AND d.year > 2000
        AND l.gccsa_name IN ('Greater Sydney', 'Rest of NSW')
),

-- Outlier detection using IQR method
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

-- Calculate yearly medians on cleaned data
yearly_metrics AS (
    SELECT
        suburb,
        property_type,
        region_category,
        year,
        approx_quantiles(sale_price, 100)[OFFSET(50)] AS median_price,
        COUNT(*) AS sales_count
    FROM clean_base
    GROUP BY suburb, property_type, region_category, year
    HAVING COUNT(*) >= 5  -- Minimum 5 sales per year
),

-- Get current year metrics (latest year only)
current_metrics AS (
    SELECT
        suburb,
        property_type,
        region_category,
        year AS current_year,
        median_price AS current_median_price,
        sales_count AS current_year_sales
    FROM yearly_metrics
    WHERE year = (SELECT MAX(year) FROM yearly_metrics)
),

-- Get historical prices for different time windows
price_windows AS (
    SELECT
        cm.suburb,
        cm.property_type,
        cm.region_category,
        cm.current_year,
        cm.current_median_price,
        cm.current_year_sales,
        
        -- 1 year ago
        y1.median_price AS price_1y_ago,
        y1.sales_count AS sales_1y_ago,
        
        -- 3 years ago
        y3.median_price AS price_3y_ago,
        y3.sales_count AS sales_3y_ago,
        
        -- 5 years ago
        y5.median_price AS price_5y_ago,
        y5.sales_count AS sales_5y_ago,

        -- 10 years ago
        y10.median_price AS price_10y_ago,
        y10.sales_count AS sales_10y_ago
        
    FROM current_metrics cm
    
    LEFT JOIN yearly_metrics y1
        ON cm.suburb = y1.suburb
        AND cm.property_type = y1.property_type
        AND cm.region_category = y1.region_category
        AND y1.year = cm.current_year - 1
    
    LEFT JOIN yearly_metrics y3
        ON cm.suburb = y3.suburb
        AND cm.property_type = y3.property_type
        AND cm.region_category = y3.region_category
        AND y3.year = cm.current_year - 2  -- Changed from -3 to -2 to match window
    
    LEFT JOIN yearly_metrics y5
        ON cm.suburb = y5.suburb
        AND cm.property_type = y5.property_type
        AND cm.region_category = y5.region_category
        AND y5.year = cm.current_year - 4  -- Changed from -5 to -4 to match window

    LEFT JOIN yearly_metrics y10
        ON cm.suburb = y10.suburb
        AND cm.property_type = y10.property_type
        AND cm.region_category = y10.region_category
        AND y10.year = cm.current_year - 9        
),

-- Calculate total sales in each window for data quality
window_sales_prep AS (
    SELECT
        suburb,
        property_type,
        region_category,
        year,
        sales_count,
        MAX(year) OVER (PARTITION BY suburb, property_type, region_category) AS max_year_for_group
    FROM yearly_metrics
),

window_sales AS (
    SELECT
        suburb,
        property_type,
        region_category,
        
        -- Get max year for this group
        MAX(max_year_for_group) AS max_year_in_group,
        
        -- Sales counts for different windows (1yr = current year only, 3yr = last 3 years, 5yr = last 5 years)
        SUM(CASE WHEN year >= max_year_for_group - 0 
            THEN sales_count ELSE 0 END) AS total_sales_1y,
        SUM(CASE WHEN year >= max_year_for_group - 2 
            THEN sales_count ELSE 0 END) AS total_sales_3y,
        SUM(CASE WHEN year >= max_year_for_group - 4 
            THEN sales_count ELSE 0 END) AS total_sales_5y,
        SUM(CASE WHEN year >= max_year_for_group - 9 
            THEN sales_count ELSE 0 END) AS total_sales_10y,
        
        -- Years with data
        COUNT(DISTINCT CASE WHEN year >= max_year_for_group - 2 
            THEN year END) AS years_in_3y_window,
        COUNT(DISTINCT CASE WHEN year >= max_year_for_group - 4 
            THEN year END) AS years_in_5y_window,
        COUNT(DISTINCT CASE WHEN year >= max_year_for_group - 9 
            THEN year END) AS years_in_10y_window,
        
        -- Overall data quality
        COUNT(DISTINCT year) AS total_years_with_data,
        SUM(sales_count) AS lifetime_sales
        
    FROM window_sales_prep
    GROUP BY suburb, property_type, region_category
),

-- Calculate growth rates with quality checks
growth_metrics AS (
    SELECT
        pw.*,
        ws.total_sales_1y,
        ws.total_sales_3y,
        ws.total_sales_5y,
        ws.total_sales_10y,
        ws.years_in_3y_window,
        ws.years_in_5y_window,
        ws.years_in_10y_window,
        ws.total_years_with_data,
        ws.lifetime_sales,
        
        -- 1-year growth
        CASE 
            WHEN pw.price_1y_ago IS NOT NULL 
                 AND ws.total_sales_1y >= 5
            THEN ROUND(SAFE_DIVIDE(pw.current_median_price - pw.price_1y_ago, pw.price_1y_ago) * 100, 2)
            ELSE NULL
        END AS growth_1y_pct,
        
        -- 3-year growth (annualized)
        CASE 
            WHEN pw.price_3y_ago IS NOT NULL 
                 AND ws.total_sales_3y >= 25
                 AND ws.years_in_3y_window = 3
            THEN ROUND(SAFE_DIVIDE(pw.current_median_price - pw.price_3y_ago, pw.price_3y_ago) * 100, 2)
            ELSE NULL
        END AS growth_3y_pct,
        
        -- 3-year CAGR (Compound Annual Growth Rate)
        CASE 
            WHEN pw.price_3y_ago IS NOT NULL 
                 AND ws.total_sales_3y >= 25
                 AND ws.years_in_3y_window >= 3
            THEN ROUND((POWER(SAFE_DIVIDE(pw.current_median_price, pw.price_3y_ago), 1.0/3) - 1) * 100, 2)
            ELSE NULL
        END AS cagr_3y_pct,
        
        -- 5-year growth
        CASE 
            WHEN pw.price_5y_ago IS NOT NULL 
                 AND ws.total_sales_5y >= 40
                 AND ws.years_in_5y_window = 5
            THEN ROUND(SAFE_DIVIDE(pw.current_median_price - pw.price_5y_ago, pw.price_5y_ago) * 100, 2)
            ELSE NULL
        END AS growth_5y_pct,
        
        -- 5-year CAGR
        CASE 
            WHEN pw.price_5y_ago IS NOT NULL 
                 AND ws.total_sales_5y >= 40
                 AND ws.years_in_5y_window >= 4
            THEN ROUND((POWER(SAFE_DIVIDE(pw.current_median_price, pw.price_5y_ago), 1.0/5) - 1) * 100, 2)
            ELSE NULL
        END AS cagr_5y_pct,

        -- 10-year growth
        CASE 
            WHEN pw.price_10y_ago IS NOT NULL 
                 AND ws.total_sales_10y >= 80
                 AND ws.years_in_10y_window >= 8  -- At least 8 of 10 years
            THEN ROUND(SAFE_DIVIDE(pw.current_median_price - pw.price_10y_ago, pw.price_10y_ago) * 100, 2)
            ELSE NULL
        END AS growth_10y_pct,

        -- 10-year CAGR
        CASE 
            WHEN pw.price_10y_ago IS NOT NULL 
                 AND ws.total_sales_10y >= 80
                 AND ws.years_in_10y_window >= 8
            THEN ROUND((POWER(SAFE_DIVIDE(pw.current_median_price, pw.price_10y_ago), 1.0/10) - 1) * 100, 2)
            ELSE NULL
        END AS cagr_10y_pct
        
    FROM price_windows pw
    LEFT JOIN window_sales ws
        ON pw.suburb = ws.suburb
        AND pw.property_type = ws.property_type
        AND pw.region_category = ws.region_category
)

SELECT
    suburb,
    property_type,
    region_category,
    current_year,
    
    -- Current metrics
    current_median_price,
    current_year_sales,
    
    -- Historical prices
    price_1y_ago,
    price_3y_ago,
    price_5y_ago,
    price_10y_ago,
    
    -- Growth metrics
    growth_1y_pct,
    growth_3y_pct,
    growth_5y_pct,
    growth_10y_pct,
    cagr_3y_pct,
    cagr_5y_pct,
    cagr_10y_pct,
    
    -- Sales volume by window
    total_sales_1y,
    total_sales_3y,
    total_sales_5y,
    total_sales_10y,
    
    -- Data quality metrics
    years_in_3y_window,
    years_in_5y_window,
    total_years_with_data,
    lifetime_sales,
    
    -- Reliability flags
    CASE 
        WHEN growth_1y_pct IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS has_reliable_1y_growth,
    
    CASE 
        WHEN growth_3y_pct IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS has_reliable_3y_growth,
    
    CASE 
        WHEN growth_5y_pct IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS has_reliable_5y_growth,

    CASE 
        WHEN growth_10y_pct IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS has_reliable_10y_growth,
    
    -- Overall reliability
    CASE 
        WHEN lifetime_sales >= 50 AND total_years_with_data >= 5 
        THEN TRUE 
        ELSE FALSE 
    END AS is_reliable_suburb

FROM growth_metrics
ORDER BY region_category, suburb, property_type