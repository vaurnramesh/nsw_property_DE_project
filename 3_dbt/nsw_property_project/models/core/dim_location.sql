{{ config(materialized='table') }}

WITH base AS (
    SELECT DISTINCT
        postcode,
        suburb,
        lga_code
    FROM {{ ref("stg_google_nswprop_data_2001_2024") }}
    WHERE postcode IS NOT NULL
    AND suburb IS NOT NULL
),

gccsa_ranked AS (
    -- Rank GCCSA per postcode by RATIO_FROM_TO to pick the dominant region
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY postcode ORDER BY RATIO_FROM_TO DESC) AS rn
    FROM {{ ref("postcode_to_gccsa") }}
    WHERE GCCSA_NAME_2021 IN ('Greater Sydney', 'Rest of NSW')
),

gccsa_deduped AS (
    SELECT
        CAST(POSTCODE as string) AS postcode,
        GCCSA_CODE_2021 AS gccsa_code,
        GCCSA_NAME_2021 AS gccsa_name
    FROM gccsa_ranked
    WHERE rn = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['b.postcode', 'b.suburb', 'b.lga_code']) }} AS location_id,
    b.postcode,
    b.suburb,
    b.lga_code,
    g.gccsa_code,
    g.gccsa_name
FROM base b
LEFT JOIN gccsa_deduped g
    ON b.postcode = g.postcode
