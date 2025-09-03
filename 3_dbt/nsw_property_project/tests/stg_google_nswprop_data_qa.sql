/*
QA Query for stg_google_nswprop_data_2001_2024
- Checks nulls, duplicates, and property_type distribution
- Uses about 300MB to scan the full dataset. 
*/

WITH null_checks AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNTIF(property_id IS NULL) AS null_property_id,
        COUNTIF(sale_price IS NULL) AS null_sale_price,
        COUNTIF(property_type IS NULL) AS null_property_type,
        COUNTIF(unit_number IS NOT NULL AND property_type != 'UNIT/APARTMENT') AS section_mismatch,
        COUNTIF(REGEXP_CONTAINS(street_number, r'\d+\s?[A-Z]') AND property_type != 'Townhouse') AS townhouse_mismatch,
        COUNTIF(property_category = 'V' AND property_type != 'Vacant Land') AS vacantland_mismatch
    FROM `nsw-property-de-project.nsw_prop_data_all.stg_google_nswprop_data_2001_2024`
),

property_type_dist AS (
    SELECT property_type, COUNT(*) AS cnt
    FROM `nsw-property-de-project.nsw_prop_data_all.stg_google_nswprop_data_2001_2024`
    GROUP BY property_type
),

duplicate_check AS (
    SELECT property_id, contract_date, COUNT(*) AS cnt
    FROM `nsw-property-de-project.nsw_prop_data_all.stg_google_nswprop_data_2001_2024`
    GROUP BY property_id, contract_date
    HAVING cnt > 1
    LIMIT 20
)

SELECT
    'NULL_CHECKS' AS check_type,
    NULL AS property_type,
    total_rows,
    null_property_id,
    null_sale_price,
    null_property_type,
    section_mismatch,
    townhouse_mismatch,
    vacantland_mismatch 
FROM null_checks

UNION ALL

SELECT
    'PROPERTY_TYPE_DISTRIBUTION' AS check_type,
    CAST(property_type AS STRING) AS property_type,
    CAST(cnt AS INT64) AS total_rows,
    NULL AS null_property_id,
    NULL AS null_sale_price,
    NULL AS null_property_type,
    NULL AS section_mismatch,
    NULL AS townhouse_mismatch,
    NULL AS vacantland_mismatch
FROM property_type_dist

UNION ALL

SELECT
    'DUPLICATES' AS check_type,
    CAST(property_id AS STRING) AS property_id,
    CAST(cnt AS INT64) AS total_rows,
    NULL, NULL, NULL, NULL, NULL, NULL
FROM duplicate_check

LIMIT 100;

/**
# Data Quality result - 03/09/2025

check_type,property_type,total_rows,null_property_id,null_sale_price,null_property_type,section_mismatch,townhouse_mismatch,vacantland_mismatch
PROPERTY_TYPE_DISTRIBUTION,Townhouse,111809,,,,,,
PROPERTY_TYPE_DISTRIBUTION,Vacant Land,542394,,,,,,
PROPERTY_TYPE_DISTRIBUTION,HOUSE,2366222,,,,,,
PROPERTY_TYPE_DISTRIBUTION,UNIT/APARTMENT,1252988,,,,,,
NULL_CHECKS,,4273413,0,17,0,9292,61584,0
DUPLICATES,,20,,,,,,
DUPLICATES,,5,,,,,,
DUPLICATES,,11,,,,,,
DUPLICATES,,2,,,,,,
DUPLICATES,1039504,2,,,,,,
DUPLICATES,1150197,4,,,,,,
DUPLICATES,1188694,2,,,,,,
DUPLICATES,1345874,19,,,,,,
DUPLICATES,1373971,2,,,,,,
DUPLICATES,1422856,2,,,,,,
DUPLICATES,1471459,2,,,,,,
DUPLICATES,1478093,2,,,,,,
DUPLICATES,1484445,2,,,,,,
DUPLICATES,1515158,2,,,,,,
DUPLICATES,1527526,2,,,,,,
DUPLICATES,1538899,2,,,,,,
DUPLICATES,1548720,3,,,,,,
DUPLICATES,1588882,20,,,,,,
DUPLICATES,1588882,20,,,,,,
DUPLICATES,1609587,2,,,,,,		

-----------------------------------------
-----------------------------------------
# SUMMARY
## Property Type Distribution
1) HOUSE: 2,366,222
2) UNIT/APARTMENT: 1,252,988
3) Townhouse: 111,809
4) Vacant Land: 542,394

## Null Checks
total_rows: 4,273,413
null_property_id: 0
null_sale_price: 17 (a tiny fraction, further investigation needed)
null_property_type: 0
section_mismatch: 9,292 (section_no exists but property_type isn’t UNIT/APARTMENT)
townhouse_mismatch: 61,584 (street_no matches townhouse pattern but property_type isn’t Townhouse)
vacantland_mismatch: 0
-----------------------------------------
-----------------------------------------
**/