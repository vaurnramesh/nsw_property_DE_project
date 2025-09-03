# QA
## Data Quality result - 03/09/2025

```csv
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
```	

### SUMMARY
#### Property Type Distribution
- HOUSE: 2,366,222
- UNIT/APARTMENT: 1,252,988
- Townhouse: 111,809
- Vacant Land: 542,394

#### Null Checks

- total_rows: 4,273,413
- null_property_id: 0
- null_sale_price: 17 (a tiny fraction further investigation needed)
- null_property_type: 0
- section_mismatch: 9,292 (section_no exists but property_type isn’t UNIT/APARTMENT)
- townhouse_mismatch: 61,584 (street_no matches townhouse pattern but property_type isn’t Townhouse)
- vacantland_mismatch: 0
