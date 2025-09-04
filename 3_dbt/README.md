# Data Modelling and Analytics Engineering
## Business Questions

#### How have property prices changed over time?
- Median sale_price per year (or per quarter) by property_type
- Comparison between Unit/Apartment vs House vs Townhouse vs Vacant Land.
- Comparison by region: Greater Sydney vs Rest of NSW

#### Which suburbs have had the highest appreciation?
- Median % price growth per suburb over time
- “Top 10 suburbs by growth (2001–2024) for units/apartments”
- Are top-growing suburbs concentrated in Greater Sydney or regional NSW?

#### What has been the volume of property sales?
- Count of sales per year / quarter
- Volume by property_type to understand market composition
- Volume by region (Metro vs Regional) and by SA4/SA2 
- Seasonal trends: monthly or quarterly sales peaks.

#### Property type distribution and evolution
- Count/proportion of each property_type sold per year
- Count of flagged property types to understand edge-case prevalence
- Compare property type mix in Greater Sydney vs Rest of NSW

#### Top-performing regions or suburbs
- Identify SA4/SA2 regions with highest average or median price growth.
- Compare growth between Metro vs Regional areas.

#### Further ideas
- Sales with multiple transactions to calculate appreciation per property
- Sales price trends relative to zoning or other geographic attributes


## Phase 1 – Staging & Seeds

1. Staging table: stg_google_nswprop_data_2001_2024 (cleaned, deduped, property types flagged)

2. Seed table: postcode_sa2_nsw.csv (keep SA3/SA4 for reference, but only join on postcode → GCCSA for your analysis)

3. Goal: Prepare clean, ready-to-use raw data for dimensional modeling

```
                     +----------------+
                     |    dim_lga     |
                     |----------------|
                     | lga_code (PK)  |
                     | lga_name       |
                     +----------------+
                             ^
                             |
                             |
                             |
+----------------+     +----------------+     +----------------+
|  dim_time      |     |  fact_sales    |     |  dim_property  |
|----------------|     |----------------|     |----------------|
| time_id (PK)   |<----| unique_row_id  |---->| property_id(PK)|
| date           |     | property_id FK |     | building_name  |
| year           |     | lga_code  FK   |     | section_no     |
| month          |     | time_id FK     |     | street_no      |
| quarter        |     | processed_dt   |     | street_name    |
| day_of_week    |     | contract_date  |     | locality       |
+----------------+     | settlement_date|     | postcode       |
                       | sale_price     |     | zoning         |
                       | land_area_sqm  |     | property_cat   |
                       +----------------+     | property_desc  |
                                              | area_type      |
                                              +----------------+

```

## Granularity

## Slowly Changing Dimensions

## DBT models

## QA testing
