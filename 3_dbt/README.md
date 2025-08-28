# Data Modelling and Analytics Engineering

## Star Schema

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
