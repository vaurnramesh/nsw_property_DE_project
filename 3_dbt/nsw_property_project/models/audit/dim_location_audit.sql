{{ config(materialized='view') }}

with deduped as (
    select *
    from {{ ref('dim_location') }}
),

summary as (
    select
        count(*) as total_rows,
        count(distinct upper(trim(suburb)), postcode) as distinct_locations,
        1 - count(distinct upper(trim(suburb)), postcode) / count(*) as pct_duplicates
    from deduped
),

duplicates as (
    select
        upper(trim(suburb)) as suburb,
        postcode,
        count(*) as duplicate_count
    from deduped
    group by upper(trim(suburb)), postcode
    having count(*) > 1
)

select 'summary' as type, total_rows, distinct_locations, pct_duplicates, NULL as suburb, NULL as postcode, NULL as duplicate_count
from summary

union all

select 'duplicates' as type, NULL as total_rows, NULL as distinct_locations, NULL as pct_duplicates,
       suburb, postcode, duplicate_count
from duplicates
