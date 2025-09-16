with deduped_snapshot as (
    select *
    from {{ ref('stg_google_nswprop_data_2001_2024') }}
),

summary as (
    select
        count(*) as total_rows,
        count(distinct unique_row_id) as distinct_rows,
        1 - count(distinct unique_row_id) / count(*) as pct_duplicates
    from deduped_snapshot
),

audit_duplicates as (
    select
        unique_row_id,
        count(*) as duplicate_count
    from deduped_snapshot
    group by unique_row_id
    having count(*) > 1
)

select 'summary' as type, total_rows, distinct_rows, pct_duplicates, NULL as unique_row_id, NULL as duplicate_count
from summary

union all

select 'duplicates' as type, NULL as total_rows, NULL as distinct_rows, NULL as pct_duplicates, unique_row_id, duplicate_count
from audit_duplicates
