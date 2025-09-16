{{ config(materialized='table') }}

-- Add row_number to pick 1 canonical record per property
with ranked as (
    select 
        *,
        row_number() over (
            partition by 
                property_source_id,
                coalesce(nullif(trim(unit_number), ''), 'NA'),
                coalesce(nullif(trim(street_number), ''), 'NA'),
                upper(trim(street_name)),
                upper(trim(suburb)),
                postcode
            order by last_seen desc, property_sk asc
        ) as rn
    from {{ ref('dim_property') }}
)

select *
from ranked
where rn = 1
