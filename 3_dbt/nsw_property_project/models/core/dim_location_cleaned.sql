{{ config(materialized='table') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by upper(trim(suburb)), postcode
            order by location_id asc
        ) as rn
    from {{ ref('dim_location') }}
    where gccsa_code is not null
    and gccsa_name is not null
)

select
    location_id,                          -- keep existing UUID/hash
    upper(trim(suburb)) as suburb,
    postcode,
    lga_code,
    gccsa_code,
    gccsa_name
from ranked
where rn = 1
