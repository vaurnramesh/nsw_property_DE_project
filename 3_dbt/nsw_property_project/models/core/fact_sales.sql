{{ config(materialized='incremental', unique_key='unique_row_id') }}

with stg as (
    select *
    from {{ ref('stg_google_nswprop_data_2001_2024') }}
    where street_name is not null
      and suburb is not null
      and postcode is not null
),

stg_mapped as (
    select *,
        case property_category
            when 'R' then 'Residential'
            when 'V' then 'Vacant'
            when '3' then 'Other'
            else 'Other'
        end as property_category_label
    from stg
),

property_fk as (
    select
        s.*,
        p.property_sk
    from stg_mapped s
    left join {{ ref('dim_property_cleaned')}} p
        on s.property_id = p.property_source_id
        and upper(trim(s.street_name)) = upper(trim(p.street_name))
        and upper(trim(s.suburb)) = upper(trim(p.suburb))
        and upper(trim(s.postcode)) = upper(trim(p.postcode))
        and coalesce(trim(s.unit_number), 'NA') = coalesce(trim(p.unit_number), 'NA')
        and coalesce(trim(s.street_number), 'NA') = coalesce(trim(p.street_number), 'NA') 
),


-- Join to dim_date to get surrogate keys for contract and settlement dates
-- This converts the raw date fields in the staging table to integer surrogate keys in the date dimension
-- cd.date_id -> surrogate key for contract_date
-- sd.date_id -> surrogate key for settlement_date
date_fk as (
    select
        pf.*,
        cd.date_id as contract_date_id,
        sd.date_id as settlement_date_id
    from property_fk pf
    left join {{ ref('dim_dates') }} cd
      on pf.contract_date = cd.full_date
    left join {{ ref('dim_dates') }} sd
      on pf.settlement_date = sd.full_date
),

location_fk as (
    select
        df.*,
        loc.location_id
    from date_fk df
    left join {{ ref('dim_location_cleaned') }} loc
      on upper(trim(df.suburb)) = upper(trim(loc.suburb))
      and cast(df.postcode as string) = loc.postcode
),

final as (
    select
        lf.unique_row_id,
        lf.property_sk,
        lf.contract_date_id,
        lf.settlement_date_id,
        lf.location_id,
        pt.property_type_sk,
        lf.sale_price,
        current_timestamp() as created_at
    from location_fk lf
    left join {{ ref('dim_property_type') }} pt
        on upper(trim(lf.property_type)) = upper(trim(pt.property_type))
        and upper(trim(lf.property_category_label)) = upper(trim(pt.property_category))
        and coalesce(upper(trim(lf.property_type_flag)), 'NA') = coalesce(upper(trim(pt.property_type_flag)), 'NA')
)

select * from final