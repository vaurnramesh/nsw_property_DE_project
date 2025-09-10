{{ config(materialized='table') }}

WITH date_spine AS (
    -- Generate a continuous range of dates covering your dataset
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('1990-01-01' as date)",
    end_date="cast('2030-12-31' as date)"
   ) }}
),

final AS (
    SELECT
        cast(format_date('%Y%m%d', date_day) as int) AS date_id,          -- PK
        cast(date_day as date) AS full_date,

        -- Hierarchies
        extract(year from date_day) AS year,
        extract(quarter from date_day) AS quarter,
        extract(month from date_day) AS month,
        format_date('%B', date_day) AS month_name,

        extract(day from date_day) AS day,
        extract(dayofweek from date_day) AS day_of_week_num,
        format_date('%A', date_day) AS day_of_week_name,

        format_date('%G-W%V', date_day) AS iso_week,

        -- Essentially makes (2024, week 2 as 202402) & (1990, week 14 as 199014)
        cast(concat(
            extract(year from date_day),
            lpad(cast(extract(week from date_day) as string), 2, '0')
        ) as int) AS year_week_id
    FROM date_spine
)

SELECT * FROM final
