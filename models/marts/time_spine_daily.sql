
{{ config(materialized='table') }}

with base_dates as (

    {{ dbt.date_spine(
        'day',
        "DATE('2000-01-01')",
        "DATE('2030-01-01')"
    ) }}

)

select cast(date_day as date) as date_day
from base_dates
where date_day > dateadd(year, -5, current_date())
  and date_day < dateadd(day, 30, current_date())