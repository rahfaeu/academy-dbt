{{ config(
    cluster_by = [
        'address_id'
        , 'address_city'
        , 'state_province_name'
        , 'country_region_name'
    ]
) }}

with
    dim_date as (
        select
            date_id
            , `date`
            , year_date
            , year_week
            , year_day
            , fiscal_year
            , fiscal_qtr
            , month_date
            , month_name
            , week_day
            , day_name
            , day_is_weekday
        from {{ ref('stg_conformed_date') }}
    )
select *
from dim_date
