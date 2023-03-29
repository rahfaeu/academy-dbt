{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'date_id'
        , 'year_date'
        , 'month_date'
        , 'year_week'
    ]
) }}

with
    date_array as (
        select date_range
        from unnest(generate_date_array('2011-05-01', '2014-01-31', interval 1 day)) as date_range
    )
    , dim_calendar as (
        select
            format_date('%F', date_range) as date_id
            , cast(date_range as date) as `date`
            , extract(year from date_range) as `year_date`
            , extract(week from date_range) as year_week
            , extract(day from date_range) as year_day
            , extract(year from date_range) as fiscal_year
            , format_date('%Q', date_range) as fiscal_qtr
            , extract(month from date_range) as month_date
            , format_date('%B', date_range) as month_name
            , format_date('%w', date_range) as week_day
            , format_date('%A', date_range) as day_name
            , case
                when format_date('%A', date_range) in ('Sunday', 'Saturday') then 0
                else 1
            end as day_is_weekday
        from date_array
    )
select *
from dim_calendar