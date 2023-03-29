{{ config(
    cluster_by = [
        'sales_order_header_sales_reason_sales_order_id'
        , 'sales_reason_name'
        , 'sales_reason_type'
    ]
) }}

with
    header_sales_reason as (
        select
            sales_order_header_sales_reason_sales_order_id
            , sales_order_header_sales_reason_sales_reason_id
        from {{ ref('stg_sap_adw_sales_order_header_sales_reason') }}
    )
    , sales_reason as (
        select
            sales_reason_id
            , sales_reason_name
            , sales_reason_type
        from {{ ref('stg_sap_adw_sales_reason') }}
    )
    , joining_tables as (
        select
            {{ numeric_surrogate_key(['sales_order_header_sales_reason_sales_order_id']) }} as sales_reason_sales_order_sk
            , sales_order_header_sales_reason_sales_order_id
            , sales_reason_name
            , sales_reason_type
        from header_sales_reason
        left join sales_reason on header_sales_reason.sales_order_header_sales_reason_sales_reason_id = sales_reason.sales_reason_id
    )
select *
from joining_tables
