{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'sales_order_detail_sales_order_sk'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'sales_order_detail_sales_order_id'
        , 'sales_order_header_customer_id'
        , 'sales_order_header_territory_id'
        , 'sales_order_header_credit_card_id'
    ]
) }}

with
    dim_address as (
        select
            address_sk
            , address_id
            , state_province_territory_id
        from {{ ref('dim_sap_adw_address') }}
    )
    , dim_credit_card as (
        select
            credit_card_sk
            , credit_card_id
        from {{ ref('dim_sap_adw_credit_card') }}
    )
    , dim_customer as (
        select
            customer_sk
            , customer_id
        from {{ ref('dim_sap_adw_customer') }}
    )
    , dim_product as (
        select
            product_sk
            , product_id
        from {{ ref('dim_sap_adw_product') }}
    )
    , dim_sales_reason as (
        select
            sales_reason_sales_order_sk
            , sales_order_header_sales_reason_sales_order_id
        from {{ ref('dim_sap_adw_sales_reason') }}
    )
    , sales_order_detail as (
        select
            sales_order_detail_sales_order_id
            , sales_order_detail_product_id
            , sales_order_detail_order_quantity
            , sales_order_detail_unit_price
            , sales_order_detail_unit_price_discount
        from {{ ref('stg_sap_adw_sales_order_detail') }}
    )
    , sales_order_header as (
        select
            sales_order_header_sales_order_id
            , sales_order_header_customer_id
            , sales_order_header_territory_id
            , sales_order_header_ship_to_address_id
            , sales_order_header_credit_card_id
            , sales_order_header_order_date
            , sales_order_header_due_date
            , sales_order_header_ship_date
            , sales_order_header_status
            , sales_order_header_sub_total
            , sales_order_header_total_due
        from {{ ref('stg_sap_adw_sales_order_header') }}            
    )
    , joining_tables as (
        select
            sales_order_detail.sales_order_detail_sales_order_id
            , sales_order_header.sales_order_header_customer_id
            , sales_order_header.sales_order_header_territory_id
            -- , sales_order_header.sales_order_header_ship_to_address_id
            , sales_order_header.sales_order_header_credit_card_id
            , sales_order_header.sales_order_header_order_date
            , sales_order_header.sales_order_header_due_date -- Criar dim_dates
            , sales_order_header.sales_order_header_ship_date
            , sales_order_header.sales_order_header_status
            , sales_order_detail.sales_order_detail_product_id
            , sales_order_detail.sales_order_detail_order_quantity
            , sales_order_detail.sales_order_detail_unit_price
            , sales_order_detail.sales_order_detail_unit_price_discount
            , sales_order_header.sales_order_header_sub_total
            , sales_order_header.sales_order_header_total_due
        from sales_order_detail
        left join sales_order_header on sales_order_detail.sales_order_detail_sales_order_id = sales_order_header.sales_order_header_sales_order_id
    )
    , creating_sk as ( 
        select
            {{ numeric_surrogate_key(['sales_order_detail_sales_order_id']) }} as sales_order_detail_sales_order_sk
            , joining_tables.sales_order_detail_sales_order_id
            , dim_customer.customer_sk
            , dim_address.address_sk
            -- , sales_order_header.sales_order_header_ship_to_address_id
            , dim_credit_card.credit_card_sk
            , dim_product.product_sk
            , joining_tables.sales_order_header_order_date
            , joining_tables.sales_order_header_due_date
            , joining_tables.sales_order_header_ship_date
            , joining_tables.sales_order_header_status
            , joining_tables.sales_order_detail_order_quantity
            , joining_tables.sales_order_detail_unit_price
            , joining_tables.sales_order_detail_unit_price_discount
            , joining_tables.sales_order_header_sub_total
            , joining_tables.sales_order_header_total_due
        from joining_tables
        left join dim_address on joining_tables.sales_order_header_territory_id = dim_address.state_province_territory_id
        left join dim_credit_card on joining_tables.sales_order_header_credit_card_id = dim_credit_card.credit_card_id
        left join dim_customer on joining_tables.sales_order_header_customer_id = dim_customer.customer_id
        left join dim_product on joining_tables.sales_order_detail_product_id = dim_product.product_id
    )
select *
from creating_sk



