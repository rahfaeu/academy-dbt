{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'sales_order_header_order_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'sales_order_detail_sales_order_id'
        , 'customer_fk'
        , 'ship_address_fk'
        , 'product_fk'
    ]
) }}

with
    dim_address as (
        select
            address_sk
            , address_id
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
            , sales_order_header_ship_to_address_id
            , sales_order_header_bill_to_address_id
            , sales_order_header_credit_card_id
            , sales_order_header_order_date
            , sales_order_header_due_date
            , sales_order_header_ship_date
            , sales_order_header_status
        from {{ ref('stg_sap_adw_sales_order_header') }}            
    )
    , joining_tables as (
        select
            {{ numeric_surrogate_key([
                'sales_order_detail_sales_order_id'
                , 'sales_order_header_order_date'
                , 'sales_order_header_ship_to_address_id'
                , 'sales_order_header_bill_to_address_id'
                , 'sales_order_header_credit_card_id'
                , 'sales_order_header_customer_id'
                , 'sales_order_detail_product_id'
                , 'sales_order_detail_sales_order_id'
                , 'sales_order_header_status'
            ]) }} as sales_order_detail_sales_order_item_sk
            , sales_order_detail.sales_order_detail_sales_order_id
            , sales_order_header.sales_order_header_customer_id
            , sales_order_header.sales_order_header_ship_to_address_id
            , sales_order_header.sales_order_header_bill_to_address_id
            , sales_order_header.sales_order_header_credit_card_id
            , sales_order_header.sales_order_header_order_date
            , sales_order_header.sales_order_header_due_date
            , sales_order_header.sales_order_header_ship_date
            , sales_order_header.sales_order_header_status
            , sales_order_detail.sales_order_detail_product_id
            , sales_order_detail.sales_order_detail_order_quantity
            , sales_order_detail.sales_order_detail_unit_price
            , sales_order_detail.sales_order_detail_unit_price_discount
            , sales_order_detail_unit_price * sales_order_detail_order_quantity as sales_order_detail_sales_gross_value
            , sales_order_detail_unit_price * (1 - sales_order_detail_unit_price_discount) * sales_order_detail_order_quantity as sales_order_detail_sales_net_value
        from sales_order_detail
        left join sales_order_header on sales_order_detail.sales_order_detail_sales_order_id = sales_order_header.sales_order_header_sales_order_id
    )
    , getting_fk as ( 
        select
            sales_order_detail_sales_order_item_sk
            , joining_tables.sales_order_detail_sales_order_id
            , case
                when dim_customer.customer_sk is null then -1
                else dim_customer.customer_sk
            end as customer_fk
            , case
                when ship_address.address_sk is null then -1
                else ship_address.address_sk
            end as ship_address_fk
            , case
                when bill_address.address_sk is null then -1
                else bill_address.address_sk
            end as bill_address_fk
            , case
                when dim_credit_card.credit_card_sk is null then -1
                else dim_credit_card.credit_card_sk
            end as credit_card_fk
            , case
                when dim_product.product_sk is null then -1
                else dim_product.product_sk
            end as product_fk
            , case
                when dim_sales_reason.sales_reason_sales_order_sk is null then -1
                else dim_sales_reason.sales_reason_sales_order_sk
            end as sales_reason_sales_order_fk
            , joining_tables.sales_order_header_order_date
            , joining_tables.sales_order_header_due_date
            , joining_tables.sales_order_header_ship_date
            , joining_tables.sales_order_header_status
            , joining_tables.sales_order_detail_order_quantity
            , joining_tables.sales_order_detail_unit_price
            , joining_tables.sales_order_detail_unit_price_discount
            , joining_tables.sales_order_detail_sales_gross_value
            , joining_tables.sales_order_detail_sales_net_value
        from joining_tables
        /* Fazemos join com a tabela de endereço 2 vezes para pegar a sk do endereço de cobrança e o de entrega */
        left join dim_address as ship_address on joining_tables.sales_order_header_ship_to_address_id = ship_address.address_id
        left join dim_address as bill_address on joining_tables.sales_order_header_bill_to_address_id = bill_address.address_id
        left join dim_credit_card on joining_tables.sales_order_header_credit_card_id = dim_credit_card.credit_card_id
        left join dim_customer on joining_tables.sales_order_header_customer_id = dim_customer.customer_id
        left join dim_product on joining_tables.sales_order_detail_product_id = dim_product.product_id
        left join dim_sales_reason on joining_tables.sales_order_detail_sales_order_id = dim_sales_reason.sales_order_header_sales_reason_sales_order_id
    )
select *
from getting_fk