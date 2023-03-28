{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'sales_order_header_order_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'sales_order_header_customer_id'
        , 'sales_order_header_territory_id'
        , 'sales_order_header_ship_to_address_id'
        , 'sales_order_header_credit_card_id'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
*/
with
    source_salesorderheader as (
        select 
            cast(salesorderid as int64) as sales_order_header_sales_order_id
            , cast(customerid as int64) as sales_order_header_customer_id
            , cast(territoryid as int64) as sales_order_header_territory_id
            , cast(shiptoaddressid as int64) as sales_order_header_ship_to_address_id
            , cast(creditcardid	as int64) as sales_order_header_credit_card_id
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', orderdate)) as sales_order_header_order_date
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', duedate)) as sales_order_header_due_date
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', shipdate)) as sales_order_header_ship_date
            , cast(status as int64) as sales_order_header_status
            , cast(subtotal	as float64)	as sales_order_header_subtotal
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{source('sap_adw','salesorderheader')}}
    )
select *
from source_salesorderheader