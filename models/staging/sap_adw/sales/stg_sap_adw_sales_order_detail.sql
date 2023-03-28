{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'modified_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'sales_order_detail_sales_order_id'
        , 'sales_order_detail_product_id'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
*/
with
    source_salesorderdetail as(
        select
            cast(salesorderid as int64) sales_order_detail_sales_order_id
            --, salesorderdetailid
            , cast(productid as int64) as sales_order_detail_product_id
            --, castspecialofferid
            --, carriertrackingnumber
            , cast(orderqty as int64) as sales_order_detail_order_quantity
            , cast(unitprice as float64) as sales_order_detail_unit_price
            , cast(unitpricediscount as float64) as sales_order_detail_unit_price_discount
            --, rowguid
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{ source('sap_adw', 'salesorderdetail') }}
    )
select *
from source_salesorderdetail