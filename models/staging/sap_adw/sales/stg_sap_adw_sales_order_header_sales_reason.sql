{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'modified_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'sales_order_header_sales_reason_sales_order_id'
        , 'sales_order_header_sales_reason_sales_reason_id'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
    As demais colunas serão comentadas.
*/
with
    source_salesorderheadersalesreason as(
        select
            cast(salesorderid as int) as sales_order_header_sales_reason_sales_order_id
            , cast(salesreasonid as int) as sales_order_header_sales_reason_sales_reason_id
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{ source('sap_adw', 'salesorderheadersalesreason') }}
    )
select *
from source_salesorderheadersalesreason