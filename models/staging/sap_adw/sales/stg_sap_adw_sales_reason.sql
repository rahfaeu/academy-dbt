{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'modified_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'sales_reason_id'
        , 'sales_reason_name'
        , 'sales_reason_type'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
    As demais colunas serão comentadas.
*/
with
    source_salesreason as(
        select
            cast(salesreasonid as int) as sales_reason_id
            , cast(name as string) as sales_reason_name
            , cast(reasontype as  string) as sales_reason_type
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{ source('sap_adw', 'salesreason') }}
    )
select *
from source_salesreason