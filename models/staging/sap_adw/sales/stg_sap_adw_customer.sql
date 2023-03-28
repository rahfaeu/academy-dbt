{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'modified_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'customer_id'
        , 'customer_person_id'
        , 'customer_store_id'
        , 'customer_territory_id'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
    As demais colunas serão comentadas.
*/
with
    source_customer as(
        select
            cast(customerid as int64) as customer_id
            , cast(personid as int64) as customer_person_id
            , cast(storeid as int64) as customer_store_id
            , cast(territoryid as int64) as customer_territory_id
            --, rowguid
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{ source('sap_adw', 'customer') }}
    )
select *
from source_customer