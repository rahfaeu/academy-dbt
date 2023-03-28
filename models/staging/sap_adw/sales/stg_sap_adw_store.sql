{{ config (
    materialized = 'table'
    , cluster_by = [
        'store_id'
        , 'store_name'
        , 'store_person_id'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
    As demais colunas serão comentadas.
*/
with
    source_store as(
        select
            cast(businessentityid as int64) as store_id
            , cast(name as string) as store_name
            , cast(salespersonid as int64) as store_person_id
        from {{ source('sap_adw', 'store') }}
    )
select *
from source_store