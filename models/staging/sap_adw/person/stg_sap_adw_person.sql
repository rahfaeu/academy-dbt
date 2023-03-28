{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'modified_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'person_business_entity_id'
        , 'person_first_name'
        , 'person_last_name'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
    As demais colunas serão comentadas.
*/
with
    source_person as(
        select
            cast(businessentityid as int) as person_business_entity_id
            --, persontype
            --, namestyle
            --, title
            , cast(firstname as string) as person_first_name
            --, middlename
            , cast(lastname as string) as person_last_name
            --, suffix
            --, emailpromotion
            --, additionalcontactinfo
            --, demographics
            --, rowguid
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{ source('sap_adw', 'person') }}
    )
select *
from source_person