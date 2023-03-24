{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'modified_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'country_region_code'
        , 'country_region_name'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
    As demais colunas serão comentadas.
*/
with
    source_countryregion as(
        select
            cast(countryregioncode as string) as country_region_code
            , cast(name as string) as country_region_name
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{ source('sap_adw', 'countryregion') }}
    )
select *
from source_countryregion