{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'modified_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'address_id'
        , 'address_city'
        , 'address_state_province_id'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
    As demais colunas serão comentadas.
*/
with
    source_address as(
        select
            cast(addressid as int) as address_id
            -- , cast(addressline1 as string) as address_line_1
            -- , cast(addressline2 as string) as address_line_2
            , cast(city as string) as address_city
            , cast(stateprovinceid as int) as address_state_province_id
            -- , cast(postalcode as string) as address_postal_code
            -- , cast(spatiallocation as string) as address_spatial_location
            -- , cast(rowguid as string) as address_row_guid
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{ source('sap_adw', 'address') }}
    )
select *
from source_address