{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'modified_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'product_id'
        , 'product_name'
        , 'product_number'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
    As demais colunas serão comentadas.
*/
with
    source_product as(
        select
            cast(productid as int64) as product_id
            --, productsubcategoryid
            --, productmodelid
            , cast(name as string) as product_name
            , cast(productnumber as string) as product_number
            --, makeflag
            --, finishedgoodsflag
            --, color
            --, safetystocklevel
            --, reorderpoint
            --, standardcost
            --, listprice
            --, size
            --, sizeunitmeasurecode
            --, weightunitmeasurecode
            --, weight
            --, daystomanufacture
            --, productline
            --, class
            --, style
            --, sellstartdate
            --, sellenddate
            --, discontinueddate
            --, rowguid
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{ source('sap_adw', 'product') }}
    )
select *
from source_product