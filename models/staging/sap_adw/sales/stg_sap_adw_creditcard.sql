{{ config (
    materialized = 'table'
    , partition_by = {
        'field': 'modified_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'credit_card_id'
        , 'credit_card_type'
    ]
) }}

/* 
    Selecionei somente as colunas pertinentes para o desafio.
    As demais colunas serão comentadas.
*/
with
    source_creditcard as(
        select
            cast(creditcardid as int) as credit_card_id
            , cast(cardtype as string) as credit_card_type
            --, cardnumber
            --, expmonth
            --, expyear
            , date(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as modified_date
        from {{ source('sap_adw', 'creditcard') }}
    )
select *
from source_creditcard