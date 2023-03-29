{{ config(
    cluster_by = [
        'product_sk'
        , 'product_id'
        , 'product_name'
        , 'product_number'
    ]
) }}

with
    product as (
        select
            {{ numeric_surrogate_key(['product_id']) }} as product_sk
            , product_id
            , product_name
            , product_number
        from {{ ref('stg_sap_adw_product') }}
    )
select *
from product