{{ config(
    cluster_by = [
        'customer_id'
        , 'customer_name'
        , 'customer_territory_id'
    ]
) }}

with 
    customer as (
        select 
            customer_id
            , customer_person_id
            , customer_store_id
            , customer_territory_id
        from {{ ref('stg_sap_adw_customer') }}
    )
    , person as (
        select 
            person_business_entity_id
            , concat(person_first_name, " ", person_last_name) as person_name
        from {{ ref('stg_sap_adw_person') }}
    )
    , store as (
        select
            store_id
            , store_name
        from {{ ref('stg_sap_adw_store') }}
    )
    , joining_tables as (
      select 
          customer.customer_id
          , case
              when customer.customer_store_id is not null then store.store_name
              else person.person_name
          end as customer_name
          , customer.customer_territory_id
      from customer
      left join person on customer.customer_person_id = person.person_business_entity_id
      left join store on customer.customer_store_id = store.store_id
    )
    , creating_customer_sk as (
        select
            {{ numeric_surrogate_key([
                'customer_id'
                , 'customer_name'
                , 'customer_territory_id'
            ]) }} as customer_sk
            , customer_id
            , customer_name
            , customer_territory_id
        from joining_tables
    )
select *
from creating_customer_sk