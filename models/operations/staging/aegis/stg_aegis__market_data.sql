{{
    config(
        materialized='view',
        tags=['aegis', 'staging', 'hedging']
    )
}}

with

source as (

    select * from {{ source('aegis_raw', 'MARKET_DATA') }}

),

renamed as (

    select
        _id::varchar as market_data_id,
        code::varchar as pricing_index_code,
        name::varchar as pricing_index_name,
        product::varchar as product_type,
        asofdate::timestamp_ntz as as_of_date,
        deliverydate::varchar as delivery_date_string,
        try_to_date(deliverydate, 'YYYY-MM-DD') as delivery_date,
        price::float as price,
        _portable_extracted::timestamp_ntz as extracted_at

    from source

),

filtered as (

    select * from renamed
    where market_data_id is not null
        and as_of_date is not null

),

enhanced as (

    select
        *,
        year(delivery_date) as delivery_year,
        month(delivery_date) as delivery_month,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        market_data_id,
        pricing_index_code,
        pricing_index_name,
        product_type,
        as_of_date,
        delivery_date,
        delivery_year,
        delivery_month,
        price,
        extracted_at,
        _loaded_at

    from enhanced

)

select * from final