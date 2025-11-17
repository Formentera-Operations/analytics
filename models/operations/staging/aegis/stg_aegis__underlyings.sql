{{
    config(
        materialized='view',
        tags=['aegis', 'staging', 'hedging']
    )
}}

with

source as (

    select * from {{ source('aegis_raw', 'UNDERLYINGS') }}

),

renamed as (

    select
        id::int as underlying_id,
        code::varchar as underlying_code,
        name::varchar as underlying_name,
        product::varchar as product_type,
        _portable_extracted::timestamp_ntz as extracted_at

    from source

),

filtered as (

    select * from renamed
    where underlying_id is not null

),

enhanced as (

    select
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        underlying_id,
        underlying_code,
        underlying_name,
        product_type,
        extracted_at,
        _loaded_at

    from enhanced

)

select * from final