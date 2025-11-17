{{
    config(
        materialized='view',
        tags=['aegis', 'staging', 'hedging']
    )
}}

with

source as (

    select * from {{ source('aegis_raw', 'ENTITIES') }}

),

renamed as (

    select
        platformid::int as platform_id,
        name::varchar as entity_name,
        longname::varchar as entity_long_name,
        company_slash_pe::varchar as company_type,
        _portable_extracted::timestamp_ntz as extracted_at

    from source

),

filtered as (

    select * from renamed
    where platform_id is not null

),

enhanced as (

    select
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        platform_id,
        entity_name,
        entity_long_name,
        company_type,
        extracted_at,
        _loaded_at

    from enhanced

)

select * from final