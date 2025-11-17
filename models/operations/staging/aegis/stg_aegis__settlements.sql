{{
    config(
        materialized='view',
        tags=['aegis', 'staging', 'hedging']
    )
}}

with

source as (

    select * from {{ source('aegis_raw', 'SETTLEMENTS') }}

),

renamed as (

    select
        date::timestamp_ntz as settlement_date,
        pricingindexcode::varchar as pricing_index_code,
        pricingindexname::varchar as pricing_index_name,
        dailymonthly::int as daily_monthly_flag,
        value::float as settlement_value,
        _portable_extracted::timestamp_ntz as extracted_at

    from source

),

filtered as (

    select * from renamed
    where settlement_date is not null
        and pricing_index_code is not null

),

enhanced as (

    select
        *,
        year(settlement_date) as settlement_year,
        month(settlement_date) as settlement_month,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        settlement_date,
        pricing_index_code,
        pricing_index_name,
        daily_monthly_flag,
        settlement_value,
        settlement_year,
        settlement_month,
        extracted_at,
        _loaded_at

    from enhanced

)

select * from final