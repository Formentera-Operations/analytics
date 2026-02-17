{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for MDM Calendar Dimension

    Source: MDM_CALENDAR (Estuary batch, 37K rows)
    Grain: One row per calendar date (date_key)

    Notes:
    - Shared MDM table, not ODA-specific — used across multiple domains
    - Primary key is date_key (integer), not id
    - No audit columns (CREATEDATE/UPDATEDATE) in source
    - is_weekend already boolean in source (no conversion needed)
    - Source has typo: FIRSTDATEOFQUATER / LASTDATEOFQUATER (missing R)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'MDM_CALENDAR') }}
),

renamed as (
    select
        -- primary key
        DATEKEY::int as date_key,

        -- date information
        "DATE"::date as date, -- noqa: RF06
        "DAY"::int as day, -- noqa: RF06
        trim(DAYSUFFIX)::varchar as day_suffix,
        DAYOFYEAR::int as day_of_year,

        -- week information
        WEEKDAY::int as weekday,
        trim(WEEKDAYNAME)::varchar as weekday_name,
        trim(WEEKDAYNAME_SHORT)::varchar as weekday_name_short,
        trim(WEEKDAYNAME_FIRSTLETTER)::varchar as weekday_name_first_letter,
        ISWEEKEND::boolean as is_weekend,
        "WEEK"::date as week, -- noqa: RF06
        trim(WEEKNAME)::varchar as week_name,
        WEEKOFMONTH::int as week_of_month,
        WEEKOFYEAR::int as week_of_year,
        FIRSTDATEOFWEEK::date as first_date_of_week,
        LASTDATEOFWEEK::date as last_date_of_week,
        DOWINMONTH::int as dow_in_month,

        -- month information
        "MONTH"::int as month, -- noqa: RF06
        trim(MONTHNAME)::varchar as month_name,
        trim(MONTHNAME_SHORT)::varchar as month_name_short,
        trim(MONTHNAME_FIRSTLETTER)::varchar as month_name_first_letter,
        trim(MMYYYY)::varchar as mm_yyyy,
        trim(MONTHYEAR)::varchar as month_year,
        trim(MONTHYEARNAME)::varchar as month_year_name,
        FIRSTDATEOFMONTH::date as first_date_of_month,
        LASTDATEOFMONTH::date as last_date_of_month,

        -- quarter information
        "QUARTER"::int as quarter, -- noqa: RF06
        trim(QUARTERNAME)::varchar as quarter_name,
        FIRSTDATEOFQUATER::date as first_date_of_quarter,
        LASTDATEOFQUATER::date as last_date_of_quarter,

        -- year information
        "YEAR"::int as year, -- noqa: RF06
        FIRSTDATEOFYEAR::date as first_date_of_year,
        LASTDATEOFYEAR::date as last_date_of_year,

        -- ingestion metadata
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where date_key is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['date_key']) }} as calendar_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        calendar_sk,

        -- primary key
        date_key,

        -- date information
        date,
        day,
        day_suffix,
        day_of_year,

        -- week information
        weekday,
        weekday_name,
        weekday_name_short,
        weekday_name_first_letter,
        is_weekend,
        week,
        week_name,
        week_of_month,
        week_of_year,
        first_date_of_week,
        last_date_of_week,
        dow_in_month,

        -- month information
        month,
        month_name,
        month_name_short,
        month_name_first_letter,
        mm_yyyy,
        month_year,
        month_year_name,
        first_date_of_month,
        last_date_of_month,

        -- quarter information
        quarter,
        quarter_name,
        first_date_of_quarter,
        last_date_of_quarter,

        -- year information
        year,
        first_date_of_year,
        last_date_of_year,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
