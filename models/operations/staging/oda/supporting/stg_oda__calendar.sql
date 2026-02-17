with source as (

    select * from {{ source('oda', 'MDM_CALENDAR') }}

),

renamed as (

    select
        -- Primary key
        DATEKEY as date_key,

        -- Date information
        DATE as date,
        DAY as day,
        DAYSUFFIX as day_suffix,
        DAYOFYEAR as day_of_year,

        -- Week information
        WEEKDAY as weekday,
        WEEKDAYNAME as weekday_name,
        WEEKDAYNAME_SHORT as weekday_name_short,
        WEEKDAYNAME_FIRSTLETTER as weekday_name_first_letter,
        ISWEEKEND as is_weekend,
        WEEK as week,
        WEEKNAME as week_name,
        WEEKOFMONTH as week_of_month,
        WEEKOFYEAR as week_of_year,
        FIRSTDATEOFWEEK as first_date_of_week,
        LASTDATEOFWEEK as last_date_of_week,
        DOWINMONTH as dow_in_month,

        -- Month information
        MONTH as month,
        MONTHNAME as month_name,
        MONTHNAME_SHORT as month_name_short,
        MONTHNAME_FIRSTLETTER as month_name_first_letter,
        MMYYYY as mm_yyyy,
        MONTHYEAR as month_year,
        MONTHYEARNAME as month_year_name,
        FIRSTDATEOFMONTH as first_date_of_month,
        LASTDATEOFMONTH as last_date_of_month,

        -- Quarter information
        QUARTER as quarter,
        QUARTERNAME as quarter_name,
        FIRSTDATEOFQUATER as first_date_of_quarter,
        LASTDATEOFQUATER as last_date_of_quarter,

        -- Year information
        YEAR as year,
        FIRSTDATEOFYEAR as first_date_of_year,
        LASTDATEOFYEAR as last_date_of_year,

        -- Metadata
        FLOW_PUBLISHED_AT as flow_published_at,

        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed
