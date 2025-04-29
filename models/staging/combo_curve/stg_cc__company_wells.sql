with source as (

    select * from {{ source('combo_curve', 'wells') }}

),

renamed as (

    select
        -- ids
        id as well_id,
        ariesid as aries_id,
        phdwinid as phdwin_id,
        chosenid as chosen_id,
        wellname as well_name,
        wellnumber as well_number,
        welltype as well_type,
        leasename as lease_name,
        api10 as api_10,
        api12 as api_12,
        api14 as api_14,

        -- operator details

        currentoperator as operator,
        currentoperatorcode as operator_code,
        case 
            when customstring3 = 'OP' then TRUE
            else false
        end as is_operated,            
        
        -- well details
        status,
        primaryproduct as primary_product,
        surfacelatitude as surface_latitude,
        surfacelongitude as surface_longitude,
        measureddepth as measured_depth,
        trueverticaldepth as true_vertical_depth,
        laterallength as lateral_length,
        
        -- location
        basin,
        -- strip out state abbreviations from county (format: "MCCLAIN (OK)")
        regexp_replace(county, '\\s*\\([A-Z]{2}\\)$', '') as county,
        state,

        -- combocurve custom columns

        customstring1 as reserve_category,
        customstring22 as company_name,
        customstring3 as operator_cateogry,

        
        -- metadata
        datapool as data_pool,
        datasource as data_source,
        hasdaily as has_daily,
        hasmonthly as has_monthly,
        createdat as created_at,
        updatedat as updated_at,
        _portable_extracted

    from source

)

select * from renamed