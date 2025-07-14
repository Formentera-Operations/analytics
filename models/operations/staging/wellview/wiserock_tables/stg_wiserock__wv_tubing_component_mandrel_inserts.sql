{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'components', 'mandrels', 'inserts', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMPMANDRELINSERT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- Basic information
        com,
        
        -- Dates
        dttmpull,
        dttmrun,
        
        -- Latch specifications
        latchmaterial,
        latchtyp,
        
        -- Manufacturing details
        make,
        model,
        
        -- Material specifications
        orificematerial,
        
        -- Operational information
        pullreason,
        refid,
        retrievemeth,
        service,
        sn,
        
        -- Dimensions (converted from meters to inches)
        szod / 0.0254 as szod,
        szport / 0.0254 as szport,
        
        -- Pressures (converted from kPa to PSI)
        pressurfgaugeopen / 6.894757 as pressurfgaugeopen,
        pressurfgaugeclose / 6.894757 as pressurfgaugeclose,
        tropull / 6.894757 as tropull,
        trorun / 6.894757 as trorun,
        
        -- Temperature (converted from Celsius to Fahrenheit)
        temp / 0.555555555555556 + 32 as temp,
        
        -- Valve specifications
        valvedes,
        valvematerial,
        valvepacking,
        valvetyp,
        
        -- System fields
        syslockmeui,
        syslockchildrenui,
        syslockme,
        syslockchildren,
        syslockdate,
        sysmoddate,
        sysmoduser,
        syscreatedate,
        syscreateuser,
        systag,
        
        -- Fivetran metadata
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted

    from source_data
)

select * from renamed