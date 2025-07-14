{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'components', 'tally', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMPTALLY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- Centralizer information
        centralized,
        centralizersdes,
        centralizersno,
        
        -- Depths (converted from meters to feet)
        depthbtmcalc / 0.3048 as depthbtmcalc,
        depthtopcalc / 0.3048 as depthtopcalc,
        depthtvdbtmcalc / 0.3048 as depthtvdbtmcalc,
        depthtvdtopcalc / 0.3048 as depthtvdtopcalc,
        
        -- External jewelry
        extjewelry,
        
        -- Joint and run information
        jointrun,
        
        -- Lengths (converted from meters to feet)
        length / 0.3048 as length,
        lengthcumcalc / 0.3048 as lengthcumcalc,
        
        -- Reference information
        refid,
        refno,
        
        -- Run number (no conversion)
        runnocalc,
        
        -- Volumes (converted from cubic meters to barrels)
        volumedispcumcalc / 0.158987294928 as volumedispcumcalc,
        volumeinternalcalc / 0.158987294928 as volumeinternalcalc,
        volumeinternalcumcalc / 0.158987294928 as volumeinternalcumcalc,
        
        -- Weight (converted from Newtons to 1000 lbf)
        weightcumcalc / 4448.2216152605 as weightcumcalc,
        
        -- System fields
        sysseq,
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