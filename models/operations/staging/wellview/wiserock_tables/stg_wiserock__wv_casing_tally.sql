{{ config(
    materialized='view',
    tags=['wellview', 'casing', 'components', 'tally', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVCASCOMPTALLY') }}
),

final as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- Centralizer information
        centralized,
        centralizersdes,
        centralizersno,
        
        -- Depths (converted to feet)
        depthbtmcalc / 0.3048 as depthbtmcalc,
        depthtopcalc / 0.3048 as depthtopcalc,
        depthtvdbtmcalc / 0.3048 as depthtvdbtmcalc,
        depthtvdtopcalc / 0.3048 as depthtvdtopcalc,
        
        -- Component details
        extjewelry,
        heatno,
        jointrun,
        
        -- Lengths (converted to feet)
        length / 0.3048 as length,
        lengthcumcalc / 0.3048 as lengthcumcalc,
        
        -- Reference information
        refid,
        refno,
        runnocalc,
        
        -- Volumes (converted to barrels)
        volumedispcumcalc / 0.158987294928 as volumedispcumcalc,
        volumeinternalcalc / 0.158987294928 as volumeinternalcalc,
        volumeinternalcumcalc / 0.158987294928 as volumeinternalcumcalc,
        
        -- Weight (converted to 1000LBF)
        weightcumcalc / 4448.2216152605 as weightcumcalc,
        
        -- System sequence
        sysseq,
        
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
        
        -- Special column mappings to match the view
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted
        
    from source_data
)

select * from final