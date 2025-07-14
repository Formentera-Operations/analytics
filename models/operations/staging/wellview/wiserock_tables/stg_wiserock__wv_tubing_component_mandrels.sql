{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'components', 'mandrels', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMPMANDREL') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- Mandrel station information
        stationno,
        
        -- System fields
        syslockdate,
        syscreatedate,
        sysmoduser,
        syslockchildrenui,
        syslockmeui,
        syscreateuser,
        syslockchildren,
        systag,
        sysmoddate,
        syslockme,
        
        -- Fivetran metadata
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted

    from source_data
)

select * from renamed