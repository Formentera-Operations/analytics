{{ config(
    materialized='view',
    tags=['wellview', 'system', 'integration', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview', 'WVT_WVSYSINTEGRATION') }}
),

final as (
    select
        -- Maintain exact column mapping as per the Snowflake view
        idwell as idwell,
        idrecparent as idrecparent,
        idrec as idrec,
        tblkeyparent as tblkeyparent,
        integratordes as integratordes,
        integratorver as integratorver,
        afproduct as afproduct,
        afidentity as afidentity,
        afidrec as afidrec,
        note as note,
        syslockmeui as syslockmeui,
        syslockchildrenui as syslockchildrenui,
        syslockme as syslockme,
        syslockchildren as syslockchildren,
        syslockdate as syslockdate,
        sysmoddate as sysmoddate,
        sysmoduser as sysmoduser,
        syscreatedate as syscreatedate,
        syscreateuser as syscreateuser,
        systag as systag,
        
        -- Special column mappings to match the view
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted
        
    from source_data
)

select * from final