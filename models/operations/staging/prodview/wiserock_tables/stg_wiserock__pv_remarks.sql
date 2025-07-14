{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('prodview', 'PVT_PVUNITREMARK') }}

),

renamed as (

    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,
        
        -- Date and comments
        dttm,
        com,
        
        -- Remark information
        typ,
        idrecitem,
        idrecitemtk,
        
        -- Flags
        actionrqd,
        explainedimbalance,
        
        -- System lock fields
        syslockmeui,
        syslockchildrenui,
        syslockme,
        syslockchildren,
        syslockdate,
        
        -- System metadata
        sysmoddate,
        sysmoduser,
        syscreatedate,
        syscreateuser,
        systag,
        
        -- Fivetran metadata mapped to standard names
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted

    from source

)

select * from renamed