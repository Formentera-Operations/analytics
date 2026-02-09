{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPUMPROD') }}
        qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)
),

renamed as (
    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,

        -- Pump specifications (metric / raw - no conversions for WiseRock)
        pumpdiameter,

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