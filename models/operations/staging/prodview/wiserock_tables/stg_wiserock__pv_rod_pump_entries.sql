{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPUMPRODENTRY') }}
        qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)
),

renamed as (
    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,

        -- Date fields
        dttm,

        -- Operational measurements (metric / raw - no conversions for WiseRock)
        spm,
        strokelength,
        usernum1,
        volperdaycalc,

        -- Comments and user fields
        com,
        usertxt1,
        usertxt2,
        usertxt3,
        usernum2,
        usernum3,

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