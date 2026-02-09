{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPUMP') }}
        qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)
),

renamed as (
    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,

        -- Equipment details
        name,
        make,
        model,
        size,
        material,
        typ,
        powerrating,
        controller,
        serialnum,
        engineeringid,
        regulatoryid,
        otherid,
        entryreqperiod,
        com,

        -- Date fields
        dttmstart,
        dttmend,
        dttmhide,

        -- Calculated fields
        daysinholecalc,

        -- Import/Export identifiers
        importid1,
        importtyp1,
        importid2,
        importtyp2,
        exportid1,
        exporttyp1,
        exportid2,
        exporttyp2,

        -- Migration tracking
        keymigrationsource,
        typmigrationsource,

        -- User-defined fields
        usertxt1,
        usertxt2,
        usertxt3,
        usertxt4,
        usertxt5,
        usernum1,
        usernum2,
        usernum3,
        usernum4,
        usernum5,
        userdttm1,
        userdttm2,
        userdttm3,
        userdttm4,
        userdttm5,

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