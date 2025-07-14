{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('prodview', 'PVT_PVUNITCOMP') }}

),

renamed as (

    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,
        
        -- Date fields
        dttmstartalloc,
        dttmend,
        dttmonprod,
        dttmfirstsale,
        dttmflowbackstart,
        dttmflowbackend,
        dttmabandon,
        dttmlastproducedcalc,
        dttmlastproducedhcliqcalc,
        dttmlastproducedgascalc,
        
        -- Completion identifiers
        completionname,
        permanentid,
        compidregulatory,
        compidpa,
        completionlicensee,
        completionlicenseno,
        dttmlicense,
        compida,
        compidb,
        compidc,
        compidd,
        completionide,
        completioncode,
        
        -- Well information
        wellname,
        heldbyproductionthreshold,  -- Keep as-is (no conversion needed)
        
        -- Well identifiers
        wellidregulatory,
        wellidpa,
        welllicenseno,
        wellida,
        wellidb,
        wellidc,
        wellidd,
        wellide,
        
        -- Import/Export identifiers
        importid1,
        importtyp1,
        importid2,
        importtyp2,
        exportid1,
        exporttyp1,
        exportid2,
        exporttyp2,
        
        -- Location coordinates
        latitude,
        longitude,
        latlongsource,
        latlongdatum,
        
        -- Entry requirements
        entryreqperiodfluidlevel,
        entryreqperiodparam,
        
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
        
        -- Migration tracking
        keymigrationsource,
        typmigrationsource,
        
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