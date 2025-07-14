{{ config(
    materialized='view',
    tags=['wellview', 'job-reports', 'daily-costs', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORTCOSTGEN') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- System carry forward flag
        syscarryfwdp,
        
        -- Account coding structure
        code1,
        code2,
        code3,
        code4,
        code5,
        code6,
        
        -- Cost information
        cost,
        costcumcalc,
        
        -- Description
        des,
        
        -- Custom allocation fields
        idrecafecustom,
        idrecafecustomtk,
        idrecintervalproblemcustom,
        idrecintervalproblemcustomtk,
        idrecphasecustom,
        idrecphasecustomtk,
        
        -- Unit conversion: Convert proportion to percentage
        intervalproblempct / 0.01 as intervalproblempct,
        
        -- Additional fields
        note,
        opscategory,
        polineno,
        pono,
        sn,
        status,
        ticketno,
        unschedtyp,
        userboolean1,
        usertxt1,
        
        -- Vendor information
        vendor,
        vendorcode,
        vendorsubcode,
        
        -- Work order
        workorderno,
        
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
        
        -- Fivetran metadata (mapping to match view naming)
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted

    from source_data
)

select * from renamed