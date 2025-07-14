{{ config(
    materialized='view',
    tags=['wellview', 'job', 'afe', 'cost', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBAFE') }}
),

final as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- AFE amount calculations
        afeamtcalc,
        afeamtnormcalc,
        afenumber,
        afenumbersupp,
        afecosttypcalc,
        afestatus,
        afesupamtcalc,
        afesupamtnormcalc,
        afetotalcalc,
        afetotalnormcalc,
        afeamtnetcalc,
        afesupamtnetcalc,
        afetotalnetcalc,
        
        -- Project information
        com,
        contactname,
        
        -- Cost calculations
        costforecastcalc,
        costnetforecastcalc,
        costnettotalcalc,
        costnormtotalcalc,
        costnormforecastcalc,
        costtotalcalc,
        costtyp,
        
        -- Date fields
        dttmafe,
        dttmafeclose,
        
        exclude,
        
        -- Final invoice calculations
        finalinvoicetotalcalc,
        finalinvoicetotalnetcalc,
        finalinvoicetotalnormcalc,
        
        -- Project details
        projectname,
        projectrefnumber,
        typ,
        
        -- Variance calculations
        variancefieldcalc,
        variancenormfieldcalc,
        varianceafefinalcalc,
        variancenormafefinalcalc,
        variancefieldfinalcalc,
        variancenormfieldfinalcalc,
        
        -- Working interest (converted to percentage)
        workingint / 0.01 as workingint,
        workingintnote,
        
        -- Net variance calculations
        variancenetafefinalcalc,
        variancenetfieldcalc,
        variancenetfieldfinalcalc,
        
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