{{ config(
    materialized='view',
    tags=['wellview', 'casing', 'components', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVCASCOMP') }}
),

final as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- Component details
        centralizersnotallycalc,
        com,
        compsubtyp,
        connectaltcalc,
        connectcalc,
        
        -- Connection sizes (converted to inches)
        connszbtm / 0.0254 as connszbtm,
        connsztop / 0.0254 as connsztop,
        
        conntgtperfbtm,
        conntgtperftop,
        connthrdbtm,
        connthrdtop,
        cost,
        costunitlabel,
        
        -- Depths (converted to feet)
        depthbtmcalc / 0.3048 as depthbtmcalc,
        depthtopcalc / 0.3048 as depthtopcalc,
        
        conntypbtm,
        conntyptop,
        currentstatuscalc,
        depthtopcorrected / 0.3048 as depthtopcorrected,
        depthtvdbtmcalc / 0.3048 as depthtvdbtmcalc,
        depthtvdtopcalc / 0.3048 as depthtvdtopcalc,
        
        des,
        desjtcalc,
        dttmmanufacture,
        dttmstatuscalc,
        grade,
        heatrating,
        iconname,
        idreclastfailurecalc,
        idreclastfailurecalctk,
        
        -- Inclinations (already in degrees, no conversion needed)
        inclbtmcalc,
        inclmaxcalc,
        incltopcalc,
        
        itemnocalc,
        joints,
        jointstallycalc,
        
        -- Lengths (converted to feet)
        length / 0.3048 as length,
        lengthcumcalc / 0.3048 as lengthcumcalc,
        lengthtallycalc / 0.3048 as lengthtallycalc,
        
        make,
        material,
        model,
        
        -- Pressures (converted to PSI)
        presaxialinner / 6.894757 as presaxialinner,
        presaxialouter / 6.894757 as presaxialouter,
        presburst / 6.894757 as presburst,
        prescollapse / 6.894757 as prescollapse,
        
        refid,
        sn,
        
        -- Sizes (converted to inches)
        szdrift / 0.0254 as szdrift,
        szidnom / 0.0254 as szidnom,
        szodmax / 0.0254 as szodmax,
        szodnom / 0.0254 as szodnom,
        
        -- Forces (converted to 1000LBF)
        tensilemax / 4448.2216152605 as tensilemax,
        
        -- Torque (converted to FT•LB)
        torquemax / 1.3558179483314 as torquemax,
        torquemin / 1.3558179483314 as torquemin,
        
        upsetbtm,
        upsettop,
        usedclass,
        
        -- Volumes (converted to BBL)
        volumedispcalc / 0.158987294928 as volumedispcalc,
        volumedispcumcalc / 0.158987294928 as volumedispcumcalc,
        volumeinternalcalc / 0.158987294928 as volumeinternalcalc,
        
        -- Weights (converted to kips/1000LBF)
        weightcalc / 4448.2216152605 as weightcalc,
        weightcumcalc / 4448.2216152605 as weightcumcalc,
        
        -- Weight per length (converted to LB/FT)
        wtperlength / 1.48816394356955 as wtperlength,
        
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