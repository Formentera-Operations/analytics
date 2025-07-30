{{ config(
    materialized='view',
    tags=['prodview', 'allocations', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITALLOCMONTHDAY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Allocation Record ID",
        idrecparent as "Allocation Parent Record ID",
        idflownet as "Flow Net ID",
        idrecunit as "Unit ID",
        idrecunittk as "Unit Table",
        idreccomp as "Completion ID",
        idreccomptk as "Completion Table",
        idreccompzone as "Reporting/Contact Interval ID",
        idreccompzonetk as "Reporting/Contact Interval Table",
        
        -- Date/Time information
        dttm as "Allocation Date",
        year as "Allocation Year",
        month as "Allocation Month",
        dayofmonth as "Allocation Day of Month",
        
        -- Operational time (converted to hours)
        durdown / 0.0416666666666667 as "Downtime Hours",
        durop / 0.0416666666666667 as "Operating Time Hours",
        
        -- Gathered volumes (converted to US units)
        volprodgathhcliq / 0.158987294928 as "Gathered HCLiq bbl",
        volprodgathgas / 28.316846592 as "Gathered Gas mcf",
        volprodgathwater / 0.158987294928 as "Gathered Water bbl",
        volprodgathsand / 0.158987294928 as "Gathered Sand bbl",
        
        -- Allocated volumes (converted to US units)
        volprodallochcliq / 0.158987294928 as "Allocated HCLiq bbl",
        volprodallocoil / 0.158987294928 as "Allocated Oil bbl",
        volprodalloccond / 0.158987294928 as "Allocated Condensate bbl",
        volprodallocngl / 0.158987294928 as "Allocated NGL bbl",
        volprodallochcliqgaseq / 28.316846592 as "Allocated Gas Equivalent of HCLiq mcf",
        volprodallocgas / 28.316846592 as "Allocated Gas mcf",
        volprodallocwater / 0.158987294928 as "Allocated Water bbl",
        volprodallocsand / 0.158987294928 as "Allocated Sand bbl",
        
        -- Allocation factors (unitless ratios)
        AllocFactHCLiq AS "Allocation Factor HCLiq",
        AllocFactGas AS "Allocation Factor Gas",
        AllocFactWater AS "Allocation Factor Water",
        AllocFactSand AS "Allocation Factor Sand",
        
        -- New production volumes (converted to US units)
        volnewprodallochcliq / 0.158987294928 as "New Production HCLiq bbl",
        volnewprodallocoil / 0.158987294928 as "New Production Oil bbl",
        volnewprodalloccond / 0.158987294928 as "New Production Condensate bbl",
        volnewprodallocngl / 0.158987294928 as "New Production Ngl bbl",
        volnewprodallochcliqgaseq / 28.316846592 as "New Production Hcliq Gas Equivalent mcf",
        volnewprodallocgas / 28.316846592 as "New Production Gas mcf",
        volnewprodallocwater / 0.158987294928 as "New Production Water bbl",
        volnewprodallocsand / 0.158987294928 as "New Production Sand bbl",

        
        wihcliq / 0.01 as "Working Interest Oil Cond pct",
        wigas / 0.01 as "Working Interest Gas pct",
        wiwater / 0.01 as "Working Interest Water pct",
        wisand / 0.01 as "Working Interest Sand pct",

        -- Net revenue interest (converted to percentages)
        nrihcliq / 0.01 as "Net Revenue Interest Oil Cond pct",
        nrigas / 0.01 as "Net Revenue Interest Gas pct",
        nriwater / 0.01 as "Net Revenue Interest Water pct",
        nrisand / 0.01 as "Net Revenue Interest Sand pct",

        -- Lost production due to downtime (converted to US units)
        vollosthcliq / 0.158987294928 as "Deferred Oil Condensate Production bbl",
        vollostgas / 28.316846592 as "Deferred Gas Production mcf",
        vollostwater / 0.158987294928 as "Deferred Water Production bbl",
        vollostsand / 0.158987294928 as "Deferred Sand Production bbl",

        -- Difference from target (converted to US units)
        voldifftargethcliq / 0.158987294928 as "Difference From Target Hcliq bbl",
        voldifftargetoil / 0.158987294928 as "Difference From Target Oil bbl",
        voldifftargetcond / 0.158987294928 as "Difference From Target Condensate bbl",
        voldifftargetngl / 0.158987294928 as "Difference From Target Ngl bbl",
        voldifftargetgas / 28.316846592 as "Difference From Target Gas mcf",
        voldifftargetwater / 0.158987294928 as "Difference From Target Water bbl",
        voldifftargetsand / 0.158987294928 as "Difference From Target Sand bbl",

        -- Recoverable load/lift - Starting volumes (converted to US units)
        volstartremainrecovhcliq / 0.158987294928 as "Starting Load Oil Condensate bbl",
        volstartremainrecovgas / 28.316846592 as "Starting Lift Gas mcf",
        volstartremainrecovwater / 0.158987294928 as "Starting Load Water bbl",
        volstartremainrecovsand / 0.158987294928 as "Starting Sand bbl",

        -- Recoverable load/lift - Recovered volumes (converted to US units)
        volrecovhcliq / 0.158987294928 as "Recovered Load Oil Condensate bbl",
        volrecovgas / 28.316846592 as "Recovered Lift Gas mcf",
        volrecovwater / 0.158987294928 as "Recovered Load Water bbl",
        volrecovsand / 0.158987294928 as "Recovered Sand bbl",

        -- Recoverable load/lift - Injected volumes (converted to US units)
        volinjectrecovgas / 28.316846592 as "Injected Lift Gas bbl",
        volinjectrecovhcliq / 0.158987294928 as "Injected Load Oil Condensate bbl",
        volinjectrecovwater / 0.158987294928 as "Injected Load Water bbl",
        volinjectrecovsand / 0.158987294928 as "Injected Sand bbl",

        -- Recoverable load/lift - Remaining volumes (converted to US units)
        volremainrecovhcliq / 0.158987294928 as "Remaining Load Oil Condensate bbl",
        volremainrecovgas / 28.316846592 as "Remaining Lift Gas mcf",
        volremainrecovwater / 0.158987294928 as "Remaining Load Water bbl",
        volremainrecovsand / 0.158987294928 as "Remaining Sand bbl",

        -- Opening inventory (converted to US units)
        volstartinvhcliq / 0.158987294928 as "Opening Inventory Oil Condensate bbl",
        volstartinvhcliqgaseq / 28.316846592 as "Opening Inventory Gas Equivalent Oil Cond mcf",
        volstartinvwater / 0.158987294928 as "Opening Inventory Water bbl",
        volstartinvsand / 0.158987294928 as "Opening Inventory Sand bbl",

        -- Closing inventory (converted to US units)
        volendinvhcliq / 0.158987294928 as "Closing Inventory Oil Condensate bbl",
        volendinvhcliqgaseq / 28.316846592 as "Closing Inventory Gas Equiv Oil Condensate mcf",
        volendinvwater / 0.158987294928 as "Closing Inventory Water bbl",
        volendinvsand / 0.158987294928 as "Closing Inventory Sand bbl",

        -- Change in inventory (converted to US units)
        volchginvhcliq / 0.158987294928 as "Change In Inventory Oil Condensate bbl",
        volchginvhcliqgaseq / 28.316846592 as "Change In Inventory Gas Equivalent Oil Cond mcf",
        volchginvwater / 0.158987294928 as "Change In Inventory Water bbl",
        volchginvsand / 0.158987294928 as "Change In Inventory Sand bbl",

        -- Dispositions - Sales (converted to US units)
        voldispsalehcliq / 0.158987294928 as "Disposed Allocated Sales Hcliq bbl",
        voldispsaleoil / 0.158987294928 as "Disposed Allocated Sales Oil bbl",
        voldispsalecond / 0.158987294928 as "Disposed Allocated Sales Condensate bbl",
        voldispsalengl / 0.158987294928 as "Disposed Allocated Sales Ngl bbl",
        voldispsalegas / 28.316846592 as "Disposed Allocated Sales Gas mcf",

        -- Dispositions - Gas uses (converted to US units)
        voldispfuelgas / 28.316846592 as "Disposed Allocated Fuel Gas mcf",
        voldispflaregas / 28.316846592 as "Disposed Allocated Flare Gas mcf",
        voldispincinerategas / 28.316846592 as "Disposed Allocated Incineration Gas mcf",
        voldispventgas / 28.316846592 as "Disposed Allocated Vent Gas mcf",
        voldispinjectgas / 28.316846592 as "Disposed Allocated Injected Gas mcf",
        voldispinjectwater / 0.158987294928 as "Disposed Allocated Injected Water bbl",

        -- Injection well volumes (converted to US units)
        volinjecthcliq / 0.158987294928 as "Injection Well Oil Cond bbl",
        volinjectgas / 28.316846592 as "Injection Well Gas mcf",
        volinjectwater / 0.158987294928 as "Injection Well Water bbl",
        volinjectsand / 0.158987294928 as "Injection Well Sand bbl",

        -- Cumulative production (converted to US units)
        volprodcumhcliq / 0.158987294928 as "Cumulated Hcliq bbl",
        volprodcumoil / 0.158987294928 as "Cumulated Oil bbl",
        volprodcumcond / 0.158987294928 as "Cumulated Condensate bbl",
        volprodcumngl / 0.158987294928 as "Cumulated Ngl bbl",
        volprodcumgas / 28.316846592 as "Cumulated Gas mcf",
        volprodcumwater / 0.158987294928 as "Cumulated Water bbl",
        volprodcumsand / 0.158987294928 as "Cumulated Sand bbl",

        -- Heat content (converted to US units)
        heatprodgath / 1055055852.62 as "Gathered Heat mmbtu",
        factheatgath / 37258.9458078313 as "Gathered Heat Factor btu Per ft3",
        heatprodalloc / 1055055852.62 as "Allocated Heat mmbtu",
        factheatalloc / 37258.9458078313 as "Allocated Heat Factor btu Per ft3",
        heatnewprodalloc / 1055055852.62 as "New Production Heat mmbtu",
        heatdispsale / 1055055852.62 as "Disposed Sales Heat mmbtu",
        heatdispfuel / 1055055852.62 as "Disposed Fuel Heat mmbtu",
        heatdispflare / 1055055852.62 as "Disposed Flare Heat mmbtu",
        heatdispvent / 1055055852.62 as "Disposed Vent Heat mmbtu",
        heatdispincinerate / 1055055852.62 as "Disposed Incinerate Heat mmbtu",

        -- Density (converted to API gravity)
        power(nullif(densityalloc, 0), -1) / 7.07409872233005E-06 + -131.5 as "Allocated Density api",
        power(nullif(densitysale, 0), -1) / 7.07409872233005E-06 + -131.5 as "Sales Density api",

        -- Reference IDs for related records
        idrecmeasmeth as "Last Measurement Method ID",
        idrecmeasmethtk as "Last Measurement Method Table",
        idrecfluidlevel as "Last Fluid Level ID",
        idrecfluidleveltk as "Last Fluid Level Table",
        idrectest as "Last Test ID",
        idrectesttk as "Last Test Table",
        idrecparam as "Last Param ID",
        idrecparamtk as "Last Param Table",
        idrecdowntime as "Downtime ID",
        idrecdowntimetk as "Downtime Table",
        idrecdeferment as "Deferment ID",
        idrecdefermenttk as "Deferment Table",
        idrecgasanalysis as "Gas Analysis ID",
        idrecgasanalysistk as "Gas Analysis Table",
        idrechcliqanalysis as "Hc Liquid Analysis ID",
        idrechcliqanalysistk as "Hc Liquid Analysis Table",
        idrecoilanalysis as "Oil Properties ID",
        idrecoilanalysistk as "Oil Properties Table",
        idrecwateranalysis as "Water Properties ID",
        idrecwateranalysistk as "Water Properties Table",
        idrecstatus as "Status ID",
        idrecstatustk as "Status Table",
        idrecpumpentry as "Last Pump Entry ID",
        idrecpumpentrytk as "Last Pump Entry Table",
        idrecfacility as "Reporting Facility ID",
        idrecfacilitytk as "Reporting Facility Table",
        idreccalcset as "Calc Settings ID",
        idreccalcsettk as "Calc Settings Table",

        -- Other operational metrics
        pumpeff / 0.01 as "Pump Efficiency pct",

        -- System fields
        syscreatedate as "Created At",
        syscreateuser as "Created By",
        sysmoddate as "Last Mod At",
        sysmoduser as "Last Mod By",
        systag as "System Tag",
        syslockdate as "System Lock Date",
        syslockme as "System Lock Me",
        syslockchildren as "System Lock Children",
        syslockmeui as "System Lock Me UI",
        syslockchildrenui as "System Lock Children UI",

        -- Fivetran fields
        _fivetran_synced as "Fivetran Synced At"

    from source_data
)

select * from renamed