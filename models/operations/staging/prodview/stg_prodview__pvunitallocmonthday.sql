with source as (
    select * from {{ source('prodview', 'PVT_PVUNITALLOCMONTHDAY') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        
        -- Unit and completion references
        IDRECUNIT,
        IDRECUNITTK,
        IDRECCOMP,
        IDRECCOMPTK,
        IDRECCOMPZONE,
        IDRECCOMPZONETK,
        
        -- Time period (daily granularity)
        DTTM,
        YEAR,
        MONTH,
        DAYOFMONTH,
        
        -- Duration with unit conversion (minutes to hours)
        DURDOWN / 0.0416666666666667 as DURDOWN,
        case when DURDOWN is not null then 'HR' else null end as DURDOWNUNITLABEL,
        DUROP / 0.0416666666666667 as DUROP,
        case when DUROP is not null then 'HR' else null end as DUROPUNITLABEL,
        
        -- Production gathered volumes (cubic meters to barrels/MCF)
        VOLPRODGATHHCLIQ / 0.158987294928 as VOLPRODGATHHCLIQ,
        case when VOLPRODGATHHCLIQ is not null then 'BBL' else null end as VOLPRODGATHHCLIQUNITLABEL,
        VOLPRODGATHGAS / 28.316846592 as VOLPRODGATHGAS,
        case when VOLPRODGATHGAS is not null then 'MCF' else null end as VOLPRODGATHGASUNITLABEL,
        VOLPRODGATHWATER / 0.158987294928 as VOLPRODGATHWATER,
        case when VOLPRODGATHWATER is not null then 'BBL' else null end as VOLPRODGATHWATERUNITLABEL,
        VOLPRODGATHSAND / 0.158987294928 as VOLPRODGATHSAND,
        case when VOLPRODGATHSAND is not null then 'BBL' else null end as VOLPRODGATHSANDUNITLABEL,
        
        -- Production allocated volumes
        VOLPRODALLOCHCLIQ / 0.158987294928 as VOLPRODALLOCHCLIQ,
        case when VOLPRODALLOCHCLIQ is not null then 'BBL' else null end as VOLPRODALLOCHCLIQUNITLABEL,
        VOLPRODALLOCOIL / 0.158987294928 as VOLPRODALLOCOIL,
        case when VOLPRODALLOCOIL is not null then 'BBL' else null end as VOLPRODALLOCOILUNITLABEL,
        VOLPRODALLOCCOND / 0.158987294928 as VOLPRODALLOCCOND,
        case when VOLPRODALLOCCOND is not null then 'BBL' else null end as VOLPRODALLOCCONDUNITLABEL,
        VOLPRODALLOCNGL / 0.158987294928 as VOLPRODALLOCNGL,
        case when VOLPRODALLOCNGL is not null then 'BBL' else null end as VOLPRODALLOCNGLUNITLABEL,
        VOLPRODALLOCHCLIQGASEQ / 28.316846592 as VOLPRODALLOCHCLIQGASEQ,
        case when VOLPRODALLOCHCLIQGASEQ is not null then 'MCF' else null end as VOLPRODALLOCHCLIQGASEQUNITLABEL,
        VOLPRODALLOCGAS / 28.316846592 as VOLPRODALLOCGAS,
        case when VOLPRODALLOCGAS is not null then 'MCF' else null end as VOLPRODALLOCGASUNITLABEL,
        VOLPRODALLOCWATER / 0.158987294928 as VOLPRODALLOCWATER,
        case when VOLPRODALLOCWATER is not null then 'BBL' else null end as VOLPRODALLOCWATERUNITLABEL,
        VOLPRODALLOCSAND / 0.158987294928 as VOLPRODALLOCSAND,
        case when VOLPRODALLOCSAND is not null then 'BBL' else null end as VOLPRODALLOCSANDUNITLABEL,
        
        -- Allocation factors (dimensionless ratios)
        ALLOCFACTHCLIQ,
        case when ALLOCFACTHCLIQ is not null then 'M³/M³' else null end as ALLOCFACTHCLIQUNITLABEL,
        ALLOCFACTGAS,
        case when ALLOCFACTGAS is not null then 'M³/M³' else null end as ALLOCFACTGASUNITLABEL,
        ALLOCFACTWATER,
        case when ALLOCFACTWATER is not null then 'M³/M³' else null end as ALLOCFACTWATERUNITLABEL,
        ALLOCFACTSAND,
        case when ALLOCFACTSAND is not null then 'M³/M³' else null end as ALLOCFACTSANDUNITLABEL,
        
        -- New production allocated volumes
        VOLNEWPRODALLOCHCLIQ / 0.158987294928 as VOLNEWPRODALLOCHCLIQ,
        case when VOLNEWPRODALLOCHCLIQ is not null then 'BBL' else null end as VOLNEWPRODALLOCHCLIQUNITLABEL,
        VOLNEWPRODALLOCOIL / 0.158987294928 as VOLNEWPRODALLOCOIL,
        case when VOLNEWPRODALLOCOIL is not null then 'BBL' else null end as VOLNEWPRODALLOCOILUNITLABEL,
        VOLNEWPRODALLOCCOND / 0.158987294928 as VOLNEWPRODALLOCCOND,
        case when VOLNEWPRODALLOCCOND is not null then 'BBL' else null end as VOLNEWPRODALLOCCONDUNITLABEL,
        VOLNEWPRODALLOCNGL / 0.158987294928 as VOLNEWPRODALLOCNGL,
        case when VOLNEWPRODALLOCNGL is not null then 'BBL' else null end as VOLNEWPRODALLOCNGLUNITLABEL,
        VOLNEWPRODALLOCHCLIQGASEQ / 28.316846592 as VOLNEWPRODALLOCHCLIQGASEQ,
        case when VOLNEWPRODALLOCHCLIQGASEQ is not null then 'MCF' else null end as VOLNEWPRODALLOCHCLIQGASEQUNITLABEL,
        VOLNEWPRODALLOCGAS / 28.316846592 as VOLNEWPRODALLOCGAS,
        case when VOLNEWPRODALLOCGAS is not null then 'MCF' else null end as VOLNEWPRODALLOCGASUNITLABEL,
        VOLNEWPRODALLOCWATER / 0.158987294928 as VOLNEWPRODALLOCWATER,
        case when VOLNEWPRODALLOCWATER is not null then 'BBL' else null end as VOLNEWPRODALLOCWATERUNITLABEL,
        VOLNEWPRODALLOCSAND / 0.158987294928 as VOLNEWPRODALLOCSAND,
        case when VOLNEWPRODALLOCSAND is not null then 'BBL' else null end as VOLNEWPRODALLOCSANDUNITLABEL,
        
        -- Working interest percentages (decimal to percentage)
        WIHCLIQ / 0.01 as WIHCLIQ,
        case when WIHCLIQ is not null then '%' else null end as WIHCLIQUNITLABEL,
        WIGAS / 0.01 as WIGAS,
        case when WIGAS is not null then '%' else null end as WIGASUNITLABEL,
        WIWATER / 0.01 as WIWATER,
        case when WIWATER is not null then '%' else null end as WIWATERUNITLABEL,
        WISAND / 0.01 as WISAND,
        case when WISAND is not null then '%' else null end as WISANDUNITLABEL,
        
        -- Net revenue interest percentages
        NRIHCLIQ / 0.01 as NRIHCLIQ,
        case when NRIHCLIQ is not null then '%' else null end as NRIHCLIQUNITLABEL,
        NRIGAS / 0.01 as NRIGAS,
        case when NRIGAS is not null then '%' else null end as NRIGASUNITLABEL,
        NRIWATER / 0.01 as NRIWATER,
        case when NRIWATER is not null then '%' else null end as NRIWATERUNITLABEL,
        NRISAND / 0.01 as NRISAND,
        case when NRISAND is not null then '%' else null end as NRISANDUNITLABEL,
        
        -- Lost volumes
        VOLLOSTHCLIQ / 0.158987294928 as VOLLOSTHCLIQ,
        case when VOLLOSTHCLIQ is not null then 'BBL' else null end as VOLLOSTHCLIQUNITLABEL,
        VOLLOSTGAS / 28.316846592 as VOLLOSTGAS,
        case when VOLLOSTGAS is not null then 'MCF' else null end as VOLLOSTGASUNITLABEL,
        VOLLOSTWATER / 0.158987294928 as VOLLOSTWATER,
        case when VOLLOSTWATER is not null then 'BBL' else null end as VOLLOSTWATERUNITLABEL,
        VOLLOSTSAND / 0.158987294928 as VOLLOSTSAND,
        case when VOLLOSTSAND is not null then 'BBL' else null end as VOLLOSTSANDUNITLABEL,
        
        -- Target difference volumes
        VOLDIFFTARGETHCLIQ / 0.158987294928 as VOLDIFFTARGETHCLIQ,
        case when VOLDIFFTARGETHCLIQ is not null then 'BBL' else null end as VOLDIFFTARGETHCLIQUNITLABEL,
        VOLDIFFTARGETOIL / 0.158987294928 as VOLDIFFTARGETOIL,
        case when VOLDIFFTARGETOIL is not null then 'BBL' else null end as VOLDIFFTARGETOILUNITLABEL,
        VOLDIFFTARGETCOND / 0.158987294928 as VOLDIFFTARGETCOND,
        case when VOLDIFFTARGETCOND is not null then 'BBL' else null end as VOLDIFFTARGETCONDUNITLABEL,
        VOLDIFFTARGETNGL / 0.158987294928 as VOLDIFFTARGETNGL,
        case when VOLDIFFTARGETNGL is not null then 'BBL' else null end as VOLDIFFTARGETNGLUNITLABEL,
        VOLDIFFTARGETGAS / 28.316846592 as VOLDIFFTARGETGAS,
        case when VOLDIFFTARGETGAS is not null then 'MCF' else null end as VOLDIFFTARGETGASUNITLABEL,
        VOLDIFFTARGETWATER / 0.158987294928 as VOLDIFFTARGETWATER,
        case when VOLDIFFTARGETWATER is not null then 'BBL' else null end as VOLDIFFTARGETWATERUNITLABEL,
        VOLDIFFTARGETSAND / 0.158987294928 as VOLDIFFTARGETSAND,
        case when VOLDIFFTARGETSAND is not null then 'BBL' else null end as VOLDIFFTARGETSANDUNITLABEL,
        
        -- Starting remaining recovery volumes
        VOLSTARTREMAINRECOVHCLIQ / 0.158987294928 as VOLSTARTREMAINRECOVHCLIQ,
        case when VOLSTARTREMAINRECOVHCLIQ is not null then 'BBL' else null end as VOLSTARTREMAINRECOVHCLIQUNITLABEL,
        VOLSTARTREMAINRECOVGAS / 28.316846592 as VOLSTARTREMAINRECOVGAS,
        case when VOLSTARTREMAINRECOVGAS is not null then 'MCF' else null end as VOLSTARTREMAINRECOVGASUNITLABEL,
        VOLSTARTREMAINRECOVWATER / 0.158987294928 as VOLSTARTREMAINRECOVWATER,
        case when VOLSTARTREMAINRECOVWATER is not null then 'BBL' else null end as VOLSTARTREMAINRECOVWATERUNITLABEL,
        VOLSTARTREMAINRECOVSAND / 0.158987294928 as VOLSTARTREMAINRECOVSAND,
        case when VOLSTARTREMAINRECOVSAND is not null then 'BBL' else null end as VOLSTARTREMAINRECOVSANDUNITLABEL,
        
        -- Recovery volumes
        VOLRECOVHCLIQ / 0.158987294928 as VOLRECOVHCLIQ,
        case when VOLRECOVHCLIQ is not null then 'BBL' else null end as VOLRECOVHCLIQUNITLABEL,
        VOLRECOVGAS / 28.316846592 as VOLRECOVGAS,
        case when VOLRECOVGAS is not null then 'MCF' else null end as VOLRECOVGASUNITLABEL,
        VOLRECOVWATER / 0.158987294928 as VOLRECOVWATER,
        case when VOLRECOVWATER is not null then 'BBL' else null end as VOLRECOVWATERUNITLABEL,
        VOLRECOVSAND / 0.158987294928 as VOLRECOVSAND,
        case when VOLRECOVSAND is not null then 'BBL' else null end as VOLRECOVSANDUNITLABEL,
        
        -- Injection recovery volumes
        VOLINJECTRECOVGAS / 28.316846592 as VOLINJECTRECOVGAS,
        case when VOLINJECTRECOVGAS is not null then 'MCF' else null end as VOLINJECTRECOVGASUNITLABEL,
        VOLINJECTRECOVHCLIQ / 0.158987294928 as VOLINJECTRECOVHCLIQ,
        case when VOLINJECTRECOVHCLIQ is not null then 'BBL' else null end as VOLINJECTRECOVHCLIQUNITLABEL,
        VOLINJECTRECOVWATER / 0.158987294928 as VOLINJECTRECOVWATER,
        case when VOLINJECTRECOVWATER is not null then 'BBL' else null end as VOLINJECTRECOVWATERUNITLABEL,
        VOLINJECTRECOVSAND / 0.158987294928 as VOLINJECTRECOVSAND,
        case when VOLINJECTRECOVSAND is not null then 'BBL' else null end as VOLINJECTRECOVSANDUNITLABEL,
        
        -- Remaining recovery volumes
        VOLREMAINRECOVHCLIQ / 0.158987294928 as VOLREMAINRECOVHCLIQ,
        case when VOLREMAINRECOVHCLIQ is not null then 'BBL' else null end as VOLREMAINRECOVHCLIQUNITLABEL,
        VOLREMAINRECOVGAS / 28.316846592 as VOLREMAINRECOVGAS,
        case when VOLREMAINRECOVGAS is not null then 'MCF' else null end as VOLREMAINRECOVGASUNITLABEL,
        VOLREMAINRECOVWATER / 0.158987294928 as VOLREMAINRECOVWATER,
        case when VOLREMAINRECOVWATER is not null then 'BBL' else null end as VOLREMAINRECOVWATERUNITLABEL,
        VOLREMAINRECOVSAND / 0.158987294928 as VOLREMAINRECOVSAND,
        case when VOLREMAINRECOVSAND is not null then 'BBL' else null end as VOLREMAINRECOVSANDUNITLABEL,
        
        -- Starting inventory volumes
        VOLSTARTINVHCLIQ / 0.158987294928 as VOLSTARTINVHCLIQ,
        case when VOLSTARTINVHCLIQ is not null then 'BBL' else null end as VOLSTARTINVHCLIQUNITLABEL,
        VOLSTARTINVHCLIQGASEQ / 28.316846592 as VOLSTARTINVHCLIQGASEQ,
        case when VOLSTARTINVHCLIQGASEQ is not null then 'MCF' else null end as VOLSTARTINVHCLIQGASEQUNITLABEL,
        VOLSTARTINVWATER / 0.158987294928 as VOLSTARTINVWATER,
        case when VOLSTARTINVWATER is not null then 'BBL' else null end as VOLSTARTINVWATERUNITLABEL,
        VOLSTARTINVSAND / 0.158987294928 as VOLSTARTINVSAND,
        case when VOLSTARTINVSAND is not null then 'BBL' else null end as VOLSTARTINVSANDUNITLABEL,
        
        -- Ending inventory volumes
        VOLENDINVHCLIQ / 0.158987294928 as VOLENDINVHCLIQ,
        case when VOLENDINVHCLIQ is not null then 'BBL' else null end as VOLENDINVHCLIQUNITLABEL,
        VOLENDINVHCLIQGASEQ / 28.316846592 as VOLENDINVHCLIQGASEQ,
        case when VOLENDINVHCLIQGASEQ is not null then 'MCF' else null end as VOLENDINVHCLIQGASEQUNITLABEL,
        VOLENDINVWATER / 0.158987294928 as VOLENDINVWATER,
        case when VOLENDINVWATER is not null then 'BBL' else null end as VOLENDINVWATERUNITLABEL,
        VOLENDINVSAND / 0.158987294928 as VOLENDINVSAND,
        case when VOLENDINVSAND is not null then 'BBL' else null end as VOLENDINVSANDUNITLABEL,
        
        -- Inventory change volumes
        VOLCHGINVHCLIQ / 0.158987294928 as VOLCHGINVHCLIQ,
        case when VOLCHGINVHCLIQ is not null then 'BBL' else null end as VOLCHGINVHCLIQUNITLABEL,
        VOLCHGINVHCLIQGASEQ / 28.316846592 as VOLCHGINVHCLIQGASEQ,
        case when VOLCHGINVHCLIQGASEQ is not null then 'MCF' else null end as VOLCHGINVHCLIQGASEQUNITLABEL,
        VOLCHGINVWATER / 0.158987294928 as VOLCHGINVWATER,
        case when VOLCHGINVWATER is not null then 'BBL' else null end as VOLCHGINVWATERUNITLABEL,
        VOLCHGINVSAND / 0.158987294928 as VOLCHGINVSAND,
        case when VOLCHGINVSAND is not null then 'BBL' else null end as VOLCHGINVSANDUNITLABEL,
        
        -- Disposition sale volumes
        VOLDISPSALEHCLIQ / 0.158987294928 as VOLDISPSALEHCLIQ,
        case when VOLDISPSALEHCLIQ is not null then 'BBL' else null end as VOLDISPSALEHCLIQUNITLABEL,
        VOLDISPSALEOIL / 0.158987294928 as VOLDISPSALEOIL,
        case when VOLDISPSALEOIL is not null then 'BBL' else null end as VOLDISPSALEOILUNITLABEL,
        VOLDISPSALECOND / 0.158987294928 as VOLDISPSALECOND,
        case when VOLDISPSALECOND is not null then 'BBL' else null end as VOLDISPSALECONDUNITLABEL,
        VOLDISPSALENGL / 0.158987294928 as VOLDISPSALENGL,
        case when VOLDISPSALENGL is not null then 'BBL' else null end as VOLDISPSALENGLUNITLABEL,
        VOLDISPSALEGAS / 28.316846592 as VOLDISPSALEGAS,
        case when VOLDISPSALEGAS is not null then 'MCF' else null end as VOLDISPSALEGASUNITLABEL,
        
        -- Gas disposition volumes
        VOLDISPFUELGAS / 28.316846592 as VOLDISPFUELGAS,
        case when VOLDISPFUELGAS is not null then 'MCF' else null end as VOLDISPFUELGASUNITLABEL,
        VOLDISPFLAREGAS / 28.316846592 as VOLDISPFLAREGAS,
        case when VOLDISPFLAREGAS is not null then 'MCF' else null end as VOLDISPFLAREGASUNITLABEL,
        VOLDISPINCINERATEGAS / 28.316846592 as VOLDISPINCINERATEGAS,
        case when VOLDISPINCINERATEGAS is not null then 'MCF' else null end as VOLDISPINCINERATEGASUNITLABEL,
        VOLDISPVENTGAS / 28.316846592 as VOLDISPVENTGAS,
        case when VOLDISPVENTGAS is not null then 'MCF' else null end as VOLDISPVENTGASUNITLABEL,
        VOLDISPINJECTGAS / 28.316846592 as VOLDISPINJECTGAS,
        case when VOLDISPINJECTGAS is not null then 'MCF' else null end as VOLDISPINJECTGASUNITLABEL,
        VOLDISPINJECTWATER / 0.158987294928 as VOLDISPINJECTWATER,
        case when VOLDISPINJECTWATER is not null then 'BBL' else null end as VOLDISPINJECTWATERUNITLABEL,
        
        -- Injection volumes
        VOLINJECTHCLIQ / 0.158987294928 as VOLINJECTHCLIQ,
        case when VOLINJECTHCLIQ is not null then 'BBL' else null end as VOLINJECTHCLIQUNITLABEL,
        VOLINJECTGAS / 28.316846592 as VOLINJECTGAS,
        case when VOLINJECTGAS is not null then 'MCF' else null end as VOLINJECTGASUNITLABEL,
        VOLINJECTWATER / 0.158987294928 as VOLINJECTWATER,
        case when VOLINJECTWATER is not null then 'BBL' else null end as VOLINJECTWATERUNITLABEL,
        VOLINJECTSAND / 0.158987294928 as VOLINJECTSAND,
        case when VOLINJECTSAND is not null then 'BBL' else null end as VOLINJECTSANDUNITLABEL,
        
        -- Cumulative production volumes
        VOLPRODCUMHCLIQ / 0.158987294928 as VOLPRODCUMHCLIQ,
        case when VOLPRODCUMHCLIQ is not null then 'BBL' else null end as VOLPRODCUMHCLIQUNITLABEL,
        VOLPRODCUMOIL / 0.158987294928 as VOLPRODCUMOIL,
        case when VOLPRODCUMOIL is not null then 'BBL' else null end as VOLPRODCUMOILUNITLABEL,
        VOLPRODCUMCOND / 0.158987294928 as VOLPRODCUMCOND,
        case when VOLPRODCUMCOND is not null then 'BBL' else null end as VOLPRODCUMCONDUNITLABEL,
        VOLPRODCUMNGL / 0.158987294928 as VOLPRODCUMNGL,
        case when VOLPRODCUMNGL is not null then 'BBL' else null end as VOLPRODCUMNGLUNITLABEL,
        VOLPRODCUMGAS / 28.316846592 as VOLPRODCUMGAS,
        case when VOLPRODCUMGAS is not null then 'MCF' else null end as VOLPRODCUMGASUNITLABEL,
        VOLPRODCUMWATER / 0.158987294928 as VOLPRODCUMWATER,
        case when VOLPRODCUMWATER is not null then 'BBL' else null end as VOLPRODCUMWATERUNITLABEL,
        VOLPRODCUMSAND / 0.158987294928 as VOLPRODCUMSAND,
        case when VOLPRODCUMSAND is not null then 'BBL' else null end as VOLPRODCUMSANDUNITLABEL,
        
        -- Heat content and heating values
        HEATPRODGATH / 1055055852.62 as HEATPRODGATH,
        case when HEATPRODGATH is not null then 'MMBTU' else null end as HEATPRODGATHUNITLABEL,
        FACTHEATGATH / 37258.9458078313 as FACTHEATGATH,
        case when FACTHEATGATH is not null then 'BTU/FT³' else null end as FACTHEATGATHUNITLABEL,
        HEATPRODALLOC / 1055055852.62 as HEATPRODALLOC,
        case when HEATPRODALLOC is not null then 'MMBTU' else null end as HEATPRODALLOCUNITLABEL,
        FACTHEATALLOC / 37258.9458078313 as FACTHEATALLOC,
        case when FACTHEATALLOC is not null then 'BTU/FT³' else null end as FACTHEATALLOCUNITLABEL,
        HEATNEWPRODALLOC / 1055055852.62 as HEATNEWPRODALLOC,
        case when HEATNEWPRODALLOC is not null then 'MMBTU' else null end as HEATNEWPRODALLOCUNITLABEL,
        HEATDISPSALE / 1055055852.62 as HEATDISPSALE,
        case when HEATDISPSALE is not null then 'MMBTU' else null end as HEATDISPSALEUNITLABEL,
        HEATDISPFUEL / 1055055852.62 as HEATDISPFUEL,
        case when HEATDISPFUEL is not null then 'MMBTU' else null end as HEATDISPFUELUNITLABEL,
        HEATDISPFLARE / 1055055852.62 as HEATDISPFLARE,
        case when HEATDISPFLARE is not null then 'MMBTU' else null end as HEATDISPFLAREUNITLABEL,
        HEATDISPVENT / 1055055852.62 as HEATDISPVENT,
        case when HEATDISPVENT is not null then 'MMBTU' else null end as HEATDISPVENTUNITLABEL,
        HEATDISPINCINERATE / 1055055852.62 as HEATDISPINCINERATE,
        case when HEATDISPINCINERATE is not null then 'MMBTU' else null end as HEATDISPINCINERATEUNITLABEL,
        
        -- Density conversions (complex formula to API gravity)
        power(nullif(DENSITYALLOC, 0), -1) / 7.07409872233005E-06 + -131.5 as DENSITYALLOC,
        case when DENSITYALLOC is not null then '°API' else null end as DENSITYALLOCUNITLABEL,
        power(nullif(DENSITYSALE, 0), -1) / 7.07409872233005E-06 + -131.5 as DENSITYSALE,
        case when DENSITYSALE is not null then '°API' else null end as DENSITYSALEUNITLABEL,
        
        -- Reference IDs
        IDRECMEASMETH,
        IDRECMEASMETHTK,
        IDRECFLUIDLEVEL,
        IDRECFLUIDLEVELTK,
        IDRECTEST,
        IDRECTESTTK,
        IDRECPARAM,
        IDRECPARAMTK,
        IDRECDOWNTIME,
        IDRECDOWNTIMETK,
        IDRECDEFERMENT,
        IDRECDEFERMENTTK,
        IDRECGASANALYSIS,
        IDRECGASANALYSISTK,
        IDRECHCLIQANALYSIS,
        IDRECHCLIQANALYSISTK,
        IDRECOILANALYSIS,
        IDRECOILANALYSISTK,
        IDRECWATERANALYSIS,
        IDRECWATERANALYSISTK,
        IDRECSTATUS,
        IDRECSTATUSTK,
        IDRECPUMPENTRY,
        IDRECPUMPENTRYTK,
        IDRECFACILITY,
        IDRECFACILITYTK,
        IDRECCALCSET,
        IDRECCALCSETTK,
        
        -- Pump efficiency (decimal to percentage)
        PUMPEFF / 0.01 as PUMPEFF,
        case when PUMPEFF is not null then '%' else null end as PUMPEFFUNITLABEL,
        
        -- System locking fields
        SYSLOCKMEUI,
        SYSLOCKCHILDRENUI,
        SYSLOCKME,
        SYSLOCKCHILDREN,
        SYSLOCKDATE,
        
        -- System audit fields
        SYSMODDATE,
        SYSMODUSER,
        SYSCREATEDATE,
        SYSCREATEUSER,
        SYSTAG,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as UPDATE_DATE,
        _FIVETRAN_DELETED as DELETED

    from source
)

select * from renamed