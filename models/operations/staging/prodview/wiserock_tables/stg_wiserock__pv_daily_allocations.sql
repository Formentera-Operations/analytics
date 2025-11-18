{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('prodview', 'PVT_PVUNITALLOCMONTHDAY') }}
    qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)

),

renamed as (

    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,
        
        -- Unit and completion references
        idrecunit,
        idrecunittk,
        idreccomp,
        idreccomptk,
        idreccompzone,
        idreccompzonetk,
        
        -- Date and time information
        dttm,
        year,
        month,
        dayofmonth,
        
        -- Duration fields converted to hours
        durdown / 0.0416666666666667 as durdown,  -- Convert days to hours
        durop / 0.0416666666666667 as durop,      -- Convert days to hours
        
        -- Gathered volumes with imperial conversions
        volprodgathhcliq / 0.158987294928 as volprodgathhcliq,     -- Convert m³ to BBL
        volprodgathgas / 28.316846592 as volprodgathgas,           -- Convert m³ to MCF
        volprodgathwater / 0.158987294928 as volprodgathwater,     -- Convert m³ to BBL
        volprodgathsand / 0.158987294928 as volprodgathsand,       -- Convert m³ to BBL
        
        -- Allocated volumes with imperial conversions
        volprodallochcliq / 0.158987294928 as volprodallochcliq,   -- Convert m³ to BBL
        volprodallocoil / 0.158987294928 as volprodallocoil,       -- Convert m³ to BBL
        volprodalloccond / 0.158987294928 as volprodalloccond,     -- Convert m³ to BBL
        volprodallocngl / 0.158987294928 as volprodallocngl,       -- Convert m³ to BBL
        volprodallochcliqgaseq / 28.316846592 as volprodallochcliqgaseq,  -- Convert m³ to MCF
        volprodallocgas / 28.316846592 as volprodallocgas,         -- Convert m³ to MCF
        volprodallocwater / 0.158987294928 as volprodallocwater,   -- Convert m³ to BBL
        volprodallocsand / 0.158987294928 as volprodallocsand,     -- Convert m³ to BBL
        
        -- Allocation factors (keep as-is - M³/M³)
        allocfacthcliq,
        allocfactgas,
        allocfactwater,
        allocfactsand,
        
        -- New production volumes with imperial conversions
        volnewprodallochcliq / 0.158987294928 as volnewprodallochcliq,     -- Convert m³ to BBL
        volnewprodallocoil / 0.158987294928 as volnewprodallocoil,         -- Convert m³ to BBL
        volnewprodalloccond / 0.158987294928 as volnewprodalloccond,       -- Convert m³ to BBL
        volnewprodallocngl / 0.158987294928 as volnewprodallocngl,         -- Convert m³ to BBL
        volnewprodallochcliqgaseq / 28.316846592 as volnewprodallochcliqgaseq,  -- Convert m³ to MCF
        volnewprodallocgas / 28.316846592 as volnewprodallocgas,           -- Convert m³ to MCF
        volnewprodallocwater / 0.158987294928 as volnewprodallocwater,     -- Convert m³ to BBL
        volnewprodallocsand / 0.158987294928 as volnewprodallocsand,       -- Convert m³ to BBL
        
        -- Working interest (convert proportion to percentage)
        wihcliq / 0.01 as wihcliq,      -- Convert to %
        wigas / 0.01 as wigas,          -- Convert to %
        wiwater / 0.01 as wiwater,      -- Convert to %
        wisand / 0.01 as wisand,        -- Convert to %
        
        -- Net revenue interest (convert proportion to percentage)
        nrihcliq / 0.01 as nrihcliq,    -- Convert to %
        nrigas / 0.01 as nrigas,        -- Convert to %
        nriwater / 0.01 as nriwater,    -- Convert to %
        nrisand / 0.01 as nrisand,      -- Convert to %
        
        -- Lost production volumes with imperial conversions
        vollosthcliq / 0.158987294928 as vollosthcliq,     -- Convert m³ to BBL
        vollostgas / 28.316846592 as vollostgas,           -- Convert m³ to MCF
        vollostwater / 0.158987294928 as vollostwater,     -- Convert m³ to BBL
        vollostsand / 0.158987294928 as vollostsand,       -- Convert m³ to BBL
        
        -- Difference from target volumes with imperial conversions
        voldifftargethcliq / 0.158987294928 as voldifftargethcliq,     -- Convert m³ to BBL
        voldifftargetoil / 0.158987294928 as voldifftargetoil,         -- Convert m³ to BBL
        voldifftargetcond / 0.158987294928 as voldifftargetcond,       -- Convert m³ to BBL
        voldifftargetngl / 0.158987294928 as voldifftargetngl,         -- Convert m³ to BBL
        voldifftargetgas / 28.316846592 as voldifftargetgas,           -- Convert m³ to MCF
        voldifftargetwater / 0.158987294928 as voldifftargetwater,     -- Convert m³ to BBL
        voldifftargetsand / 0.158987294928 as voldifftargetsand,       -- Convert m³ to BBL
        
        -- Recoverable volumes with imperial conversions
        volstartremainrecovhcliq / 0.158987294928 as volstartremainrecovhcliq,     -- Convert m³ to BBL
        volstartremainrecovgas / 28.316846592 as volstartremainrecovgas,           -- Convert m³ to MCF
        volstartremainrecovwater / 0.158987294928 as volstartremainrecovwater,     -- Convert m³ to BBL
        volstartremainrecovsand / 0.158987294928 as volstartremainrecovsand,       -- Convert m³ to BBL
        
        volrecovhcliq / 0.158987294928 as volrecovhcliq,       -- Convert m³ to BBL
        volrecovgas / 28.316846592 as volrecovgas,             -- Convert m³ to MCF
        volrecovwater / 0.158987294928 as volrecovwater,       -- Convert m³ to BBL
        volrecovsand / 0.158987294928 as volrecovsand,         -- Convert m³ to BBL
        
        volinjectrecovgas / 28.316846592 as volinjectrecovgas,         -- Convert m³ to MCF
        volinjectrecovhcliq / 0.158987294928 as volinjectrecovhcliq,   -- Convert m³ to BBL
        volinjectrecovwater / 0.158987294928 as volinjectrecovwater,   -- Convert m³ to BBL
        volinjectrecovsand / 0.158987294928 as volinjectrecovsand,     -- Convert m³ to BBL
        
        volremainrecovhcliq / 0.158987294928 as volremainrecovhcliq,   -- Convert m³ to BBL
        volremainrecovgas / 28.316846592 as volremainrecovgas,         -- Convert m³ to MCF
        volremainrecovwater / 0.158987294928 as volremainrecovwater,   -- Convert m³ to BBL
        volremainrecovsand / 0.158987294928 as volremainrecovsand,     -- Convert m³ to BBL
        
        -- Inventory volumes with imperial conversions
        volstartinvhcliq / 0.158987294928 as volstartinvhcliq,         -- Convert m³ to BBL
        volstartinvhcliqgaseq / 28.316846592 as volstartinvhcliqgaseq, -- Convert m³ to MCF
        volstartinvwater / 0.158987294928 as volstartinvwater,         -- Convert m³ to BBL
        volstartinvsand / 0.158987294928 as volstartinvsand,           -- Convert m³ to BBL
        
        volendinvhcliq / 0.158987294928 as volendinvhcliq,             -- Convert m³ to BBL
        volendinvhcliqgaseq / 28.316846592 as volendinvhcliqgaseq,     -- Convert m³ to MCF
        volendinvwater / 0.158987294928 as volendinvwater,             -- Convert m³ to BBL
        volendinvsand / 0.158987294928 as volendinvsand,               -- Convert m³ to BBL
        
        volchginvhcliq / 0.158987294928 as volchginvhcliq,             -- Convert m³ to BBL
        volchginvhcliqgaseq / 28.316846592 as volchginvhcliqgaseq,     -- Convert m³ to MCF
        volchginvwater / 0.158987294928 as volchginvwater,             -- Convert m³ to BBL
        volchginvsand / 0.158987294928 as volchginvsand,               -- Convert m³ to BBL
        
        -- Disposition volumes with imperial conversions
        voldispsalehcliq / 0.158987294928 as voldispsalehcliq,         -- Convert m³ to BBL
        voldispsaleoil / 0.158987294928 as voldispsaleoil,             -- Convert m³ to BBL
        voldispsalecond / 0.158987294928 as voldispsalecond,           -- Convert m³ to BBL
        voldispsalengl / 0.158987294928 as voldispsalengl,             -- Convert m³ to BBL
        voldispsalegas / 28.316846592 as voldispsalegas,               -- Convert m³ to MCF
        voldispfuelgas / 28.316846592 as voldispfuelgas,               -- Convert m³ to MCF
        voldispflaregas / 28.316846592 as voldispflaregas,             -- Convert m³ to MCF
        voldispincinerategas / 28.316846592 as voldispincinerategas,   -- Convert m³ to MCF
        voldispventgas / 28.316846592 as voldispventgas,               -- Convert m³ to MCF
        voldispinjectgas / 28.316846592 as voldispinjectgas,           -- Convert m³ to MCF
        voldispinjectwater / 0.158987294928 as voldispinjectwater,     -- Convert m³ to BBL
        
        -- Injection volumes with imperial conversions
        volinjecthcliq / 0.158987294928 as volinjecthcliq,     -- Convert m³ to BBL
        volinjectgas / 28.316846592 as volinjectgas,           -- Convert m³ to MCF
        volinjectwater / 0.158987294928 as volinjectwater,     -- Convert m³ to BBL
        volinjectsand / 0.158987294928 as volinjectsand,       -- Convert m³ to BBL
        
        -- Cumulative production volumes with imperial conversions
        volprodcumhcliq / 0.158987294928 as volprodcumhcliq,   -- Convert m³ to BBL
        volprodcumoil / 0.158987294928 as volprodcumoil,       -- Convert m³ to BBL
        volprodcumcond / 0.158987294928 as volprodcumcond,     -- Convert m³ to BBL
        volprodcumngl / 0.158987294928 as volprodcumngl,       -- Convert m³ to BBL
        volprodcumgas / 28.316846592 as volprodcumgas,         -- Convert m³ to MCF
        volprodcumwater / 0.158987294928 as volprodcumwater,   -- Convert m³ to BBL
        volprodcumsand / 0.158987294928 as volprodcumsand,     -- Convert m³ to BBL
        
        -- Heat values with imperial conversions
        heatprodgath / 1055055852.62 as heatprodgath,         -- Convert joules to MMBTU
        factheatgath / 37258.9458078313 as factheatgath,      -- Convert J/m³ to BTU/FT³
        heatprodalloc / 1055055852.62 as heatprodalloc,       -- Convert joules to MMBTU
        factheatalloc / 37258.9458078313 as factheatalloc,    -- Convert J/m³ to BTU/FT³
        heatnewprodalloc / 1055055852.62 as heatnewprodalloc, -- Convert joules to MMBTU
        heatdispsale / 1055055852.62 as heatdispsale,         -- Convert joules to MMBTU
        heatdispfuel / 1055055852.62 as heatdispfuel,         -- Convert joules to MMBTU
        heatdispflare / 1055055852.62 as heatdispflare,       -- Convert joules to MMBTU
        heatdispvent / 1055055852.62 as heatdispvent,         -- Convert joules to MMBTU
        heatdispincinerate / 1055055852.62 as heatdispincinerate,  -- Convert joules to MMBTU
        
        -- Density conversion to API gravity
        power(nullif(densityalloc, 0), -1) / 7.07409872233005e-06 - 131.5 as densityalloc,  -- Convert to °API
        power(nullif(densitysale, 0), -1) / 7.07409872233005e-06 - 131.5 as densitysale,    -- Convert to °API
        
        -- Reference ID fields
        idrecmeasmeth,
        idrecmeasmethtk,
        idrecfluidlevel,
        idrecfluidleveltk,
        idrectest,
        idrectesttk,
        idrecparam,
        idrecparamtk,
        idrecdowntime,
        idrecdowntimetk,
        idrecdeferment,
        idrecdefermenttk,
        idrecgasanalysis,
        idrecgasanalysistk,
        idrechcliqanalysis,
        idrechcliqanalysistk,
        idrecoilanalysis,
        idrecoilanalysistk,
        idrecwateranalysis,
        idrecwateranalysistk,
        idrecstatus,
        idrecstatustk,
        idrecpumpentry,
        idrecpumpentrytk,
        idrecfacility,
        idrecfacilitytk,
        
        -- Pump efficiency (convert proportion to percentage)
        pumpeff / 0.01 as pumpeff,    -- Convert to %
        
        -- Calculation settings
        idreccalcset,
        idreccalcsettk,
        
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