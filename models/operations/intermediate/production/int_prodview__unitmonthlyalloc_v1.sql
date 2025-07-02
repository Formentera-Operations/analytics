{{
  config(
    materialized='view',
    alias='unitmonthlyalloc_v1'
  )
}}

with pvunitallocmonth as (
    select * from {{ ref('stg_prodview__monthly_allocations') }}
    where DELETED = false
)

select
    -- Allocation factors (dimensionless ratios)
    ALLOCFACTGAS,
    ALLOCFACTHCLIQ,
    ALLOCFACTSAND,
    ALLOCFACTWATER,
    
    -- Time period
    DTTMEND,
    DTTMSTART,
    MONTH,
    YEAR,
    
    -- Duration
    DURDOWN,
    DUROP,
    
    -- Key IDs
    IDREC,
    IDRECPARENT as UNITID,
    IDRECSTATUS as STATUSID,
    
    -- Net revenue interest percentages
    NRIGAS,
    NRIHCLIQ,
    NRISAND,
    NRIWATER,
    
    -- Working interest percentages
    WIGAS,
    WIHCLIQ,
    WISAND,
    WIWATER,
    
    -- System audit fields
    SYSCREATEDATE,
    SYSCREATEUSER,
    SYSMODDATE,
    SYSMODUSER,
    
    -- Inventory change volumes
    VOLCHGINVHCLIQ,
    VOLCHGINVHCLIQGASEQ,
    VOLCHGINVSAND,
    VOLCHGINVWATER,
    
    -- Target difference volumes
    VOLDIFFTARGETCOND,
    VOLDIFFTARGETGAS,
    VOLDIFFTARGETHCLIQ,
    VOLDIFFTARGETNGL,
    VOLDIFFTARGETOIL,
    VOLDIFFTARGETSAND,
    VOLDIFFTARGETWATER,
    
    -- Disposition volumes
    VOLDISPFLAREGAS,
    VOLDISPFUELGAS,
    VOLDISPINCINERATEGAS,
    VOLDISPINJECTGAS,
    VOLDISPINJECTWATER,
    VOLDISPSALECOND,
    VOLDISPSALEGAS,
    VOLDISPSALEHCLIQ,
    VOLDISPSALENGL,
    VOLDISPSALEOIL,
    VOLDISPVENTGAS,
    
    -- Ending inventory volumes
    VOLENDINVHCLIQ,
    VOLENDINVHCLIQGASEQ,
    VOLENDINVSAND,
    VOLENDINVWATER,
    
    -- Injection volumes
    VOLINJECTGAS,
    VOLINJECTHCLIQ,
    VOLINJECTRECOVGAS,
    VOLINJECTRECOVHCLIQ,
    VOLINJECTRECOVSAND,
    VOLINJECTRECOVWATER,
    VOLINJECTSAND,
    VOLINJECTWATER,
    
    -- Lost volumes
    VOLLOSTGAS,
    VOLLOSTHCLIQ,
    VOLLOSTSAND,
    VOLLOSTWATER,
    
    -- New production allocated volumes
    VOLNEWPRODALLOCCOND,
    VOLNEWPRODALLOCGAS,
    VOLNEWPRODALLOCHCLIQ,
    VOLNEWPRODALLOCHCLIQGASEQ,
    VOLNEWPRODALLOCNGL,
    VOLNEWPRODALLOCOIL,
    VOLNEWPRODALLOCSAND,
    VOLNEWPRODALLOCWATER,
    
    -- Production allocated volumes
    VOLPRODALLOCCOND,
    VOLPRODALLOCGAS,
    VOLPRODALLOCHCLIQ,
    VOLPRODALLOCHCLIQGASEQ,
    VOLPRODALLOCNGL,
    VOLPRODALLOCOIL,
    VOLPRODALLOCSAND,
    VOLPRODALLOCWATER,
    
    -- Cumulative production volumes
    VOLPRODCUMCOND,
    VOLPRODCUMGAS,
    VOLPRODCUMHCLIQ,
    VOLPRODCUMNGL,
    VOLPRODCUMOIL,
    VOLPRODCUMSAND,
    VOLPRODCUMWATER,
    
    -- Production gathered volumes
    VOLPRODGATHGAS,
    VOLPRODGATHHCLIQ,
    VOLPRODGATHSAND,
    VOLPRODGATHWATER,
    
    -- Recovery volumes
    VOLRECOVGAS,
    VOLRECOVHCLIQ,
    VOLRECOVSAND,
    VOLRECOVWATER,
    
    -- Remaining recovery volumes
    VOLREMAINRECOVGAS,
    VOLREMAINRECOVHCLIQ,
    VOLREMAINRECOVSAND,
    VOLREMAINRECOVWATER,
    
    -- Starting inventory volumes
    VOLSTARTINVHCLIQ,
    VOLSTARTINVHCLIQGASEQ,
    VOLSTARTINVSAND,
    VOLSTARTINVWATER,
    
    -- Starting remaining recovery volumes
    VOLSTARTREMAINRECOVGAS,
    VOLSTARTREMAINRECOVHCLIQ,
    VOLSTARTREMAINRECOVSAND,
    VOLSTARTREMAINRECOVWATER,
    
    -- Update tracking
    UPDATE_DATE

from pvunitallocmonth