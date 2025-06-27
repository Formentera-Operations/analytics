

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITDISPMONTHDAY') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        
        -- Time period (daily granularity)
        DTTM,
        YEAR,
        MONTH,
        DAYOFMONTH,
        
        -- Unit and completion references
        IDRECUNIT,
        IDRECUNITTK,
        IDRECCOMP,
        IDRECCOMPTK,
        IDRECCOMPZONE,
        IDRECCOMPZONETK,
        
        -- Outlet and disposition references
        IDRECOUTLETSEND,
        IDRECOUTLETSENDTK,
        IDRECDISPUNITNODE,
        IDRECDISPUNITNODETK,
        IDRECDISPUNIT,
        IDRECDISPUNITTK,
        
        -- Total fluid volumes
        VOLHCLIQ / 0.158987294928 as VOLHCLIQ,
        case when VOLHCLIQ is not null then 'BBL' else null end as VOLHCLIQUNITLABEL,
        VOLHCLIQGASEQ / 28.316846592 as VOLHCLIQGASEQ,
        case when VOLHCLIQGASEQ is not null then 'MCF' else null end as VOLHCLIQGASEQUNITLABEL,
        VOLGAS / 28.316846592 as VOLGAS,
        case when VOLGAS is not null then 'MCF' else null end as VOLGASUNITLABEL,
        VOLWATER / 0.158987294928 as VOLWATER,
        case when VOLWATER is not null then 'BBL' else null end as VOLWATERUNITLABEL,
        VOLSAND / 0.158987294928 as VOLSAND,
        case when VOLSAND is not null then 'BBL' else null end as VOLSANDUNITLABEL,
        
        -- C1 (Methane) component volumes
        VOLC1LIQ / 0.158987294928 as VOLC1LIQ,
        case when VOLC1LIQ is not null then 'BBL' else null end as VOLC1LIQUNITLABEL,
        VOLC1GASEQ / 28.316846592 as VOLC1GASEQ,
        case when VOLC1GASEQ is not null then 'MCF' else null end as VOLC1GASEQUNITLABEL,
        VOLC1GAS / 28.316846592 as VOLC1GAS,
        case when VOLC1GAS is not null then 'MCF' else null end as VOLC1GASUNITLABEL,
        
        -- C2 (Ethane) component volumes
        VOLC2LIQ / 0.158987294928 as VOLC2LIQ,
        case when VOLC2LIQ is not null then 'BBL' else null end as VOLC2LIQUNITLABEL,
        VOLC2GASEQ / 28.316846592 as VOLC2GASEQ,
        case when VOLC2GASEQ is not null then 'MCF' else null end as VOLC2GASEQUNITLABEL,
        VOLC2GAS / 28.316846592 as VOLC2GAS,
        case when VOLC2GAS is not null then 'MCF' else null end as VOLC2GASUNITLABEL,
        
        -- C3 (Propane) component volumes
        VOLC3LIQ / 0.158987294928 as VOLC3LIQ,
        case when VOLC3LIQ is not null then 'BBL' else null end as VOLC3LIQUNITLABEL,
        VOLC3GASEQ / 28.316846592 as VOLC3GASEQ,
        case when VOLC3GASEQ is not null then 'MCF' else null end as VOLC3GASEQUNITLABEL,
        VOLC3GAS / 28.316846592 as VOLC3GAS,
        case when VOLC3GAS is not null then 'MCF' else null end as VOLC3GASUNITLABEL,
        
        -- iC4 (Iso-butane) component volumes
        VOLIC4LIQ / 0.158987294928 as VOLIC4LIQ,
        case when VOLIC4LIQ is not null then 'BBL' else null end as VOLIC4LIQUNITLABEL,
        VOLIC4GASEQ / 28.316846592 as VOLIC4GASEQ,
        case when VOLIC4GASEQ is not null then 'MCF' else null end as VOLIC4GASEQUNITLABEL,
        VOLIC4GAS / 28.316846592 as VOLIC4GAS,
        case when VOLIC4GAS is not null then 'MCF' else null end as VOLIC4GASUNITLABEL,
        
        -- nC4 (Normal butane) component volumes
        VOLNC4LIQ / 0.158987294928 as VOLNC4LIQ,
        case when VOLNC4LIQ is not null then 'BBL' else null end as VOLNC4LIQUNITLABEL,
        VOLNC4GASEQ / 28.316846592 as VOLNC4GASEQ,
        case when VOLNC4GASEQ is not null then 'MCF' else null end as VOLNC4GASEQUNITLABEL,
        VOLNC4GAS / 28.316846592 as VOLNC4GAS,
        case when VOLNC4GAS is not null then 'MCF' else null end as VOLNC4GASUNITLABEL,
        
        -- iC5 (Iso-pentane) component volumes
        VOLIC5LIQ / 0.158987294928 as VOLIC5LIQ,
        case when VOLIC5LIQ is not null then 'BBL' else null end as VOLIC5LIQUNITLABEL,
        VOLIC5GASEQ / 28.316846592 as VOLIC5GASEQ,
        case when VOLIC5GASEQ is not null then 'MCF' else null end as VOLIC5GASEQUNITLABEL,
        VOLIC5GAS / 28.316846592 as VOLIC5GAS,
        case when VOLIC5GAS is not null then 'MCF' else null end as VOLIC5GASUNITLABEL,
        
        -- nC5 (Normal pentane) component volumes
        VOLNC5LIQ / 0.158987294928 as VOLNC5LIQ,
        case when VOLNC5LIQ is not null then 'BBL' else null end as VOLNC5LIQUNITLABEL,
        VOLNC5GASEQ / 28.316846592 as VOLNC5GASEQ,
        case when VOLNC5GASEQ is not null then 'MCF' else null end as VOLNC5GASEQUNITLABEL,
        VOLNC5GAS / 28.316846592 as VOLNC5GAS,
        case when VOLNC5GAS is not null then 'MCF' else null end as VOLNC5GASUNITLABEL,
        
        -- C6 (Hexanes) component volumes
        VOLC6LIQ / 0.158987294928 as VOLC6LIQ,
        case when VOLC6LIQ is not null then 'BBL' else null end as VOLC6LIQUNITLABEL,
        VOLC6GASEQ / 28.316846592 as VOLC6GASEQ,
        case when VOLC6GASEQ is not null then 'MCF' else null end as VOLC6GASEQUNITLABEL,
        VOLC6GAS / 28.316846592 as VOLC6GAS,
        case when VOLC6GAS is not null then 'MCF' else null end as VOLC6GASUNITLABEL,
        
        -- C7+ (Heptanes plus) component volumes
        VOLC7LIQ / 0.158987294928 as VOLC7LIQ,
        case when VOLC7LIQ is not null then 'BBL' else null end as VOLC7LIQUNITLABEL,
        VOLC7GASEQ / 28.316846592 as VOLC7GASEQ,
        case when VOLC7GASEQ is not null then 'MCF' else null end as VOLC7GASEQUNITLABEL,
        VOLC7GAS / 28.316846592 as VOLC7GAS,
        case when VOLC7GAS is not null then 'MCF' else null end as VOLC7GASUNITLABEL,
        
        -- N2 (Nitrogen) component volumes
        VOLN2LIQ / 0.158987294928 as VOLN2LIQ,
        case when VOLN2LIQ is not null then 'BBL' else null end as VOLN2LIQUNITLABEL,
        VOLN2GASEQ / 28.316846592 as VOLN2GASEQ,
        case when VOLN2GASEQ is not null then 'MCF' else null end as VOLN2GASEQUNITLABEL,
        VOLN2GAS / 28.316846592 as VOLN2GAS,
        case when VOLN2GAS is not null then 'MCF' else null end as VOLN2GASUNITLABEL,
        
        -- CO2 (Carbon dioxide) component volumes
        VOLCO2LIQ / 0.158987294928 as VOLCO2LIQ,
        case when VOLCO2LIQ is not null then 'BBL' else null end as VOLCO2LIQUNITLABEL,
        VOLCO2GASEQ / 28.316846592 as VOLCO2GASEQ,
        case when VOLCO2GASEQ is not null then 'MCF' else null end as VOLCO2GASEQUNITLABEL,
        VOLCO2GAS / 28.316846592 as VOLCO2GAS,
        case when VOLCO2GAS is not null then 'MCF' else null end as VOLCO2GASUNITLABEL,
        
        -- H2S (Hydrogen sulfide) component volumes
        VOLH2SLIQ / 0.158987294928 as VOLH2SLIQ,
        case when VOLH2SLIQ is not null then 'BBL' else null end as VOLH2SLIQUNITLABEL,
        VOLH2SGASEQ / 28.316846592 as VOLH2SGASEQ,
        case when VOLH2SGASEQ is not null then 'MCF' else null end as VOLH2SGASEQUNITLABEL,
        VOLH2SGAS / 28.316846592 as VOLH2SGAS,
        case when VOLH2SGAS is not null then 'MCF' else null end as VOLH2SGASUNITLABEL,
        
        -- Other components volumes
        VOLOTHERCOMPLIQ / 0.158987294928 as VOLOTHERCOMPLIQ,
        case when VOLOTHERCOMPLIQ is not null then 'BBL' else null end as VOLOTHERCOMPLIQLIQUNITLABEL,
        VOLOTHERCOMPGASEQ / 28.316846592 as VOLOTHERCOMPGASEQ,
        case when VOLOTHERCOMPGASEQ is not null then 'MCF' else null end as VOLOTHERCOMPGASEQUNITLABEL,
        VOLOTHERCOMPGAS / 28.316846592 as VOLOTHERCOMPGAS,
        case when VOLOTHERCOMPGAS is not null then 'MCF' else null end as VOLOTHERCOMPGASUNITLABEL,
        
        -- Heat content
        HEAT / 1055055852.62 as HEAT,
        case when HEAT is not null then 'MMBTU' else null end as HEATUNITLABEL,
        
        -- Calculation set reference
        IDRECCALCSET,
        IDRECCALCSETTK,
        
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