

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITDISPMONTHDAY') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        
        -- Time period (daily granularity)
        DTTM as dttm,
        YEAR as year,
        MONTH as month,
        DAYOFMONTH as day_of_month,
        
        -- Unit and completion references
        IDRECUNIT as id_rec_unit,
        IDRECUNITTK as id_rec_unit_tk,
        IDRECCOMP as id_rec_comp,
        IDRECCOMPTK as id_rec_comp_tk,
        IDRECCOMPZONE as id_rec_comp_zone,
        IDRECCOMPZONETK as id_rec_comp_zone_tk,
        
        -- Outlet and disposition references
        IDRECOUTLETSEND as id_rec_outlet_send,
        IDRECOUTLETSENDTK as id_rec_outlet_send_tk,
        IDRECDISPUNITNODE as id_rec_disp_unit_node,
        IDRECDISPUNITNODETK as id_rec_disp_unit_node_tk,
        IDRECDISPUNIT as id_rec_disp_unit,
        IDRECDISPUNITTK as id_rec_disp_unit_tk,
        
        -- Total fluid volumes
        VOLHCLIQ / 0.158987294928 as vol_hc_liq,
        case when VOLHCLIQ is not null then 'BBL' else null end as vol_hc_liq_unit_label,
        VOLHCLIQGASEQ / 28.316846592 as vol_hc_liq_gas_eq,
        case when VOLHCLIQGASEQ is not null then 'MCF' else null end as vol_hc_liq_gas_eq_unit_label,
        VOLGAS / 28.316846592 as vol_gas,
        case when VOLGAS is not null then 'MCF' else null end as vol_gas_unit_label,
        VOLWATER / 0.158987294928 as vol_water,
        case when VOLWATER is not null then 'BBL' else null end as vol_water_unit_label,
        VOLSAND / 0.158987294928 as vol_sand,
        case when VOLSAND is not null then 'BBL' else null end as vol_sand_unit_label,
        
        -- C1 (Methane) component volumes
        VOLC1LIQ / 0.158987294928 as vol_c1_liq,
        case when VOLC1LIQ is not null then 'BBL' else null end as vol_c1_liq_unit_label,
        VOLC1GASEQ / 28.316846592 as vol_c1_gas_eq,
        case when VOLC1GASEQ is not null then 'MCF' else null end as vol_c1_gas_eq_unit_label,
        VOLC1GAS / 28.316846592 as vol_c1_gas,
        case when VOLC1GAS is not null then 'MCF' else null end as vol_c1_gas_unit_label,
        
        -- C2 (Ethane) component volumes
        VOLC2LIQ / 0.158987294928 as vol_c2_liq,
        case when VOLC2LIQ is not null then 'BBL' else null end as vol_c2_liq_unit_label,
        VOLC2GASEQ / 28.316846592 as vol_c2_gas_eq,
        case when VOLC2GASEQ is not null then 'MCF' else null end as vol_c2_gas_eq_unit_label,
        VOLC2GAS / 28.316846592 as vol_c2_gas,
        case when VOLC2GAS is not null then 'MCF' else null end as vol_c2_gas_unit_label,
        
        -- C3 (Propane) component volumes
        VOLC3LIQ / 0.158987294928 as vol_c3_liq,
        case when VOLC3LIQ is not null then 'BBL' else null end as vol_c3_liq_unit_label,
        VOLC3GASEQ / 28.316846592 as vol_c3_gas_eq,
        case when VOLC3GASEQ is not null then 'MCF' else null end as vol_c3_gas_eq_unit_label,
        VOLC3GAS / 28.316846592 as vol_c3_gas,
        case when VOLC3GAS is not null then 'MCF' else null end as vol_c3_gas_unit_label,
        
        -- iC4 (Iso-butane) component volumes
        VOLIC4LIQ / 0.158987294928 as vol_ic4_liq,
        case when VOLIC4LIQ is not null then 'BBL' else null end as vol_ic4_liq_unit_label,
        VOLIC4GASEQ / 28.316846592 as vol_ic4_gas_eq,
        case when VOLIC4GASEQ is not null then 'MCF' else null end as vol_ic4_gas_eq_unit_label,
        VOLIC4GAS / 28.316846592 as vol_ic4_gas,
        case when VOLIC4GAS is not null then 'MCF' else null end as vol_ic4_gas_unit_label,
        
        -- nC4 (Normal butane) component volumes
        VOLNC4LIQ / 0.158987294928 as vol_nc4_liq,
        case when VOLNC4LIQ is not null then 'BBL' else null end as vol_nc4_liq_unit_label,
        VOLNC4GASEQ / 28.316846592 as vol_nc4_gas_eq,
        case when VOLNC4GASEQ is not null then 'MCF' else null end as vol_nc4_gas_eq_unit_label,
        VOLNC4GAS / 28.316846592 as vol_nc4_gas,
        case when VOLNC4GAS is not null then 'MCF' else null end as vol_nc4_gas_unit_label,
        
        -- iC5 (Iso-pentane) component volumes
        VOLIC5LIQ / 0.158987294928 as vol_ic5_liq,
        case when VOLIC5LIQ is not null then 'BBL' else null end as vol_ic5_liq_unit_label,
        VOLIC5GASEQ / 28.316846592 as vol_ic5_gas_eq,
        case when VOLIC5GASEQ is not null then 'MCF' else null end as vol_ic5_gas_eq_unit_label,
        VOLIC5GAS / 28.316846592 as vol_ic5_gas,
        case when VOLIC5GAS is not null then 'MCF' else null end as vol_ic5_gas_unit_label,
        
        -- nC5 (Normal pentane) component volumes
        VOLNC5LIQ / 0.158987294928 as vol_nc5_liq,
        case when VOLNC5LIQ is not null then 'BBL' else null end as vol_nc5_liq_unit_label,
        VOLNC5GASEQ / 28.316846592 as vol_nc5_gas_eq,
        case when VOLNC5GASEQ is not null then 'MCF' else null end as vol_nc5_gas_eq_unit_label,
        VOLNC5GAS / 28.316846592 as vol_nc5_gas,
        case when VOLNC5GAS is not null then 'MCF' else null end as vol_nc5_gas_unit_label,
        
        -- C6 (Hexanes) component volumes
        VOLC6LIQ / 0.158987294928 as vol_c6_liq,
        case when VOLC6LIQ is not null then 'BBL' else null end as vol_c6_liq_unit_label,
        VOLC6GASEQ / 28.316846592 as vol_c6_gas_eq,
        case when VOLC6GASEQ is not null then 'MCF' else null end as vol_c6_gas_eq_unit_label,
        VOLC6GAS / 28.316846592 as vol_c6_gas,
        case when VOLC6GAS is not null then 'MCF' else null end as vol_c6_gas_unit_label,
        
        -- C7+ (Heptanes plus) component volumes
        VOLC7LIQ / 0.158987294928 as vol_c7_liq,
        case when VOLC7LIQ is not null then 'BBL' else null end as vol_c7_liq_unit_label,
        VOLC7GASEQ / 28.316846592 as vol_c7_gas_eq,
        case when VOLC7GASEQ is not null then 'MCF' else null end as vol_c7_gas_eq_unit_label,
        VOLC7GAS / 28.316846592 as vol_c7_gas,
        case when VOLC7GAS is not null then 'MCF' else null end as vol_c7_gas_unit_label,
        
        -- N2 (Nitrogen) component volumes
        VOLN2LIQ / 0.158987294928 as vol_n2_liq,
        case when VOLN2LIQ is not null then 'BBL' else null end as vol_n2_liq_unit_label,
        VOLN2GASEQ / 28.316846592 as vol_n2_gas_eq,
        case when VOLN2GASEQ is not null then 'MCF' else null end as vol_n2_gas_eq_unit_label,
        VOLN2GAS / 28.316846592 as vol_n2_gas,
        case when VOLN2GAS is not null then 'MCF' else null end as vol_n2_gas_unit_label,
        
        -- CO2 (Carbon dioxide) component volumes
        VOLCO2LIQ / 0.158987294928 as vol_co2_liq,
        case when VOLCO2LIQ is not null then 'BBL' else null end as vol_co2_liq_unit_label,
        VOLCO2GASEQ / 28.316846592 as vol_co2_gas_eq,
        case when VOLCO2GASEQ is not null then 'MCF' else null end as vol_co2_gas_eq_unit_label,
        VOLCO2GAS / 28.316846592 as vol_co2_gas,
        case when VOLCO2GAS is not null then 'MCF' else null end as vol_co2_gas_unit_label,
        
        -- H2S (Hydrogen sulfide) component volumes
        VOLH2SLIQ / 0.158987294928 as vol_h2s_liq,
        case when VOLH2SLIQ is not null then 'BBL' else null end as vol_h2s_liq_unit_label,
        VOLH2SGASEQ / 28.316846592 as vol_h2s_gas_eq,
        case when VOLH2SGASEQ is not null then 'MCF' else null end as vol_h2s_gas_eq_unit_label,
        VOLH2SGAS / 28.316846592 as vol_h2s_gas,
        case when VOLH2SGAS is not null then 'MCF' else null end as vol_h2s_gas_unit_label,
        
        -- Other components volumes
        VOLOTHERCOMPLIQ / 0.158987294928 as vol_other_comp_liq,
        case when VOLOTHERCOMPLIQ is not null then 'BBL' else null end as vol_other_comp_liq_unit_label,
        VOLOTHERCOMPGASEQ / 28.316846592 as vol_other_comp_gas_eq,
        case when VOLOTHERCOMPGASEQ is not null then 'MCF' else null end as vol_other_comp_gas_eq_unit_label,
        VOLOTHERCOMPGAS / 28.316846592 as vol_other_comp_gas,
        case when VOLOTHERCOMPGAS is not null then 'MCF' else null end as vol_other_comp_gas_unit_label,
        
        -- Heat content
        HEAT / 1055055852.62 as heat,
        case when HEAT is not null then 'MMBTU' else null end as heat_unit_label,
        
        -- Calculation set reference
        IDRECCALCSET as id_rec_calc_set,
        IDRECCALCSETTK as id_rec_calc_set_tk,
        
        -- System locking fields
        SYSLOCKMEUI as sys_lock_me_ui,
        SYSLOCKCHILDRENUI as sys_lock_children_ui,
        SYSLOCKME as sys_lock_me,
        SYSLOCKCHILDREN as sys_lock_children,
        SYSLOCKDATE as sys_lock_date,
        
        -- System audit fields
        SYSMODDATE as sys_mod_date,
        SYSMODUSER as sys_mod_user,
        SYSCREATEDATE as sys_create_date,
        SYSCREATEUSER as sys_create_user,
        SYSTAG as sys_tag,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as update_date,
        _FIVETRAN_DELETED as deleted

    from source
)

select * from renamed