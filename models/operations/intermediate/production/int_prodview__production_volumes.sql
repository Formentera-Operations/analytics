{{
    config(
        materialized='view'
    )
}}

{#
    Compatibility bridge: reads snake_case from staging, outputs quoted aliases
    for fct_eng_volumes and int_fct_well_header. Convert output to snake_case
    when those downstream models are migrated in a future sprint.
#}

with unitalloc as (
    select * from {{ ref('stg_prodview__daily_allocations') }}
),

compdowntime as (
    select * from {{ ref('stg_prodview__completion_downtimes') }}
),

compparam as (
    select * from {{ ref('stg_prodview__completion_parameters') }}
),

unitstatus as (
    select * from {{ ref('stg_prodview__status') }}
),

prod as (
    select
        a.allocated_gas_eq_hcliq_mcf as "Allocated Gas Equivalent of HCLiq mcf",
        a.allocated_gas_mcf as "Allocated Gas mcf",
        a.allocated_hcliq_bbl as "Allocated HCLiq bbl",
        a.allocated_ngl_bbl as "Allocated NGL bbl",
        a.allocated_oil_bbl as "Allocated Oil bbl",
        a.allocated_sand_bbl as "Allocated Sand bbl",
        a.allocated_water_bbl as "Allocated Water bbl",
        a.allocation_day_of_month as "Allocation Day of Month",
        a.alloc_factor_gas as "Allocation Factor Gas",
        a.alloc_factor_hcliq as "Allocation Factor HCLiq",
        a.alloc_factor_sand as "Allocation Factor Sand",
        a.alloc_factor_water as "Allocation Factor Water",
        a.allocation_month as "Allocation Month",
        a.id_rec as "Allocation Record ID",
        a.allocation_year as "Allocation Year",
        p.bottomhole_pressure_psi as "Bottomhole Pressure psi",
        p.bottomhole_temp_f as "Bottomhole Temperature F",
        p.casing_pressure_psi as "Casing Pressure psi",
        a.chg_inv_gas_eq_hcliq_mcf as "Change In Inventory Gas Equivalent Oil Cond mcf",
        a.chg_inv_hcliq_bbl as "Change In Inventory Oil Condensate bbl",
        a.chg_inv_sand_bbl as "Change In Inventory Sand bbl",
        a.chg_inv_water_bbl as "Change In Inventory Water bbl",
        p.choke_size_64ths as "Choke Size 64ths",
        a.closing_inv_gas_eq_hcliq_mcf as "Closing Inventory Gas Equiv Oil Condensate mcf",
        a.closing_inv_hcliq_bbl as "Tank Oil INV.",
        a.closing_inv_sand_bbl as "Closing Inventory Sand bbl",
        a.closing_inv_water_bbl as "Closing Inventory Water bbl",
        a.created_at_utc as "Created At (UTC)",
        a.created_by as "Created By",
        a.cum_condensate_bbl as "Cumulated Condensate bbl",
        a.cum_gas_mcf as "Cumulated Gas mcf",
        a.cum_hcliq_bbl as "Cumulated Hcliq bbl",
        a.cum_ngl_bbl as "Cumulated Ngl bbl",
        a.cum_oil_bbl as "Cumulated Oil bbl",
        a.cum_sand_bbl as "Cumulated Sand bbl",
        a.cum_water_bbl as "Cumulated Water bbl",
        a.deferred_gas_mcf as "Deferred Gas Production mcf",
        a.deferred_hcliq_bbl as "Deferred Oil Condensate Production bbl",
        a.deferred_sand_bbl as "Deferred Sand Production bbl",
        a.deferred_water_bbl as "Deferred Water Production bbl",
        a.diff_target_condensate_bbl as "Difference From Target Condensate bbl",
        a.diff_target_gas_mcf as "Difference From Target Gas mcf",
        a.diff_target_hcliq_bbl as "Difference From Target Hcliq bbl",
        a.diff_target_ngl_bbl as "Difference From Target Ngl bbl",
        a.diff_target_oil_bbl as "Difference From Target Oil bbl",
        a.diff_target_sand_bbl as "Difference From Target Sand bbl",
        a.diff_target_water_bbl as "Difference From Target Water bbl",
        a.disp_flare_gas_mcf as "Disposed Allocated Flare Gas mcf",
        a.disp_fuel_gas_mcf as "Disposed Allocated Fuel Gas mcf",
        a.disp_incineration_gas_mcf as "Disposed Allocated Incineration Gas mcf",
        a.disp_injected_gas_mcf as "Disposed Allocated Injected Gas mcf",
        a.disp_injected_water_bbl as "Disposed Allocated Injected Water bbl",
        a.disp_sales_condensate_bbl as "Disposed Allocated Sales Condensate bbl",
        a.disp_sales_gas_mcf as "Gross Allocated Sales Gas",
        a.disp_sales_hcliq_bbl as "Gross Allocated Sales Oil",
        a.disp_sales_ngl_bbl as "Disposed Allocated Sales Ngl bbl",
        a.disp_sales_oil_bbl as "Disposed Allocated Sales Oil bbl",
        a.disp_vent_gas_mcf as "Disposed Allocated Vent Gas mcf",
        a.downtime_hours as "Down Hours",
        d.downtime_code_2 as "Downtime Code 2",
        d.downtime_code_3 as "Downtime Code 3",
        d.downtime_code_1 as "Downtime Code",
        d.last_day as "Downtime Last Date",
        a.id_rec_downtime as "Downtime Record ID",
        d.first_day as "Downtime Start Date",
        p.dynamic_viscosity_pa_s as "Dynamic Viscosity Pascal Seconds",
        a.gathered_gas_mcf as "Gathered Gas mcf",
        a.gathered_hcliq_bbl as "Gathered HCLiq bbl",
        a.gathered_sand_bbl as "Gathered Sand bbl",
        a.gathered_water_bbl as "Gathered Water bbl",
        p.h2s_daily_reading_ppm as "H2s Daily Reading ppm",
        a.injected_lift_gas_mcf as "Injected Lift Gas bbl",
        a.injected_load_hcliq_bbl as "Injected Load Oil Condensate bbl",
        a.injected_load_water_bbl as "Injected Load Water bbl",
        a.injected_sand_bbl as "Injected Sand bbl",
        p.injection_pressure_psi as "Injection Pressure psi",
        a.injection_well_gas_mcf as "Injection Well Gas mcf",
        a.injection_well_hcliq_bbl as "Injection Well Oil Cond bbl",
        a.injection_well_sand_bbl as "Injection Well Sand bbl",
        a.injection_well_water_bbl as "Injection Well Water bbl",
        p.kinematic_viscosity_in2_per_s as "Kinematic Viscosity In2 Per S",
        a.modified_by as "Last Mod By",
        a.id_rec_param as "Last Completion Parameter Record ID",
        a.id_rec_pump_entry as "Last Pump Entry Record ID",
        a.id_rec_pump_entry_tk as "Last Pump Entry Table",
        a.id_rec_test as "Last Test Record ID",
        p.line_pressure_psi as "Line Pressure psi",
        a.nri_gas_pct as "Net Revenue Interest Gas pct",
        a.nri_hcliq_pct as "Net Revenue Interest Oil Cond pct",
        a.nri_sand_pct as "Net Revenue Interest Sand pct",
        a.nri_water_pct as "Net Revenue Interest Water pct",
        a.new_prod_condensate_bbl as "New Production Condensate bbl",
        a.new_prod_hcliq_gas_eq_mcf as "New Production Hcliq Gas Equivalent mcf",
        a.new_prod_ngl_bbl as "New Production Ngl bbl",
        a.new_prod_oil_bbl as "New Production Oil bbl",
        a.new_prod_sand_bbl as "New Production Sand bbl",
        a.new_prod_water_bbl as "New Production Water bbl",
        a.opening_inv_gas_eq_hcliq_mcf as "Opening Inventory Gas Equivalent Oil Cond mcf",
        a.opening_inv_hcliq_bbl as "Opening Inventory Oil Condensate bbl",
        a.opening_inv_sand_bbl as "Opening Inventory Sand bbl",
        a.opening_inv_water_bbl as "Opening Inventory Water bbl",
        a.operating_time_hours as "Operating Time Hours",
        p.ph_level as "PH Level",
        a.allocation_date as "Prod Date",
        s.id_rec as "Status Record ID",
        s.status as "Prod Status",
        a.pump_efficiency_pct as "Pump Efficiency pct",
        a.recovered_lift_gas_mcf as "Recovered Lift Gas mcf",
        a.recovered_load_hcliq_bbl as "Recovered Load Oil Condensate bbl",
        a.recovered_load_water_bbl as "Recovered Load Water bbl",
        a.recovered_sand_bbl as "Recovered Sand bbl",
        a.remaining_lift_gas_mcf as "Remaining Lift Gas mcf",
        a.remaining_load_hcliq_bbl as "Remaining Load Oil Condensate bbl",
        a.remaining_load_water_bbl as "Remaining Load Water bbl",
        a.remaining_sand_bbl as "Remaining Sand bbl",
        a.id_rec_facility as "Reporting Facility Record ID",
        p.shut_in_casing_pressure_psi as "Shut In Casing Pressure psi",
        p.shut_in_tubing_pressure_psi as "Shut In Tubing Pressure psi",
        a.starting_lift_gas_mcf as "Starting Lift Gas mcf",
        a.starting_load_hcliq_bbl as "Starting Load Oil Condensate bbl",
        a.starting_load_water_bbl as "Starting Load Water bbl",
        a.starting_sand_bbl as "Starting Sand bbl",
        p.tubing_pressure_psi as "Tubing Pressure psi",
        a.id_rec_unit as "Unit Record ID",
        p.wellhead_pressure_psi as "Wellhead Pressure psi",
        p.wellhead_temp_f as "Wellhead Temperature F",
        a.wi_gas_pct as "Working Interest Gas pct",
        a.wi_hcliq_pct as "Working Interest Oil Cond pct",
        a.wi_sand_pct as "Working Interest Sand pct",
        a.wi_water_pct as "Working Interest Water pct",
        coalesce(a.new_prod_hcliq_bbl, 0) + (coalesce(a.new_prod_gas_mcf, 0) / 6) as "Gross Allocated BOE",
        greatest(
            coalesce(a.modified_at_utc, to_timestamp_tz('0000-01-01T00:00:00.000Z')),
            coalesce(d.modified_at_utc, to_timestamp_tz('0000-01-01T00:00:00.000Z')),
            coalesce(p.modified_at_utc, to_timestamp_tz('0000-01-01T00:00:00.000Z')),
            coalesce(s.modified_at_utc, to_timestamp_tz('0000-01-01T00:00:00.000Z'))
        ) as "Last Mod At (UTC)",
        (a.disp_sales_gas_mcf * a.nri_gas_pct) / 100 as "Net Gas Sales",
        (a.new_prod_hcliq_bbl * a.nri_hcliq_pct) / 100 as "Net Oil Prod",
        coalesce(a.new_prod_gas_mcf, 0) as "Gross Allocated WH New Gas",
        coalesce(a.new_prod_hcliq_bbl, 0) as "Gross Allocated WH Oil",
        (
            (coalesce(a.new_prod_gas_mcf, 0) - coalesce(a.diff_target_gas_mcf, 0))
            / 24
        )
        * coalesce(a.downtime_hours, 0)
            as "Volume Lost Target Gas",
        (
            (coalesce(a.new_prod_hcliq_bbl, 0) - coalesce(a.diff_target_hcliq_bbl, 0))
            / 24
        )
        * coalesce(a.downtime_hours, 0)
            as "Volume Lost Target hcliq"
    from unitalloc a
    left join compdowntime d
        on a.id_rec_downtime = d.id_rec
    left join compparam p
        on a.id_rec_param = p.id_rec
    left join unitstatus s
        on a.id_rec_status = s.id_rec
)

select
    *,
    coalesce("Net Oil Prod", 0) + (coalesce("Net Gas Sales", 0) / 6) as "Net 2-Stream Sales BOE",
    (coalesce("Volume Lost Target hcliq", 0) + (coalesce("Volume Lost Target Gas", 0) / 6)) * -1 as "Gross Downtime BOE"
from prod
