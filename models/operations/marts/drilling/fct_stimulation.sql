{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'fact']
    )
}}

with stimulations as (
    select * from {{ ref('stg_wellview__stimulations') }}
    where
        lower(proposed_or_actual) = 'actual'
        or proposed_or_actual is null
),

well_360 as (
    select
        wellview_id,
        eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

enriched as (
    select
        s.stimulation_sk,

        -- dimensional FKs
        case
            when s.job_id is not null then {{ dbt_utils.generate_surrogate_key(['s.job_id']) }}
        end as job_sk,
        w360.eid,

        -- natural keys
        s.record_id,
        s.well_id,
        s.job_id,
        s.job_table_key,

        -- classification
        s.stimulation_type,
        s.stimulation_subtype,
        s.category,
        s.proposed_or_actual,

        -- temporal
        s.start_date::date as stim_start_date,
        s.start_date,
        s.end_date,

        -- stage metrics (WellView calc rollups)
        s.actual_total_stages,
        s.calculated_stages,
        s.design_number_of_treatment_intervals,
        s.total_number_of_clusters,
        s.clusters_per_stage,
        s.total_number_of_plugs,
        s.stages_per_day,
        s.plugs_per_day,

        -- proppant measures
        s.proppant_total_calc_lb,
        s.mass_proppant_per_stage_lb,
        s.mass_proppant_per_gross_length_lb_per_ft,
        s.mass_proppant_per_net_length_lb_per_ft,

        -- volume measures
        s.volume_clean_total_calc_bbl,
        s.volume_slurry_total_calc_bbl,
        s.volume_recovered_total_calc_bbl,
        s.total_clean_minus_recovered_volume_bbl,

        -- treatment rates
        s.treat_rate_avg_bbl_per_min,
        s.treat_rate_max_bbl_per_min,
        s.slurry_rate_avg_bbl_per_min,
        s.slurry_rate_max_bbl_per_min,

        -- pressures
        s.treat_pressure_avg_all_stages_psi,
        s.treat_pressure_max_all_stages_psi,
        s.breakdown_pressure_avg_psi,
        s.closure_pressure_avg_psi,

        -- pressure gradients
        s.closure_gradient_avg_psi_per_ft,
        s.frac_gradient_avg_psi_per_ft,

        -- concentrations
        s.bh_conc_avg_all_stages_lb_per_gal,
        s.surf_conc_avg_all_stages_lb_per_gal,

        -- depths
        s.min_top_depth_ft,
        s.max_bottom_depth_ft,
        s.length_gross_ft,
        s.length_net_ft,

        -- durations
        s.total_duration_gross_hours,
        s.total_duration_net_hours,
        s.total_pump_duration_calc_hours,

        -- cost
        s.total_cost,
        s.cost_per_stage,
        s.cost_per_cluster,

        -- operational context
        s.stim_treat_company,
        s.stim_treat_supervisor,
        s.diversion_company,
        s.diversion_method,
        s.technical_result,

        -- flags
        s.job_id is not null as has_job_link,

        -- dbt metadata
        s._loaded_at

    from stimulations as s
    left join well_360 as w360
        on s.well_id = w360.wellview_id
),

final as (
    select
        stimulation_sk,
        job_sk,
        eid,
        record_id,
        well_id,
        job_id,
        job_table_key,
        stimulation_type,
        stimulation_subtype,
        category,
        proposed_or_actual,
        stim_start_date,
        start_date,
        end_date,
        actual_total_stages,
        calculated_stages,
        design_number_of_treatment_intervals,
        total_number_of_clusters,
        clusters_per_stage,
        total_number_of_plugs,
        stages_per_day,
        plugs_per_day,
        proppant_total_calc_lb,
        mass_proppant_per_stage_lb,
        mass_proppant_per_gross_length_lb_per_ft,
        mass_proppant_per_net_length_lb_per_ft,
        volume_clean_total_calc_bbl,
        volume_slurry_total_calc_bbl,
        volume_recovered_total_calc_bbl,
        total_clean_minus_recovered_volume_bbl,
        treat_rate_avg_bbl_per_min,
        treat_rate_max_bbl_per_min,
        slurry_rate_avg_bbl_per_min,
        slurry_rate_max_bbl_per_min,
        treat_pressure_avg_all_stages_psi,
        treat_pressure_max_all_stages_psi,
        breakdown_pressure_avg_psi,
        closure_pressure_avg_psi,
        closure_gradient_avg_psi_per_ft,
        frac_gradient_avg_psi_per_ft,
        bh_conc_avg_all_stages_lb_per_gal,
        surf_conc_avg_all_stages_lb_per_gal,
        min_top_depth_ft,
        max_bottom_depth_ft,
        length_gross_ft,
        length_net_ft,
        total_duration_gross_hours,
        total_duration_net_hours,
        total_pump_duration_calc_hours,
        total_cost,
        cost_per_stage,
        cost_per_cluster,
        stim_treat_company,
        stim_treat_supervisor,
        diversion_company,
        diversion_method,
        technical_result,
        has_job_link,
        _loaded_at
    from enriched
)

select * from final
