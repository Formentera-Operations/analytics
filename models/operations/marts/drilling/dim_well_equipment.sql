{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'dimension']
    )
}}

with well_spine as (
    select
        wellview_id as well_id,
        eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

production_settings_ranked as (
    select
        well_id,
        production_method_type,
        production_method_detail,
        setting_objective,
        setting_result,
        setting_start_date,
        setting_end_date,
        row_number() over (
            partition by well_id
            order by
                coalesce(setting_end_date, '2999-12-31'::timestamp_ntz) desc,
                setting_start_date desc,
                production_setting_id desc
        ) as row_num
    from {{ ref('stg_wellview__production_settings') }}
    where production_method_type is not null or setting_objective is not null
),

production_settings as (
    select
        well_id,
        production_method_type,
        production_method_detail,
        setting_objective,
        setting_result,
        setting_start_date,
        setting_end_date
    from production_settings_ranked
    where row_num = 1
),

tubing_actual as (
    select *
    from {{ ref('stg_wellview__tubing_strings') }}
    where lower(coalesce(proposed_or_actual, 'actual')) = 'actual'
),

tubing_summary as (
    select
        well_id,
        count(*) as tubing_string_count_actual,
        count_if(pull_date is null) as tubing_strings_in_hole,
        max(run_date) as latest_tubing_run_date,
        max(pull_date) as latest_tubing_pull_date,
        max(set_depth_ft) as max_tubing_set_depth_ft,
        max(iff(description ilike '%esp%', 1, 0)) = 1 as has_esp_tubing
    from tubing_actual
    group by well_id
),

rod_actual as (
    select *
    from {{ ref('stg_wellview__rod_strings') }}
    where lower(coalesce(proposed_or_actual, 'actual')) = 'actual'
),

rod_summary as (
    select
        well_id,
        count(*) as rod_string_count_actual,
        count_if(pull_datetime is null) as rod_strings_in_hole,
        max(run_datetime) as latest_rod_run_date,
        max(pull_datetime) as latest_rod_pull_date,
        max(set_depth_ft) as max_rod_set_depth_ft
    from rod_actual
    group by well_id
),

perforation_actual as (
    select *
    from {{ ref('stg_wellview__perforations') }}
    where lower(coalesce(proposed_or_actual, 'actual')) = 'actual'
),

perforation_summary as (
    select
        well_id,
        count(*) as perforation_count,
        sum(coalesce(calculated_shot_total, entered_shot_total, 0)) as perforation_shot_count,
        min(top_depth_ft) as perforation_top_depth_ft,
        max(bottom_depth_ft) as perforation_bottom_depth_ft,
        max(perforation_date) as latest_perforation_date
    from perforation_actual
    group by well_id
),

classified as (
    select
        {{ dbt_utils.generate_surrogate_key(['ws.well_id']) }} as well_equipment_sk,
        ws.eid,
        ws.well_id,

        -- source signals
        ps.production_method_type,
        ps.production_method_detail,
        ps.setting_objective,
        ps.setting_result,
        ps.setting_start_date,
        ps.setting_end_date,

        -- string coverage
        coalesce(ts.tubing_string_count_actual, 0) as tubing_string_count_actual,
        coalesce(ts.tubing_strings_in_hole, 0) as tubing_strings_in_hole,
        coalesce(rs.rod_string_count_actual, 0) as rod_string_count_actual,
        coalesce(rs.rod_strings_in_hole, 0) as rod_strings_in_hole,
        coalesce(ts.has_esp_tubing, false) as has_esp_tubing,

        -- timing/depth snapshots
        ts.latest_tubing_run_date,
        ts.latest_tubing_pull_date,
        ts.max_tubing_set_depth_ft,
        rs.latest_rod_run_date,
        rs.latest_rod_pull_date,
        rs.max_rod_set_depth_ft,

        -- perforation snapshot
        coalesce(pf.perforation_count, 0) as perforation_count,
        coalesce(pf.perforation_shot_count, 0) as perforation_shot_count,
        pf.perforation_top_depth_ft,
        pf.perforation_bottom_depth_ft,
        pf.latest_perforation_date,

        -- lift-type candidates in priority order
        case
            when ps.production_method_type ilike '%rodpump%' then 'Rod Pump'
            when ps.production_method_type ilike '%esp%' then 'ESP'
            when ps.production_method_type ilike '%gaslift%' then 'Gas Lift'
            when ps.production_method_type ilike '%plunger%' then 'Plunger'
            when ps.production_method_type ilike '%flow%' then 'Flowing'
        end as lift_type_from_production_method,

        case
            when coalesce(rs.rod_strings_in_hole, 0) > 0 then 'Rod Pump'
            when coalesce(ts.has_esp_tubing, false) then 'ESP'
            when coalesce(ts.tubing_strings_in_hole, 0) > 0 then 'Non-Rod (Flowing/Other)'
        end as lift_type_from_equipment,

        case
            when ps.setting_objective ilike '%esp%' then 'ESP'
            when ps.setting_objective ilike '%gas lift%' then 'Gas Lift'
            when ps.setting_objective ilike '%gaslift%' then 'Gas Lift'
            when ps.setting_objective ilike '%plunger%' then 'Plunger'
            when ps.setting_objective ilike '%rod%' then 'Rod Pump'
            when ps.setting_objective ilike '%flow%' then 'Flowing'
        end as lift_type_from_setting_objective

    from well_spine as ws
    left join production_settings as ps
        on ws.well_id = ps.well_id
    left join tubing_summary as ts
        on ws.well_id = ts.well_id
    left join rod_summary as rs
        on ws.well_id = rs.well_id
    left join perforation_summary as pf
        on ws.well_id = pf.well_id
),

final as (
    select
        well_equipment_sk,
        eid,
        well_id,
        production_method_type,
        production_method_detail,
        setting_objective,
        setting_result,
        setting_start_date,
        setting_end_date,
        tubing_string_count_actual,
        tubing_strings_in_hole,
        rod_string_count_actual,
        rod_strings_in_hole,
        has_esp_tubing,
        latest_tubing_run_date,
        latest_tubing_pull_date,
        max_tubing_set_depth_ft,
        latest_rod_run_date,
        latest_rod_pull_date,
        max_rod_set_depth_ft,
        perforation_count,
        perforation_shot_count,
        perforation_top_depth_ft,
        perforation_bottom_depth_ft,
        latest_perforation_date,
        lift_type_from_production_method,
        lift_type_from_equipment,
        lift_type_from_setting_objective,
        coalesce(
            lift_type_from_production_method,
            lift_type_from_equipment,
            lift_type_from_setting_objective,
            'Unknown'
        ) as inferred_lift_type,
        case
            when lift_type_from_production_method is not null then 'production_method_type'
            when lift_type_from_equipment is not null then 'equipment_inference'
            when lift_type_from_setting_objective is not null then 'setting_objective'
            else 'none'
        end as lift_type_source,
        (tubing_string_count_actual > 0) as has_tubing_strings,
        (rod_string_count_actual > 0) as has_rod_strings,
        current_timestamp() as _loaded_at
    from classified
)

select * from final
