{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'production_operations']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per failure record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVPROBLEM') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as production_failure_id,
        trim(idwell)::varchar as well_id,
        trim(idrecjob)::varchar as repair_job_id,
        trim(idrecjobtk)::varchar as repair_job_table_key,
        trim(idreczonecompletion)::varchar as zone_completion_id,
        trim(idreczonecompletiontk)::varchar as zone_completion_table_key,
        trim(idrecprodsettingcalc)::varchar as production_setting_id,
        trim(idrecprodsettingcalctk)::varchar as production_setting_table_key,
        trim(idrecwellstatuscalc)::varchar as pre_failure_well_status_id,
        trim(idrecwellstatuscalctk)::varchar as pre_failure_well_status_table_key,

        -- failure classification
        trim(typ)::varchar as failure_type,
        trim(typcategory)::varchar as failure_type_category,
        trim(typdetail)::varchar as failure_type_detail,
        trim(des)::varchar as failure_description,
        trim(failuresystem)::varchar as failure_system,
        trim(failuresystemcategory)::varchar as failure_system_category,
        trim(failuresymptom)::varchar as failure_symptom,

        -- cause
        trim(cause)::varchar as cause_of_failure,
        trim(causecategory)::varchar as cause_category,
        trim(causedetail)::varchar as cause_detail,
        trim(causecom)::varchar as cause_comments,

        -- priority and status
        trim(priority)::varchar as failure_priority,
        trim(status1)::varchar as failure_status,
        trim(status2)::varchar as failure_status_detail,

        -- dates
        dttmstart::timestamp_ntz as failure_date,
        dttmaction::timestamp_ntz as action_date,
        dttmend::timestamp_ntz as resolution_date,

        -- durations (already in days)
        duractiontostartcalc::float as duration_action_to_start_days,
        durendtoactioncalc::float as duration_resolution_to_action_days,
        durendtostartcalc::float as duration_resolution_to_start_days,
        durjobstarttostartcalc::float as duration_job_start_to_failure_days,

        -- pre-failure production rates (converted from metric to US units)
        {{ wv_cbm_per_day_to_bbl_per_day('rateoptimumoil') }} as optimum_oil_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_mcf_per_day('rateoptimumgas') }} as optimum_gas_rate_mcf_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('rateoptimumcond') }} as optimum_condensate_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('rateoptimumwater') }} as optimum_water_rate_bbl_per_day,

        -- post-failure production rates (converted from metric to US units)
        {{ wv_cbm_per_day_to_bbl_per_day('ratefailoil') }} as failure_oil_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_mcf_per_day('ratefailgas') }} as failure_gas_rate_mcf_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('ratefailcond') }} as failure_condensate_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('ratefailwater') }} as failure_water_rate_bbl_per_day,

        -- production impact (converted from metric to US units)
        {{ wv_cbm_per_day_to_bbl_per_day('ratechangeoilcalc') }} as oil_rate_impact_bbl_per_day,
        {{ wv_cbm_per_day_to_mcf_per_day('ratechangegascalc') }} as gas_rate_impact_mcf_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('ratechangecondcalc') }} as condensate_rate_impact_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('ratechangewatercalc') }} as water_rate_impact_bbl_per_day,

        -- estimated reserve losses (converted from metric to US units)
        {{ wv_cbm_to_bbl('estreservelossoil') }} as estimated_oil_reserve_loss_bbl,
        {{ wv_cbm_to_bbl('estreservelossgas') }} as estimated_gas_reserve_loss_bbl,
        {{ wv_cbm_to_bbl('estreservelosscond') }} as estimated_condensate_reserve_loss_bbl,
        {{ wv_cbm_to_bbl('estreservelosswater') }} as estimated_water_reserve_loss_bbl,

        -- cost
        estcost::float as estimated_failure_cost,

        -- resolution details
        trim(actiontaken)::varchar as action_taken,
        trim(reportto)::varchar as reported_to,

        -- failure impact flags (raw source values for enhanced CTE)
        regulatoryissue::float as _raw_regulatory_issue,
        performanceaffect::float as _raw_performance_affect,

        -- comments
        trim(com)::varchar as comments,

        -- user fields
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
        trim(systag)::varchar as system_tag,
        syslockdate::timestamp_ntz as system_lock_date,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

-- 3. FILTERED: Remove soft deletes and null PKs. No transformations.
filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and production_failure_id is not null
),

-- 4. ENHANCED: Add surrogate key, computed flags, _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['production_failure_id']) }} as production_failure_sk,
        *,
        coalesce(_raw_regulatory_issue = 1, false) as is_regulatory_issue,
        coalesce(_raw_performance_affect = 1, false) as is_performance_affected,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        production_failure_sk,

        -- identifiers
        production_failure_id,
        well_id,
        repair_job_id,
        repair_job_table_key,
        zone_completion_id,
        zone_completion_table_key,
        production_setting_id,
        production_setting_table_key,
        pre_failure_well_status_id,
        pre_failure_well_status_table_key,

        -- failure classification
        failure_type,
        failure_type_category,
        failure_type_detail,
        failure_description,
        failure_system,
        failure_system_category,
        failure_symptom,

        -- cause
        cause_of_failure,
        cause_category,
        cause_detail,
        cause_comments,

        -- priority and status
        failure_priority,
        failure_status,
        failure_status_detail,

        -- dates
        failure_date,
        action_date,
        resolution_date,

        -- durations
        duration_action_to_start_days,
        duration_resolution_to_action_days,
        duration_resolution_to_start_days,
        duration_job_start_to_failure_days,

        -- pre-failure production rates
        optimum_oil_rate_bbl_per_day,
        optimum_gas_rate_mcf_per_day,
        optimum_condensate_rate_bbl_per_day,
        optimum_water_rate_bbl_per_day,

        -- post-failure production rates
        failure_oil_rate_bbl_per_day,
        failure_gas_rate_mcf_per_day,
        failure_condensate_rate_bbl_per_day,
        failure_water_rate_bbl_per_day,

        -- production impact
        oil_rate_impact_bbl_per_day,
        gas_rate_impact_mcf_per_day,
        condensate_rate_impact_bbl_per_day,
        water_rate_impact_bbl_per_day,

        -- estimated reserve losses
        estimated_oil_reserve_loss_bbl,
        estimated_gas_reserve_loss_bbl,
        estimated_condensate_reserve_loss_bbl,
        estimated_water_reserve_loss_bbl,

        -- cost
        estimated_failure_cost,

        -- resolution details
        action_taken,
        reported_to,

        -- flags
        is_regulatory_issue,
        is_performance_affected,

        -- comments
        comments,

        -- user fields
        user_number_1,
        user_number_2,
        user_text_1,
        user_text_2,
        user_text_3,

        -- system / audit
        created_at_utc,
        created_by,
        last_mod_at_utc,
        last_mod_by,
        system_tag,
        system_lock_date,
        system_lock_me,
        system_lock_children,
        system_lock_me_ui,
        system_lock_children_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
