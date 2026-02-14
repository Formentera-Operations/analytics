{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'casing_cement']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per cement activity)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVCEMENT') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as record_id,
        trim(idwell)::varchar as well_id,
        trim(idrecstring)::varchar as string_id,
        trim(idrecstringtk)::varchar as string_table_key,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idrecjob)::varchar as job_id,
        trim(idrecjobtk)::varchar as job_table_key,
        trim(idrecjobprogramphasecalc)::varchar as phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as phase_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,

        -- descriptive fields
        trim(proposedoractual)::varchar as proposed_or_actual_run,
        trim(des)::varchar as description,
        trim(cementtyp)::varchar as cement_type,
        trim(cementsubtyp)::varchar as cement_subtype,
        trim(contractor)::varchar as cementing_company,
        trim(contractsupt)::varchar as cementing_supervisor,
        trim(objective)::varchar as cement_objective,
        trim(evalmethod)::varchar as evaluation_method,
        trim(deseval)::varchar as cement_evaluation_results,
        trim(reasoncutpull)::varchar as reason_cut,
        trim(notecutpull)::varchar as cut_pull_note,
        trim(resulttechnical)::varchar as technical_result,
        trim(resulttechnicaldetail)::varchar as tech_result_details,
        trim(resulttechnicalnote)::varchar as tech_result_note,
        trim(com)::varchar as comment,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthbtmcalc') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,
        {{ wv_meters_to_feet('depthcutpull') }} as depth_cut_ft,

        -- cement quantities (kg to sacks — keep inline)
        amtcementtotalcalc / 45.359237 as cement_amount_sacks,

        -- volumes (converted from metric to US units)
        {{ wv_cbm_to_bbl('volpumpedtotalcalc') }} as volume_pumped_bbl,

        -- duration (converted from metric to US units)
        {{ wv_days_to_hours('durcalc') }} as duration_hours,

        -- dates
        dttmstart::timestamp_ntz as cementing_start_date,
        dttmend::timestamp_ntz as cementing_end_date,
        dttmcutpull::timestamp_ntz as cut_date,
        dttmpropcutpull::timestamp_ntz as proposed_cut_date,

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
        and record_id is not null
),

-- 4. ENHANCED: Add surrogate key and _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as cement_activity_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        cement_activity_sk,

        -- identifiers
        record_id,
        well_id,
        string_id,
        string_table_key,
        wellbore_id,
        wellbore_table_key,
        job_id,
        job_table_key,
        phase_id,
        phase_table_key,
        last_rig_id,
        last_rig_table_key,
        last_failure_id,
        last_failure_table_key,

        -- descriptive fields
        proposed_or_actual_run,
        description,
        cement_type,
        cement_subtype,
        cementing_company,
        cementing_supervisor,
        cement_objective,
        evaluation_method,
        cement_evaluation_results,
        reason_cut,
        cut_pull_note,
        technical_result,
        tech_result_details,
        tech_result_note,
        comment,

        -- depths
        top_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_ft,
        bottom_depth_tvd_ft,
        depth_cut_ft,

        -- measurements
        cement_amount_sacks,
        volume_pumped_bbl,
        duration_hours,

        -- dates
        cementing_start_date,
        cementing_end_date,
        cut_date,
        proposed_cut_date,

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
