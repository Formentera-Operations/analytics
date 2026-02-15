{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per safety incident)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBSAFETYINCIDENT') }}
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
        trim(idrecparent)::varchar as parent_record_id,
        trim(idrecjobcontact)::varchar as job_contact_id,
        trim(idrecjobcontacttk)::varchar as job_contact_table_key,
        trim(idrecjobservicecontract)::varchar as service_contract_id,
        trim(idrecjobservicecontracttk)::varchar as service_contract_table_key,
        trim(idrecjobprogramphasecalc)::varchar as phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as phase_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,

        -- classification
        trim(typ1)::varchar as incident_type,
        trim(typ2)::varchar as incident_subtype,
        trim(category)::varchar as category,
        trim(severity)::varchar as severity,
        trim(potentialseverity)::varchar as potential_severity,
        trim(cause)::varchar as cause,
        trim(opsfunction)::varchar as ops_function,
        trim(affectonline)::varchar as affected_online,
        trim(witnesstyp)::varchar as witness_type,

        -- identification
        trim(incidentid1)::varchar as incident_id_1,
        trim(incidentid2)::varchar as incident_id_2,
        trim(incidentid3)::varchar as incident_id_3,

        -- people
        trim(reportedby)::varchar as reported_by,
        trim(witness)::varchar as witness,
        trim(witnesscontact)::varchar as witness_contact,
        trim(rigcrewnamecalc)::varchar as rig_crew_name,

        -- flags and measures
        reportable::varchar as reportable,
        losttime::float as lost_time_hours,
        estcost::float as estimated_cost,

        -- temporal
        dttm::timestamp_ntz as incident_datetime,
        trim(tour)::varchar as tour,
        reportnocalc::float as report_number,

        -- descriptive
        trim(des)::varchar as description,
        trim(com)::varchar as comment,

        -- user-defined fields
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,
        userboolean1::boolean as user_boolean_1,
        userboolean2::boolean as user_boolean_2,
        userboolean3::boolean as user_boolean_3,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,

        -- system locking
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockdate::timestamp_ntz as system_lock_date,

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

-- 4. ENHANCED: Add surrogate key, is_reportable flag, + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as safety_incident_sk,
        *,
        coalesce(lower(reportable), 'no') = 'yes' as is_reportable,
        coalesce(lost_time_hours, 0) > 0 as is_lost_time_incident,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        safety_incident_sk,

        -- identifiers
        record_id,
        well_id,
        parent_record_id,
        job_contact_id,
        job_contact_table_key,
        service_contract_id,
        service_contract_table_key,
        phase_id,
        phase_table_key,
        last_rig_id,
        last_rig_table_key,

        -- classification
        incident_type,
        incident_subtype,
        category,
        severity,
        potential_severity,
        cause,
        ops_function,
        affected_online,
        witness_type,

        -- identification
        incident_id_1,
        incident_id_2,
        incident_id_3,

        -- people
        reported_by,
        witness,
        witness_contact,
        rig_crew_name,

        -- flags
        is_reportable,
        is_lost_time_incident,
        reportable,

        -- measures
        lost_time_hours,
        estimated_cost,

        -- temporal
        incident_datetime,
        tour,
        report_number,

        -- descriptive
        description,
        comment,

        -- user-defined fields
        user_text_1,
        user_text_2,
        user_text_3,
        user_number_1,
        user_number_2,
        user_number_3,
        user_boolean_1,
        user_boolean_2,
        user_boolean_3,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
        system_tag,

        -- system locking
        system_lock_me,
        system_lock_children,
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_date,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
