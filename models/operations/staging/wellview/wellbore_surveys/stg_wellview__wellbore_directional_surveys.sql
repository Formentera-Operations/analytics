{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'wellbore_surveys']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBOREDIRSURVEY') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as survey_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as wellbore_id,
        trim(idrecjob)::varchar as job_id,
        trim(idrecjobtk)::varchar as job_table_key,

        -- survey metadata
        trim(proposedoractual)::varchar as proposed_or_actual,
        propversionno::int as proposed_version_number,
        dttm::timestamp_ntz as survey_date,
        trim(des)::varchar as description,

        -- survey control flags
        calcflag::boolean as use_for_calculations,
        definitive::boolean as is_definitive,

        -- azimuth information
        trim(azimuthnorthtyp)::varchar as azimuth_north_type,
        declination::float as declination_degrees,
        convergence::float as convergence_degrees,

        -- tie-in coordinates (converted from meters to feet)
        {{ wv_meters_to_feet('mdtiein') }} as md_tie_in_ft,
        inclinationtiein::float as inclination_tie_in_degrees,
        azimuthtiein::float as azimuth_tie_in_degrees,
        {{ wv_meters_to_feet('tvdtiein') }} as tvd_tie_in_ft,
        {{ wv_meters_to_feet('nstiein') }} as ns_tie_in_ft,
        {{ wv_meters_to_feet('ewtiein') }} as ew_tie_in_ft,
        {{ wv_meters_to_feet('vscalc') }} as vs_tie_in_ft,

        -- correction methods
        trim(notecorrection)::varchar as correction_notes,
        trim(depthcorrection)::varchar as depth_correction_method,
        trim(azimuthcorrection)::varchar as azimuth_correction_method,

        -- validation information
        validateddttm::timestamp_ntz as validated_date,
        trim(validatedbyname)::varchar as validated_by_name,
        trim(validatedbycompany)::varchar as validated_by_company,

        -- comments
        trim(com)::varchar as comment,

        -- system locking
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        trim(systag)::varchar as system_tag,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and survey_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['survey_id']) }} as survey_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        survey_sk,

        -- identifiers
        survey_id,
        well_id,
        wellbore_id,
        job_id,
        job_table_key,

        -- survey metadata
        proposed_or_actual,
        proposed_version_number,
        survey_date,
        description,

        -- survey control flags
        use_for_calculations,
        is_definitive,

        -- azimuth information
        azimuth_north_type,
        declination_degrees,
        convergence_degrees,

        -- tie-in coordinates
        md_tie_in_ft,
        inclination_tie_in_degrees,
        azimuth_tie_in_degrees,
        tvd_tie_in_ft,
        ns_tie_in_ft,
        ew_tie_in_ft,
        vs_tie_in_ft,

        -- correction methods
        correction_notes,
        depth_correction_method,
        azimuth_correction_method,

        -- validation information
        validated_date,
        validated_by_name,
        validated_by_company,

        -- comments
        comment,

        -- system locking
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        system_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
