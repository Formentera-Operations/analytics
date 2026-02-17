{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBAFE') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as job_afe_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as job_id,

        -- afe identification
        trim(afenumber)::varchar as afe_number,
        trim(afenumbersupp)::varchar as supplemental_afe_number,
        trim(afecosttypcalc)::varchar as afe_cost_type,
        trim(afestatus)::varchar as afe_status,

        -- project information
        trim(projectname)::varchar as project_name,
        trim(projectrefnumber)::varchar as project_reference_number,
        trim(contactname)::varchar as contact_name,
        trim(typ)::varchar as afe_type,
        trim(costtyp)::varchar as cost_type,

        -- dates
        dttmafe::timestamp_ntz as afe_date,
        dttmafeclose::timestamp_ntz as afe_close_date,

        -- working interest
        trim(workingintnote)::varchar as working_interest_notes,

        -- afe amounts (gross)
        afeamtcalc::float as total_afe_amount,
        afesupamtcalc::float as total_afe_supplemental_amount,
        afetotalcalc::float as total_afe_plus_supplemental_amount,

        -- afe amounts (normalized)
        afeamtnormcalc::float as normalized_total_afe_amount,
        afesupamtnormcalc::float as normalized_total_afe_supplemental_amount,
        afetotalnormcalc::float as normalized_total_afe_plus_supplemental_amount,

        -- afe amounts (net)
        afeamtnetcalc::float as net_total_afe_amount,
        afesupamtnetcalc::float as net_total_afe_supplemental_amount,
        afetotalnetcalc::float as net_total_afe_plus_supplemental_amount,

        -- field estimates
        costtotalcalc::float as total_field_estimate,
        costnormtotalcalc::float as normalized_total_field_estimate,
        costnettotalcalc::float as net_total_field_estimate,

        -- forecasts
        costforecastcalc::float as forecast_amount,
        costnormforecastcalc::float as normalized_forecast_amount,
        costnetforecastcalc::float as net_forecast_amount,

        -- final invoices
        finalinvoicetotalcalc::float as total_final_invoice,
        finalinvoicetotalnormcalc::float as normalized_total_final_invoice,
        finalinvoicetotalnetcalc::float as net_total_final_invoice,

        -- variances - afe vs field
        variancefieldcalc::float as afe_minus_field_estimate_variance,
        variancenormfieldcalc::float as normalized_afe_minus_field_estimate_variance,
        variancenetfieldcalc::float as net_afe_minus_field_estimate_variance,

        -- variances - afe vs final
        varianceafefinalcalc::float as afe_minus_final_invoice_variance,
        variancenormafefinalcalc::float as normalized_afe_minus_final_invoice_variance,
        variancenetafefinalcalc::float as net_afe_minus_final_invoice_variance,

        -- variances - field vs final
        variancefieldfinalcalc::float as field_minus_final_invoice_variance,
        variancenormfieldfinalcalc::float as normalized_field_minus_final_invoice_variance,
        variancenetfieldfinalcalc::float as net_field_minus_final_invoice_variance,

        -- comments
        trim(com)::varchar as comments,

        -- sequence
        sysseq::int as sequence_number,

        -- system locking fields
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

        -- calculated fields
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- ingestion metadata
        workingint / 0.01 as working_interest_percent,
        coalesce(exclude = 1, false) as exclude_from_cost_calculations

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and job_afe_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['job_afe_id']) }} as job_afe_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        job_afe_sk,

        -- identifiers
        job_afe_id,
        well_id,
        job_id,

        -- afe identification
        afe_number,
        supplemental_afe_number,
        afe_cost_type,
        afe_status,

        -- project information
        project_name,
        project_reference_number,
        contact_name,
        afe_type,
        cost_type,

        -- dates
        afe_date,
        afe_close_date,

        -- working interest
        working_interest_percent,
        working_interest_notes,

        -- flags
        exclude_from_cost_calculations,

        -- afe amounts (gross)
        total_afe_amount,
        total_afe_supplemental_amount,
        total_afe_plus_supplemental_amount,

        -- afe amounts (normalized)
        normalized_total_afe_amount,
        normalized_total_afe_supplemental_amount,
        normalized_total_afe_plus_supplemental_amount,

        -- afe amounts (net)
        net_total_afe_amount,
        net_total_afe_supplemental_amount,
        net_total_afe_plus_supplemental_amount,

        -- field estimates
        total_field_estimate,
        normalized_total_field_estimate,
        net_total_field_estimate,

        -- forecasts
        forecast_amount,
        normalized_forecast_amount,
        net_forecast_amount,

        -- final invoices
        total_final_invoice,
        normalized_total_final_invoice,
        net_total_final_invoice,

        -- variances - afe vs field
        afe_minus_field_estimate_variance,
        normalized_afe_minus_field_estimate_variance,
        net_afe_minus_field_estimate_variance,

        -- variances - afe vs final
        afe_minus_final_invoice_variance,
        normalized_afe_minus_final_invoice_variance,
        net_afe_minus_final_invoice_variance,

        -- variances - field vs final
        field_minus_final_invoice_variance,
        normalized_field_minus_final_invoice_variance,
        net_field_minus_final_invoice_variance,

        -- comments
        comments,

        -- sequence
        sequence_number,

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
