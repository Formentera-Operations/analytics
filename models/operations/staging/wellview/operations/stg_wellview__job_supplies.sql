{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBSUPPLY') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as job_supply_id,
        trim(idrecparent)::varchar as job_id,
        trim(idwell)::varchar as well_id,
        sysseq::int as sequence_number,

        -- supply description
        trim(des)::varchar as supply_item_description,
        trim(typ)::varchar as supply_type,
        trim(note)::varchar as supply_notes,

        -- unit information
        trim(unitlabel)::varchar as unit_label,
        unitsz::float as unit_size,

        -- environmental and energy information
        trim(environmenttyp)::varchar as environmental_type,
        energyfactor::float as energy_factor_joules,

        -- vendor information
        trim(vendor)::varchar as vendor_name,
        trim(vendorcode)::varchar as vendor_code,
        trim(vendorsubcode)::varchar as vendor_subcode,

        -- cost information
        cost::float as unit_cost,
        costcalc::float as total_field_estimate_cost,

        -- cost coding system
        trim(codedes)::varchar as cost_code_description,
        trim(code1)::varchar as cost_code_1,
        trim(code2)::varchar as cost_code_2,
        trim(code3)::varchar as cost_code_3,
        trim(code4)::varchar as cost_code_4,
        trim(code5)::varchar as cost_code_5,
        trim(code6)::varchar as cost_code_6,

        -- quantity tracking
        consumedesign::float as planned_consumed_amount,
        receivedcalc::float as total_received_quantity,
        consumedcalc::float as total_consumed_quantity,
        returnedcalc::float as total_returned_quantity,
        inventorycalc::float as inventory_on_location,
        consumedesignvarcalc::float as planned_vs_actual_consumed_variance,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
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

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and job_supply_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['job_supply_id']) }} as job_supply_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        job_supply_sk,

        -- identifiers
        job_supply_id,
        job_id,
        well_id,
        sequence_number,

        -- supply description
        supply_item_description,
        supply_type,
        supply_notes,

        -- unit information
        unit_label,
        unit_size,

        -- environmental and energy information
        environmental_type,
        energy_factor_joules,

        -- vendor information
        vendor_name,
        vendor_code,
        vendor_subcode,

        -- cost information
        unit_cost,
        total_field_estimate_cost,

        -- cost coding system
        cost_code_description,
        cost_code_1,
        cost_code_2,
        cost_code_3,
        cost_code_4,
        cost_code_5,
        cost_code_6,

        -- quantity tracking
        planned_consumed_amount,
        total_received_quantity,
        total_consumed_quantity,
        total_returned_quantity,
        inventory_on_location,
        planned_vs_actual_consumed_variance,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
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
