{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBSUPPLYAMT') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as supply_amount_id,
        trim(idrecparent)::varchar as job_supply_id,
        trim(idwell)::varchar as well_id,

        -- transaction timing
        dttm::timestamp_ntz as transaction_datetime,
        reportnocalc::int as report_number,

        -- daily transaction amounts
        received::float as daily_received_quantity,
        consumed::float as daily_consumed_quantity,
        returned::float as daily_returned_quantity,

        -- cumulative calculations
        receivedcumcalc::float as cumulative_received_quantity,
        consumedcumcalc::float as cumulative_consumed_quantity,
        returnedcumcalc::float as cumulative_returned_quantity,
        inventorycumcalc::float as cumulative_inventory_on_location,

        -- cost information
        costor::float as cost_override,
        costcalc::float as daily_field_estimate_cost,
        costcumcalc::float as cumulative_field_estimate_cost,

        -- related entities
        trim(idrecjobsupportvessel)::varchar as support_vessel_id,
        trim(idrecjobsupportvesseltk)::varchar as support_vessel_table_key,
        trim(idrecitem)::varchar as linked_item_id,
        trim(idrecitemtk)::varchar as linked_item_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,

        -- additional information
        trim(note)::varchar as transaction_notes,

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
        and supply_amount_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['supply_amount_id']) }} as supply_amount_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        supply_amount_sk,

        -- identifiers
        supply_amount_id,
        job_supply_id,
        well_id,

        -- transaction timing
        transaction_datetime,
        report_number,

        -- daily transaction amounts
        daily_received_quantity,
        daily_consumed_quantity,
        daily_returned_quantity,

        -- cumulative calculations
        cumulative_received_quantity,
        cumulative_consumed_quantity,
        cumulative_returned_quantity,
        cumulative_inventory_on_location,

        -- cost information
        cost_override,
        daily_field_estimate_cost,
        cumulative_field_estimate_cost,

        -- related entities
        support_vessel_id,
        support_vessel_table_key,
        linked_item_id,
        linked_item_table_key,
        last_rig_id,
        last_rig_table_key,

        -- additional information
        transaction_notes,

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
