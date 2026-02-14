{{ config(
    materialized='view',
    tags=['wellview', 'completion', 'well_test', 'transient', 'testing', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLTESTTRANS') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as well_test_id,
        idwell as well_id,

        -- Test identification and metadata
        dttm as test_date,
        typ as test_type,
        subtyp as test_subtype,
        des as test_description,
        displayflag as display_flag,
        testedby as tested_by,
        producedto as produced_to,

        -- Operational references
        idrecjob as job_id,
        idrecjobtk as job_table_key,
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        idreczonecompletion as completion_zone_id,
        idreczonecompletiontk as completion_zone_table_key,

        -- Test depths (converted to feet)
        formationcalc as formation,
        formationlayercalc as formation_layer,
        reservoircalc as reservoir,
        phasesepmethod as phase_separation_method,

        -- Formation and reservoir information
        surfacetestequip as surface_test_equipment,
        volumemethod as volume_measurement_method,
        porositysource as porosity_source,

        -- Test conditions and equipment
        loadfluidtyp as load_fluid_type,
        com as comments,
        syscreatedate as created_at,

        -- Porosity (converted to percentage)
        syscreateuser as created_by,
        sysmoddate as modified_at,

        -- Load fluid information (converted to barrels)
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,

        -- Total production volumes (converted to appropriate US units)
        -- Oil and condensate in barrels
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,

        -- Gas volume in thousand cubic feet
        depthtop / 0.3048 as top_depth_ft,

        -- Comments
        depthbtm / 0.3048 as bottom_depth_ft,

        -- System fields
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        porosity / 0.01 as porosity_percent,
        volloadfluid / 0.158987294928 as load_fluid_volume_bbl,
        volloadfluidunrecov / 0.158987294928 as load_fluid_unrecovered_bbl,
        volloadfluidrecovcalc / 0.158987294928 as load_fluid_recovered_bbl,
        volpercentloadfluidrecovcalc / 0.01 as load_fluid_recovery_percent,
        volumeoiltotalcalc / 0.158987294928 as total_oil_volume_bbl,
        volumecondtotalcalc / 0.158987294928 as total_condensate_volume_bbl,
        volumewatertotalcalc / 0.158987294928 as total_water_volume_bbl,

        -- Fivetran fields
        volumegastotalcalc / 28.316846592 as total_gas_volume_mcf

    from source_data
)

select * from renamed
