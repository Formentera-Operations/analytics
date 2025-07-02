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
        depthtop / 0.3048 as top_depth_ft,
        depthbtm / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        
        -- Formation and reservoir information
        formationcalc as formation,
        formationlayercalc as formation_layer,
        reservoircalc as reservoir,
        
        -- Test conditions and equipment
        phasesepmethod as phase_separation_method,
        surfacetestequip as surface_test_equipment,
        volumemethod as volume_measurement_method,
        
        -- Porosity (converted to percentage)
        porosity / 0.01 as porosity_percent,
        porositysource as porosity_source,
        
        -- Load fluid information (converted to barrels)
        loadfluidtyp as load_fluid_type,
        volloadfluid / 0.158987294928 as load_fluid_volume_bbl,
        volloadfluidunrecov / 0.158987294928 as load_fluid_unrecovered_bbl,
        volloadfluidrecovcalc / 0.158987294928 as load_fluid_recovered_bbl,
        volpercentloadfluidrecovcalc / 0.01 as load_fluid_recovery_percent,
        
        -- Total production volumes (converted to appropriate US units)
        -- Oil and condensate in barrels
        volumeoiltotalcalc / 0.158987294928 as total_oil_volume_bbl,
        volumecondtotalcalc / 0.158987294928 as total_condensate_volume_bbl,
        volumewatertotalcalc / 0.158987294928 as total_water_volume_bbl,
        
        -- Gas volume in thousand cubic feet
        volumegastotalcalc / 28.316846592 as total_gas_volume_mcf,
        
        -- Comments
        com as comments,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at
        
    from source_data
)

select * from renamed