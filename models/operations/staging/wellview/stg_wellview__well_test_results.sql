{{ config(
    materialized='view',
    tags=['wellview', 'completion', 'well_test', 'transient', 'results', 'analysis', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLTESTTRANSRESULT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as well_test_result_id,
        idrecparent as well_test_id,
        idwell as well_id,
        sysseq as sequence_number,
        
        -- Analysis metadata
        analysismethod as analysis_method,
        analysissoftware as analysis_software,
        analyst as analyst_name,
        analysiscom as analysis_comments,
        definitive as is_definitive_test,
        
        -- Pressure measurements (converted to PSI)
        presresmpp / 6.894757 as mpp_pressure_psi,
        presresdatum / 6.894757 as datum_pressure_psi,
        
        -- Depth measurements (converted to feet)
        depthmpp / 0.3048 as mpp_depth_ft,
        depthtvdmppcalc / 0.3048 as mpp_depth_tvd_ft,
        investradius / 0.3048 as investigation_radius_ft,
        
        -- Temperature (converted to Fahrenheit)
        tempres / 0.555555555555556 + 32 as reservoir_temperature_f,
        
        -- Permeability (converted to Darcy)
        respermhor / 9.869233e-13 as horizontal_permeability_darcy,
        respermratio as permeability_ratio,
        
        -- Productivity measurements (converted to field units)
        productivityindex / 0.0230591575847658 as productivity_index_bbl_day_psi,
        spicalc / 0.0756534073644655 as specific_productivity_index_bbl_day_ft_psi,
        productivitycoef as productivity_coefficient,
        productivityexp as productivity_exponent,
        
        -- AOF (Absolute Open Flow) - converted to MCF/day
        aof / 28.316846592 as aof_mcf_per_day,
        
        -- Reservoir characterization
        skin as skin_factor,
        mobilityratio as mobility_ratio,
        resboundtyp as reservoir_boundary_type,
        resboundnote as reservoir_boundary_notes,
        
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