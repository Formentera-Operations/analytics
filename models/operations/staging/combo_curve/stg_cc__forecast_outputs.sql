{{
    config(
        materialized='view',
        tags=['combo_curve', 'forecasting', 'daily']
    )
}}

with source as (
    
    select * from {{ source('combo_curve', 'forecast_outputs') }}

),

renamed as (
    
    select
        -- Primary Key
        id as forecast_output_id,
        
        -- Foreign Keys
        well as well_id,
        project as project_id,
        forecast as forecast_id,
        typecurve as type_curve_id,
        
        -- Forecast Attributes
        forecasttype as forecast_type,
        forecastsubtype as forecast_subtype,
        phase as product_phase,
        status as forecast_status,
        data_freq as data_frequency,
        
        -- Boolean Flags
        forecasted as is_forecasted,
        
        -- User and Timestamp Fields
        forecastedby as forecasted_by_user,
        forecastedat as forecasted_at,
        reviewedby as reviewed_by_user,
        reviewedat as reviewed_at,
        createdat as created_at,
        updatedat as updated_at,
        "_PORTABLE_EXTRACTED" as extracted_at,
        
        -- Custom Fields
        projectcustomheader9 as project_custom_header_9,
        
        -- JSON/Variant Fields (keeping as variant for now, can be parsed in intermediate models)
        best as best_forecast_params,
        ratio as ratio_params,
        typecurveapplysettings as type_curve_apply_settings,
        typecurvedata as type_curve_data

    from source

),

typed as (
    
    select
        -- IDs
        forecast_output_id,
        well_id,
        project_id,
        forecast_id,
        type_curve_id,
        
        -- Forecast Attributes
        forecast_type,
        forecast_subtype,
        product_phase,
        forecast_status,
        data_frequency,
        
        -- Boolean Flags
        is_forecasted,
        
        -- User Fields (keeping as strings)
        forecasted_by_user,
        reviewed_by_user,
        
        -- Timestamp Fields (ensuring proper timestamp format)
        forecasted_at::timestamp_ntz as forecasted_at,
        reviewed_at::timestamp_ntz as reviewed_at,
        created_at::timestamp_ntz as created_at,
        updated_at::timestamp_ntz as updated_at,
        extracted_at::timestamp_ntz as extracted_at,
        
        -- Custom Fields (handling potential numeric conversion)
        try_cast(project_custom_header_9 as number(38,2)) as project_custom_value_9,
        
        -- JSON/Variant Fields
        best_forecast_params,
        ratio_params,
        type_curve_apply_settings,
        type_curve_data,
        
        -- Audit Fields
        current_timestamp() as dbt_loaded_at

    from renamed

),

final as (
    
    select
        *,
        
        -- Additional calculated fields
        case 
            when forecast_status = 'approved' then true
            else false
        end as is_approved,
        
        -- Extract date components for partitioning if needed
        date_trunc('day', forecasted_at) as forecast_date,
        date_trunc('month', forecasted_at) as forecast_month
    
    from typed

)

select * from final
order by project_id, forecast_id, well_id, forecasted_at desc