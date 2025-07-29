with source as (

    select * from {{ source('combo_curve', 'forecasts') }}

),

renamed as (

    select
        id as forecast_id,
        name as forecast_name,
        project as project_id,
        type as forecast_type,
        tags as forecast_tags,
        rundate as run_date,
        createdat as created_at,
        updatedat as updated_at,
        _portable_extracted as extracted_at

    from source

)

select * from renamed