with source as (

    select * from {{ source('combo_curve', 'project_scenarios') }}

),

renamed as (

    select
        id as scenario_id,
        name as scenario_name,
        project as project_id,
        createdat as created_at,
        updatedat as updated_at,
        _portable_extracted

    from source

)

select * from renamed