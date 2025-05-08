with source as (

    select * from {{ source('combo_curve', 'projects') }}

),

renamed as (

    select
        id as project_id,
        name as project_name,
        createdat as created_at,
        updatedat as updated_at,
        _portable_extracted

    from source

)

select * from renamed