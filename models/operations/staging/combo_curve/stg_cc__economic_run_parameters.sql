with source as (

    select * from {{ source('combo_curve', 'econ_runs') }}

),

renamed as (

    select
        id as econ_run_id,
        project as project_id,
        scenario as scenario_id,
        rundate as econ_run_date,
        status as status,
        tags

    from source

)

select * from renamed order by econ_run_date desc