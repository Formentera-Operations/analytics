{{
  config(
    materialized='view',
    tags=['wellview', 'intermediate', 'well-status-history']
  )
}}

with status_history as (
    select
        sh.well_id,
        sh.record_id,
        sh.status_date,
        sh.well_status,
        sh.well_sub_status,
        sh.well_type,
        sh.well_subtype,
        sh.primary_fluid_type,
        sh.status_source,
        sh.comment,
        sh.created_at,
        sh.created_by,
        sh.modified_at,
        sh.modified_by,
        sh.system_tag,
        row_number() over (
            partition by sh.well_id
            order by sh.status_date desc, sh.record_id desc
        ) as status_rank,
        lead(sh.status_date) over (
            partition by sh.well_id
            order by sh.status_date desc, sh.record_id desc
        ) as next_status_date
    from {{ ref('stg_wellview__well_status_history') }} sh
),

dated_history as (
    select
        {{ dbt_utils.generate_surrogate_key(['sh.well_id']) }} as well_sk,
        {{ dbt_utils.generate_surrogate_key(['sh.well_id', 'sh.record_id']) }} as well_status_sk,
        sh.well_id,
        sh.record_id,
        sh.status_rank,
        sh.status_date,
        sh.next_status_date,
        case
            when sh.next_status_date is not null then dateadd(day, -1, sh.next_status_date)
            else null
        end as status_end_date,
        sh.well_status,
        sh.well_sub_status,
        sh.well_type,
        sh.well_subtype,
        sh.primary_fluid_type,
        sh.status_source,
        sh.comment,
        sh.created_at,
        sh.created_by,
        sh.modified_at,
        sh.modified_by,
        sh.system_tag,
        sh.status_rank = 1 as is_current_status
    from status_history sh
)

select *
from dated_history