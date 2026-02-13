{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'dim']
) }}

with unitroute as (
    select *
    from {{ ref('int_dim_route') }}
)

select
    r.backup_lease_operator as "Backup Lease Operator",
    r.created_by as "Created By",
    r.created_at_utc as "Created Date (UTC)",
    r.id_flownet as "Flow Net ID",
    r.foreman as "Foreman",
    r.modified_by as "Last Mod By",
    r.modified_at_utc as "Last Mod Date (UTC)",
    r.notes as "NOTES",  -- noqa: RF06
    r.primary_lease_operator as "Primary Lease Operator",
    r.route_name as "Route",
    r.route_name_clean as "Route Name",
    r.id_rec_parent as "Route Parent Record ID",
    r.id_rec as "Route Record ID"
from unitroute as r
