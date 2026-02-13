{{ config(
    enabled=true,
    materialized='view'
) }}

with unitroute as (
    select
        *,
        case
            when route_name = '4405-Blk31/Dune-Juan&Gus' then '4405-Blk31/Dune'
            when route_name = '4405-Blk31/Dune-Steven&Gus' then '4405-Blk31/Dune'
            when route_name = '4903- Notrees- Martin & Antonio' then '4903- Notrees'
            when route_name = '4904- Notrees- Adrian & Deric' then '4904- Notrees'
            when route_name = '4905-Notrees-Raul&Matt' then '4905-Notrees'
            when route_name = '4906- Notrees- Mike & Demetrio' then '4906- Notrees'
            when route_name = '6801-SH-Aaron Notgrass & Sergio Colunga' then '6801-SH'
            when route_name = '6801-SH-Jerry&Josue' then '6801-SH'
            when route_name = '6801-SH-Steven & Trace' then '6801-SH'
            when route_name = '6801-SH-Steven McDowell & Leroy Millan' then '6801-SH'
            when route_name = '6802-SH- Eddie & Jacolby' then '6802-SH'
            when route_name = '6802-SH-Eddie & Matt' then '6802-SH'
            when route_name = '6802-SH-Eduardo Gonzales & Julian Gamboa' then '6802-SH'
            when route_name = '6803-SH/Pecos-Phillip&Roy' then '6803-SH'
            when route_name = '6803-SH-Phillip&Roy' then '6803-SH'
            when route_name = '6803-SH-Roy H. & Jacolby' then '6803-SH'
            when route_name = '8001- Augusta Barrow' then '8001-Augusta Barrow'
            when route_name = '8002-GS-Ector&James' then '8002-Goldsmith'
            when route_name = '8003-GS-Robert&Kenny' then '8003-Goldsmith'
            when route_name = '8003-GS-Robert&Trace' then '8003-Goldsmith'
            when route_name = 'PEARSALL' then 'Pearsall'
            when route_name = 'ST-Route 01 - Baker' then 'ST-Route 01'
            when route_name = 'ST-Route 02 - MENDEZ' then 'ST-Route 02'
            when route_name = 'ST-Route 03 - WHITE' then 'ST-Route 03'
            when route_name = 'ST-Route 04 - GRAY' then 'ST-Route 04'
            else route_name
        end as route_name_clean
    from {{ ref('stg_prodview__routes') }}
)

select *
from unitroute
