{{ config(
    enable= true,
    materialized='table'
) }}

WITH unitroute as (
    Select
        *
        ,CASE
            WHEN "Route Name" = '4405-Blk31/Dune-Juan&Gus' THEN '4405-Blk31/Dune'
            WHEN "Route Name" = '4405-Blk31/Dune-Steven&Gus' THEN '4405-Blk31/Dune'
            WHEN "Route Name" = '4903- Notrees- Martin & Antonio' THEN '4903- Notrees'
            WHEN "Route Name" = '4904- Notrees- Adrian & Deric' THEN '4904- Notrees'
            WHEN "Route Name" = '4905-Notrees-Raul&Matt' THEN '4905-Notrees'
            WHEN "Route Name" = '4906- Notrees- Mike & Demetrio' THEN '4906- Notrees'
            WHEN "Route Name" = '6801-SH-Aaron Notgrass & Sergio Colunga' THEN '6801-SH'
            WHEN "Route Name" = '6801-SH-Jerry&Josue' THEN '6801-SH'
            WHEN "Route Name" = '6801-SH-Steven & Trace' THEN '6801-SH'
            WHEN "Route Name" = '6801-SH-Steven McDowell & Leroy Millan' THEN '6801-SH'
            WHEN "Route Name" = '6802-SH- Eddie & Jacolby' THEN '6802-SH'
            WHEN "Route Name" = '6802-SH-Eddie & Matt' THEN '6802-SH'
            WHEN "Route Name" = '6802-SH-Eduardo Gonzales & Julian Gamboa' THEN '6802-SH'
            WHEN "Route Name" = '6803-SH/Pecos-Phillip&Roy' THEN '6803-SH'
            WHEN "Route Name" = '6803-SH-Phillip&Roy' THEN '6803-SH'
            WHEN "Route Name" = '6803-SH-Roy H. & Jacolby' THEN '6803-SH'
            WHEN "Route Name" = '8001- Augusta Barrow' then '8001-Augusta Barrow'
            WHEN "Route Name" = '8002-GS-Ector&James' then '8002-Goldsmith'
            WHEN "Route Name" = '8003-GS-Robert&Kenny' then '8003-Goldsmith'
            WHEN "Route Name" = '8003-GS-Robert&Trace' then '8003-Goldsmith'
            WHEN "Route Name" = 'PEARSALL' then 'Pearsall'
            WHEN "Route Name" = 'ST-Route 01 - Baker' then 'ST-Route 01'
            WHEN "Route Name" = 'ST-Route 02 - MENDEZ' then 'ST-Route 02'
            WHEN "Route Name" = 'ST-Route 03 - WHITE' then 'ST-Route 03'
            WHEN "Route Name" = 'ST-Route 04 - GRAY' then 'ST-Route 04'
        ELSE "Route Name" End
        AS "Route Name Clean"
    FROM {{ ref('stg_prodview__routes') }}  
)

Select
       r.*
    FROM unitroute r 

