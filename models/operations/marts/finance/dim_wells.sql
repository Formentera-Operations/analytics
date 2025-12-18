{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

{#
    Dimension: Wells
    
    Purpose: Well/cost center master data with geographic and operational classifications
    Grain: One row per well (well_id)
    
    Use cases:
    - LOS reporting by property
    - Geographic analysis by basin
    - Operated vs non-operated reporting
    - Production status tracking
    
    Sources:
    - stg_oda__wells
    - stg_oda__userfield (for search_key, field)
#}

WITH wells_base AS (
    SELECT 
        id,
        code,
        code_sort,
        name,
        api_number,
        legal_description,
        country_name,
        state_code,
        state_name,
        county_name,
        operating_group_code,
        operating_group_name,
        operator_id,
        cost_center_type_code,
        cost_center_type_name,
        well_status_type_code,
        well_status_type_name,
        production_status_name,
        property_reference_code,
        stripper_well,
        hold_all_billing,
        suspend_all_revenue,
        spud_date,
        first_production_date,
        shut_in_date,
        inactive_date,
        n_id
    FROM {{ ref('stg_oda__wells') }}
),

-- Pivot userfields to get search_key and field
userfields AS (
    SELECT 
        "Id" AS well_id,
        MAX(CASE WHEN "UserFieldName" = 'UF-SEARCH KEY' THEN "UserFieldValueString" END) AS search_key,
        MAX(CASE WHEN "UserFieldName" = 'UF-PV FIELD' THEN "UserFieldValueString" END) AS pv_field,
        MAX(CASE WHEN "UserFieldName" = 'UF-OPERATED?' THEN "UserFieldValueString" END) AS uf_operated,
        MAX(CASE WHEN "UserFieldName" = 'UF-OPERATOR' THEN "UserFieldValueString" END) AS uf_operator
    FROM {{ ref('stg_oda__userfield') }}
    WHERE "UserFieldName" IN ('UF-SEARCH KEY', 'UF-PV FIELD', 'UF-OPERATED?', 'UF-OPERATOR')
    GROUP BY "Id"
),

final AS (
    SELECT
        -- =================================================================
        -- Well Identity
        -- =================================================================
        w.id AS well_id,
        w.code AS well_code,
        w.code_sort,
        w.name AS well_name,
        w.api_number,
        w.legal_description,
        w.n_id,
        
        -- =================================================================
        -- Userfield Attributes
        -- =================================================================
        uf.search_key,
        uf.pv_field,
    
        
        -- =================================================================
        -- Geography
        -- =================================================================
        w.country_name,
        w.state_code,
        w.state_name,
        w.county_name,
        
        -- Basin classification based on state/county
        CASE 
            -- Permian Basin (Texas)
            WHEN w.state_name = 'Texas' AND w.county_name IN (
                'ECTOR', 'CRANE', 'WINKLER', 'ANDREWS', 'MARTIN', 'GLASSCOCK', 
                'GAINES', 'PECOS', 'REEVES', 'COCHRAN', 'HOCKLEY', 'CROCKETT', 
                'STERLING', 'UPTON', 'MIDLAND', 'HOWARD', 'WARD', 'LOVING'
            ) THEN 'Permian Basin'
            
            -- Eagle Ford / South Texas
            WHEN w.state_name = 'Texas' AND w.county_name IN (
                'FRIO', 'ZAVALA', 'DIMMIT', 'KARNES', 'DEWITT', 'GONZALES', 
                'LAVACA', 'MCMULLEN', 'LASALLE', 'ATASCOSA', 'WILSON'
            ) THEN 'Eagle Ford'
            
            -- Texas Panhandle / Anadarko
            WHEN w.state_name = 'Texas' AND w.county_name IN (
                'WHEELER', 'HEMPHILL', 'ROBERTS', 'GRAY', 'HUTCHINSON'
            ) THEN 'Texas Panhandle'
            
            -- SCOOP/STACK / Anadarko Basin (Oklahoma)
            WHEN w.state_name = 'Oklahoma' AND w.county_name IN (
                'OKLAHOMA', 'CANADIAN', 'GRADY', 'MCCLAIN', 'LOGAN',
                'GARFIELD', 'KINGFISHER', 'GRANT', 'NOBLE', 'BLAINE',
                'CUSTER', 'CADDO', 'DEWEY', 'MAJOR'
            ) THEN 'SCOOP/STACK'
            
            -- Williston Basin / Bakken (North Dakota)
            WHEN w.state_name = 'North Dakota' AND w.county_name IN (
                'DIVIDE', 'BURKE', 'BOTTINEAU', 'WILLIAMS', 'MOUNTRAIL',
                'MCKENZIE', 'DUNN', 'STARK'
            ) THEN 'Williston Basin'
            
            -- Mississippi Interior Salt Basin
            WHEN w.state_name = 'Mississippi' THEN 'Mississippi'
            
            -- Louisiana Onshore
            WHEN w.state_name = 'Louisiana' THEN 'Louisiana'
            
            -- Marcellus/Utica (Pennsylvania)
            WHEN w.state_name = 'Pennsylvania' THEN 'Appalachian Basin'
            
            -- Arkansas
            WHEN w.state_name = 'Arkansas' THEN 'Arkansas'
            
            ELSE 'Other'
        END AS basin_name,
        
        -- =================================================================
        -- Operating Group
        -- =================================================================
        w.operating_group_code,
        w.operating_group_name,
        w.operator_id,
        
        -- =================================================================
        -- Operated Status (from property reference code)
        -- =================================================================
        w.property_reference_code,
        
        CASE
            WHEN w.property_reference_code = 'NON-OPERATED' THEN 'NON-OPERATED'
            WHEN w.property_reference_code IN ('OPERATED', 'Operated') THEN 'OPERATED'
            WHEN w.property_reference_code = 'CONTRACT_OP' THEN 'CONTRACT OPERATED'
            WHEN w.property_reference_code IN ('DNU', 'ACCOUNTING', 'Accounting') THEN 'NON-WELL'
            WHEN w.property_reference_code IN ('OTHER', 'Other') THEN 'OTHER'
            WHEN w.property_reference_code = 'MIDSTREAM' THEN 'MIDSTREAM'
            ELSE 'UNKNOWN'
        END AS op_ref,
        
        COALESCE(
            -- First: check userfield UF-OPERATED?
            CASE 
                WHEN UPPER(uf.uf_operated) IN ('YES', 'Y', 'TRUE', '1') THEN TRUE
                WHEN UPPER(uf.uf_operated) IN ('NO', 'N', 'FALSE', '0') THEN FALSE
            END,
            -- Fallback: derive from property_reference_code
            w.property_reference_code IN ('OPERATED', 'Operated', 'CONTRACT_OP')
        ) AS is_operated,
        
        -- =================================================================
        -- Cost Center Classification
        -- =================================================================
        w.cost_center_type_code,
        w.cost_center_type_name,
        
        CASE 
            WHEN w.cost_center_type_name = 'Well' THEN TRUE
            ELSE FALSE
        END AS is_well,
        
        -- =================================================================
        -- Well Status
        -- =================================================================
        w.well_status_type_code,
        w.well_status_type_name,
        w.production_status_name,
        
        -- Simplified activity status
        CASE 
            WHEN w.well_status_type_name = 'Producing' AND w.production_status_name = 'Active' THEN 'Producing'
            WHEN w.well_status_type_name = 'Shut In' OR w.production_status_name = 'Shutin' THEN 'Shut In'
            WHEN w.well_status_type_name = 'Plugged and Abandoned' OR w.production_status_name = 'Plugged' THEN 'Plugged & Abandoned'
            WHEN w.well_status_type_name = 'Temp Abandoned' OR w.production_status_name = 'Temporarily Abandoned' THEN 'Temporarily Abandoned'
            WHEN w.well_status_type_name = 'Planned' THEN 'Planned'
            WHEN w.well_status_type_name = 'Injector' THEN 'Injector'
            WHEN w.well_status_type_name = 'Sold' THEN 'Sold'
            ELSE 'Other'
        END AS activity_status,
        
        -- =================================================================
        -- Well Type (from naming patterns)
        -- =================================================================
        CASE 
            -- Saltwater disposal
            WHEN UPPER(w.name) LIKE '%SWD%' OR UPPER(w.name) LIKE '%DISPOSAL%' THEN 'SWD'
            -- Injection well
            WHEN w.well_status_type_name = 'Injector' OR UPPER(w.name) LIKE '%INJ%' THEN 'Injector'
            -- Horizontal (ends in H, MXH, WXH, MH, etc.)
            WHEN REGEXP_LIKE(UPPER(w.name), '.*[0-9]+[MW]?X?H(-[A-Z0-9]+)?$') THEN 'Horizontal'
            WHEN UPPER(w.name) LIKE '%H-LL%' OR UPPER(w.name) LIKE '%H-SL%' THEN 'Horizontal'
            -- Unit wells
            WHEN UPPER(w.name) LIKE '%UNIT%' THEN 'Unit Well'
            -- Default for actual wells
            WHEN w.cost_center_type_name = 'Well' THEN 'Vertical/Conventional'
            ELSE 'Other'
        END AS well_type,
        
        -- =================================================================
        -- Operational Flags
        -- =================================================================
        w.stripper_well AS is_stripper_well,
        w.hold_all_billing AS is_hold_billing,
        w.suspend_all_revenue AS is_suspend_revenue,
        
        -- Revenue generating (active, producing, not held)
        CASE 
            WHEN w.well_status_type_name = 'Producing' 
                AND w.production_status_name = 'Active'
                AND COALESCE(w.hold_all_billing, FALSE) = FALSE
                AND COALESCE(w.suspend_all_revenue, FALSE) = FALSE
            THEN TRUE
            ELSE FALSE
        END AS is_revenue_generating,
        
        -- =================================================================
        -- Key Dates
        -- =================================================================
        w.spud_date,
        w.first_production_date,
        w.shut_in_date,
        w.inactive_date,
        
        -- =================================================================
        -- Metadata
        -- =================================================================
        CURRENT_TIMESTAMP() AS _refreshed_at

    FROM wells_base w
    LEFT JOIN userfields uf
        ON w.id = uf.well_id
)

SELECT * FROM final