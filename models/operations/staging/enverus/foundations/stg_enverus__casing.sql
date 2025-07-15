-- =============================================================================
-- FOUNDATIONS_CASING STAGING MODEL - ACTUAL COLUMN NAMES ONLY
-- models/staging/enverus/foundations/stg_enverus__casing.sql
-- 
-- Based on EXACT column names from Snowflake SHOW COLUMNS export
-- All 25 columns verified to exist in source system
-- =============================================================================

{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('enverus', 'FOUNDATIONS_CASING') }}
    where deleteddate is null
),

renamed as (
    select
        -- =============================================================================
        -- PRIMARY KEYS & IDENTIFIERS (3 columns)
        -- =============================================================================
        casingid as casing_id,
        wellid as well_id,
        completionid as completion_id,
        
        -- =============================================================================
        -- API IDENTIFIERS (6 columns)
        -- =============================================================================
        api_uwi,
        api_uwi_unformatted,
        api_uwi_12,
        api_uwi_12_unformatted,
        api_uwi_14,
        api_uwi_14_unformatted,  -- Best for joins
        
        -- =============================================================================
        -- CASING SPECIFICATIONS (4 columns)
        -- =============================================================================
        casingsize_in as casing_size_in,
        casingtypename as casing_type_name,
        holesize_in as hole_size_in,
        weightperfoot_lbs as weight_per_foot_lbs,
        weightperfoot2_lbs as weight_per_foot_2_lbs,
        
        -- =============================================================================
        -- DEPTH MEASUREMENTS (4 columns)
        -- =============================================================================
        settingdepth_ft as setting_depth_ft,
        multistageshoedepth_ft as multistage_shoe_depth_ft,
        multistagetooldepth_ft as multistage_tool_depth_ft,
        topofcement_ft as top_of_cement_ft,
        
        -- =============================================================================
        -- CEMENT SPECIFICATIONS (4 columns)
        -- =============================================================================
        cementamount_sacks as cement_amount_sacks,
        cementclassname as cement_class_name,
        slurryvolume_cf as slurry_volume_cf,
        topofcementmethod_name as top_of_cement_method_name,
        
        -- =============================================================================
        -- SOURCE INFORMATION (1 column)
        -- =============================================================================
        sourcedocumentname as source_document_name,
        
        -- =============================================================================
        -- CALCULATED FIELDS (8 columns)
        -- =============================================================================
        
        -- Casing specifications derived
        case 
            when casingsize_in is not null 
            then try_cast(regexp_substr(casingsize_in, '^[0-9.]+') as number(10,3))
            else null
        end as casing_size_numeric,
        
        case 
            when holesize_in is not null 
            then try_cast(regexp_substr(holesize_in, '^[0-9.]+') as number(10,3))
            else null
        end as hole_size_numeric,
        
        -- Depth calculations
        case 
            when settingdepth_ft is not null and topofcement_ft is not null
            then settingdepth_ft - topofcement_ft
            else null
        end as cement_column_height_ft,
        
        case 
            when multistageshoedepth_ft is not null and multistagetooldepth_ft is not null
            then abs(multistageshoedepth_ft - multistagetooldepth_ft)
            else null
        end as multistage_interval_length_ft,
        
        -- Cement calculations
        case 
            when cementamount_sacks is not null and settingdepth_ft is not null and settingdepth_ft > 0
            then cementamount_sacks / settingdepth_ft
            else null
        end as cement_sacks_per_foot,
        
        case 
            when slurryvolume_cf is not null and settingdepth_ft is not null and settingdepth_ft > 0
            then slurryvolume_cf / settingdepth_ft
            else null
        end as slurry_volume_per_foot_cf,
        
        -- Casing classifications
        case 
            when casingtypename is null then 'Unknown'
            when upper(casingtypename) like '%SURFACE%' then 'Surface'
            when upper(casingtypename) like '%INTERMEDIATE%' then 'Intermediate'
            when upper(casingtypename) like '%PRODUCTION%' then 'Production'
            when upper(casingtypename) like '%LINER%' then 'Liner'
            when upper(casingtypename) like '%CONDUCTOR%' then 'Conductor'
            else 'Other'
        end as casing_type_category,
        
        -- Depth classification
        case 
            when settingdepth_ft is null then 'Unknown'
            when settingdepth_ft <= 500 then 'Shallow'
            when settingdepth_ft between 501 and 2000 then 'Medium'
            when settingdepth_ft between 2001 and 5000 then 'Deep'
            when settingdepth_ft > 5000 then 'Very Deep'
            else 'Other'
        end as depth_category,
        
        -- =============================================================================
        -- DATA QUALITY FLAGS (8 columns)
        -- =============================================================================
        case when api_uwi_14_unformatted is null then true else false end as missing_api_flag,
        case when casingid is null then true else false end as missing_casing_id_flag,
        case when casingtypename is null then true else false end as missing_casing_type_flag,
        case when settingdepth_ft is null then true else false end as missing_depth_flag,
        case when settingdepth_ft < 0 then true else false end as negative_depth_flag,
        case when cementamount_sacks < 0 then true else false end as negative_cement_flag,
        case when weightperfoot_lbs < 0 then true else false end as negative_weight_flag,
        
        -- Overall quality score (0-100)
        100 - (
            (case when api_uwi_14_unformatted is null then 10 else 0 end) +
            (case when casingid is null then 20 else 0 end) +
            (case when casingtypename is null then 15 else 0 end) +
            (case when settingdepth_ft is null then 20 else 0 end) +
            (case when settingdepth_ft < 0 then 15 else 0 end) +
            (case when cementamount_sacks < 0 then 10 else 0 end) +
            (case when weightperfoot_lbs < 0 then 10 else 0 end)
        ) as data_quality_score,
        
        -- =============================================================================
        -- METADATA (3 columns)
        -- =============================================================================
        deleteddate as deleted_date,
        updateddate as updated_date,
        current_timestamp() as dbt_loaded_at
        
    from source
)

select * from renamed

-- =============================================================================
-- COLUMN SUMMARY
-- =============================================================================

/*
TOTAL COLUMNS: 25 (all verified from actual Snowflake schema)

COLUMN BREAKDOWN:
- Primary Keys & Identifiers: 3 columns
- API Identifiers: 6 columns
- Casing Specifications: 5 columns
- Depth Measurements: 4 columns
- Cement Specifications: 4 columns
- Source Information: 1 column
- Calculated Fields: 8 columns
- Data Quality: 8 columns
- Metadata: 3 columns

USAGE NOTES:
- All column names verified from actual Snowflake SHOW COLUMNS
- Use api_uwi_14_unformatted for joins (most reliable)
- Filter by data_quality_score >= 70 for analytics
- Casing data provides wellbore construction details
- Multiple casing strings possible per well (surface, intermediate, production)
- Depth measurements in feet, weights in pounds per foot
- Cement volumes in cubic feet and sacks
- Use casing_type_category for grouping analysis
- Calculated fields provide normalized metrics and classifications
*/