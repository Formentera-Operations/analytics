-- =============================================================================
-- FOUNDATIONS_DOWNHOLECONSUMABLE STAGING MODEL - ACTUAL COLUMN NAMES ONLY
-- models/staging/enverus/foundations/stg_enverus__downhole_consumable.sql
-- 
-- Based on EXACT column names from Snowflake SHOW COLUMNS export
-- All 24 columns verified to exist in source system
-- =============================================================================

{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('enverus', 'FOUNDATIONS_DOWNHOLECONSUMABLE') }}
    where deleteddate is null
),

renamed as (
    select
        -- =============================================================================
        -- PRIMARY KEYS & IDENTIFIERS (3 columns)
        -- =============================================================================
        downholeconsumableid as downhole_consumable_id,
        wellid as well_id,
        completionid as completion_id,
        
        -- =============================================================================
        -- API IDENTIFIERS (4 columns)
        -- =============================================================================
        api_uwi,
        api_uwi_unformatted,
        api_uwi_14,
        api_uwi_14_unformatted,
        
        -- =============================================================================
        -- CHEMICAL/ADDITIVE IDENTIFICATION (5 columns)
        -- =============================================================================
        ingredientname as ingredient_name,
        casnumber as cas_number,
        tradename as trade_name,
        supplier,
        purpose,
        
        -- =============================================================================
        -- CLASSIFICATION (3 columns)
        -- =============================================================================
        category,
        subcategory,
        meshsize as mesh_size,
        
        -- =============================================================================
        -- QUANTITIES & CONCENTRATIONS (4 columns)
        -- =============================================================================
        lineitemmassinlbs as line_item_mass_lbs,
        lineitemmassshorttons as line_item_mass_short_tons,
        additiveconcentration as additive_concentration,
        hffluidconcentration as hf_fluid_concentration,
        
        -- =============================================================================
        -- TIMING (1 column)
        -- =============================================================================
        fracturedate as fracture_date,
        
        -- =============================================================================
        -- DATA VALIDATION FLAGS (2 columns)
        -- =============================================================================
        validcasnumber as valid_cas_number,
        validmass as valid_mass,
        
        -- =============================================================================
        -- CALCULATED FIELDS (10 columns)
        -- =============================================================================
        
        -- Mass conversions
        case 
            when lineitemmassinlbs is not null and lineitemmassinlbs > 0
            then lineitemmassinlbs / 2000
            else null
        end as calculated_mass_short_tons,
        
        case 
            when lineitemmassshorttons is not null and lineitemmassshorttons > 0
            then lineitemmassshorttons * 2000
            else null
        end as calculated_mass_lbs,
        
        -- Date extractions
        extract(year from fracturedate) as fracture_year,
        extract(month from fracturedate) as fracture_month,
        extract(quarter from fracturedate) as fracture_quarter,
        
        -- Chemical category classification
        case 
            when category is null then 'Unknown'
            when upper(category) like '%PROPPANT%' then 'Proppant'
            when upper(category) like '%FLUID%' then 'Fluid'
            when upper(category) like '%ADDITIVE%' then 'Additive'
            when upper(category) like '%ACID%' then 'Acid'
            when upper(category) like '%CHEMICAL%' then 'Chemical'
            else 'Other'
        end as chemical_category,
        
        -- Purpose category classification
        case 
            when purpose is null then 'Unknown'
            when upper(purpose) like '%FRICTION%' then 'Friction Reduction'
            when upper(purpose) like '%VISCOSIT%' then 'Viscosity Control'
            when upper(purpose) like '%BIOCIDE%' then 'Biocide'
            when upper(purpose) like '%SCALE%' then 'Scale Control'
            when upper(purpose) like '%CORROSION%' then 'Corrosion Control'
            when upper(purpose) like '%PROPPANT%' then 'Proppant'
            when upper(purpose) like '%FLOWBACK%' then 'Flowback Aid'
            else 'Other'
        end as purpose_category,
        
        -- Mass category classification
        case 
            when lineitemmassinlbs is null then 'Unknown'
            when lineitemmassinlbs = 0 then 'Zero Mass'
            when lineitemmassinlbs between 0.01 and 100 then 'Low Mass'
            when lineitemmassinlbs between 100.01 and 1000 then 'Medium Mass'
            when lineitemmassinlbs between 1000.01 and 10000 then 'High Mass'
            when lineitemmassinlbs > 10000 then 'Very High Mass'
            else 'Other'
        end as mass_category,
        
        -- Mesh size category classification
        case 
            when meshsize is null then 'Unknown'
            when upper(meshsize) like '%20/40%' then '20/40 Mesh'
            when upper(meshsize) like '%30/50%' then '30/50 Mesh'
            when upper(meshsize) like '%40/70%' then '40/70 Mesh'
            when upper(meshsize) like '%100%' then '100 Mesh'
            when try_cast(meshsize as integer) is not null then meshsize || ' Mesh'
            else 'Other'
        end as mesh_size_category,
        
        -- Concentration level classification
        case 
            when additiveconcentration is null then 'Unknown'
            when additiveconcentration = 0 then 'Zero Concentration'
            when additiveconcentration between 0.01 and 1 then 'Low Concentration'
            when additiveconcentration between 1.01 and 10 then 'Medium Concentration'
            when additiveconcentration between 10.01 and 50 then 'High Concentration'
            when additiveconcentration > 50 then 'Very High Concentration'
            else 'Other'
        end as concentration_category,
        
        -- =============================================================================
        -- DATA QUALITY FLAGS (8 columns)
        -- =============================================================================
        case when api_uwi_14_unformatted is null then true else false end as missing_api_flag,
        case when downholeconsumableid is null then true else false end as missing_id_flag,
        case when ingredientname is null then true else false end as missing_ingredient_flag,
        case when lineitemmassinlbs is null and lineitemmassshorttons is null then true else false end as missing_mass_flag,
        case when lineitemmassinlbs < 0 or lineitemmassshorttons < 0 then true else false end as negative_mass_flag,
        case when additiveconcentration < 0 or hffluidconcentration < 0 then true else false end as negative_concentration_flag,
        case when validcasnumber = 'FALSE' then true else false end as invalid_cas_flag,
        case when validmass = 'FALSE' then true else false end as invalid_mass_flag,
        
        -- Overall quality score calculation
        (100 - (
            (case when api_uwi_14_unformatted is null then 10 else 0 end) +
            (case when downholeconsumableid is null then 20 else 0 end) +
            (case when ingredientname is null then 15 else 0 end) +
            (case when lineitemmassinlbs is null and lineitemmassshorttons is null then 15 else 0 end) +
            (case when lineitemmassinlbs < 0 or lineitemmassshorttons < 0 then 15 else 0 end) +
            (case when additiveconcentration < 0 or hffluidconcentration < 0 then 10 else 0 end) +
            (case when validcasnumber = 'FALSE' then 10 else 0 end) +
            (case when validmass = 'FALSE' then 5 else 0 end)
        )) as data_quality_score,
        
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
TOTAL COLUMNS: 24 (all verified from actual Snowflake schema)

COLUMN BREAKDOWN:
- Primary Keys & Identifiers: 3 columns
- API Identifiers: 4 columns
- Chemical/Additive Identification: 5 columns
- Classification: 3 columns
- Quantities & Concentrations: 4 columns
- Timing: 1 column
- Data Validation Flags: 2 columns
- Calculated Fields: 10 columns
- Data Quality: 8 columns
- Metadata: 3 columns

USAGE NOTES:
- All column names verified from actual Snowflake SHOW COLUMNS
- Use api_uwi_14_unformatted for joins (most reliable)
- Filter by data_quality_score >= 70 for analytics
- Data represents line-item level chemical/additive usage in completions
- Each record is a specific ingredient used in a completion
- Mass available in both pounds and short tons
- Concentrations may represent different measurement types
- CAS numbers provide chemical identification
- Use valid_cas_number and valid_mass flags for data quality filtering
- Mesh size important for proppant classification
- Multiple records per completion expected (one per ingredient)
*/
