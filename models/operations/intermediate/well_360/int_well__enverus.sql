{{
    config(
        materialized='view',
        tags=['well_360', 'source_prep']
    )
}}

{#
    Enverus Foundations Well Attributes
    ===================================
    Prepares Enverus data for Well 360 integration.
    
    Source Priority: LAST - Used to fill gaps in internal systems
    Join Key: API-10 (Enverus doesn't have EID)
    
    Enverus is third-party data with excellent coverage but should not
    override internal system values. Use as fallback only.
    
    Key fields from Enverus:
    - Comprehensive location data
    - Completion/drilling metrics
    - Production history
    - Well configuration details
#}

with source as (
    select 
        -- Join keys
        well_id as enverus_well_id,
        completion_id as enverus_completion_id,
        
        -- API variants for matching
        api_uwi_14_unformatted as api_14,
        left(api_uwi_14_unformatted, 10) as api_10,  -- Derive API-10 for matching
        api_uwi as api_formatted,
        
        -- Well identification
        well_name,
        lease_name,
        unit_name,
        operator,
        
        -- Location
        state_province,
        county,
        country,
        latitude,
        longitude,
        latitude_bottom_hole,
        longitude_bottom_hole,
        basin,
        play,
        sub_play,
        field,
        district,
        
        -- Well characteristics
        well_type,
        well_status,
        wellbore_type,
        production_well_type,
        producing_method,
        fluid_type,
        trajectory,  -- Horizontal/Vertical/Directional
        
        -- Drilling metrics
        lateral_length_ft,
        measured_depth_ft,
        true_vertical_depth_ft,
        perf_interval_ft,
        
        -- Completion metrics
        stimulated_stages,
        frac_stages,
        total_clusters,
        proppant_lbs,
        total_fluid_pumped_bbl,
        completion_design,
        
        -- Key dates
        spud_date,
        completion_date,
        first_production_date,
        rig_release_date,
        permit_approved_date,
        
        -- Production data (useful for benchmarking)
        cumulative_oil_bbl,
        cumulative_gas_mcf,
        cumulative_water_bbl,
        first_12_month_oil_bbl,
        first_12_month_gas_mcf,
        peak_oil_bbl,
        peak_gas_mcf,
        
        -- Calculated fields from staging
        lateral_length_category,
        oil_production_category,
        data_quality_score as enverus_data_quality_score
        
    from {{ ref('stg_enverus__wells') }}
    where 
        api_uwi_14_unformatted is not null
        and data_quality_score >= 50  -- Filter out low-quality Enverus records
),

-- Deduplicate by API-10 (may have multiple completions)
-- Prefer the most recent completion with best data quality
deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by api_10 
        order by 
            enverus_data_quality_score desc,
            completion_date desc nulls last,
            enverus_completion_id desc
    ) = 1
)

select * from deduplicated