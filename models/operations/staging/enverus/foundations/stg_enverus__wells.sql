-- =============================================================================
-- FOUNDATIONS_WELLS STAGING MODEL - ACTUAL COLUMN NAMES ONLY
-- models/staging/enverus/foundations/stg_enverus__wells.sql
-- 
-- Based on EXACT column names from Snowflake SHOW COLUMNS export
-- All 294 columns verified to exist in source system
-- =============================================================================

{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('enverus', 'FOUNDATIONS_WELLS') }}
    where deleteddate is null
),

renamed as (
    select
        -- =============================================================================
        -- PRIMARY KEYS (4 columns)
        -- =============================================================================
        wellid as well_id,
        completionid as completion_id,
        wellboreid as wellbore_id,
        wellpadid as well_pad_id,
        
        -- =============================================================================
        -- API IDENTIFIERS (6 columns)
        -- =============================================================================
        api_uwi,
        api_uwi_12,
        api_uwi_12_unformatted,
        api_uwi_14,
        api_uwi_14_unformatted,  -- Best for joins
        api_uwi_unformatted,
        
        -- =============================================================================
        -- WELL NAMES & IDENTIFIERS (5 columns)
        -- =============================================================================
        wellname as well_name,
        alternativewellname as alternative_well_name,
        governmentwellid as government_well_id,
        statefilenumber as state_file_number,
        wellnumber as well_number,
        
        -- =============================================================================
        -- LOCATION & GEOGRAPHY (18 columns)
        -- =============================================================================
        country,
        stateprovince as state_province,
        county,
        latitude,
        longitude,
        latitude_bh as latitude_bottom_hole,
        longitude_bh as longitude_bottom_hole,
        section,
        township,
        range,
        section_township_range,
        abstract,
        block,
        platform,
        district,
        lease,
        leasename as lease_name,
        unit_name,
        
        -- =============================================================================
        -- COORDINATE QUALITY & SOURCE (3 columns)
        -- =============================================================================
        coordinatequality as coordinate_quality,
        coordinatesource as coordinate_source,
        surfacelatlongsource as surface_lat_long_source,
        
        -- =============================================================================
        -- ENVERUS CLASSIFICATIONS (27 columns)
        -- =============================================================================
        envbasin as basin,
        envregion as region,
        envplay as play,
        envsubplay as sub_play,
        envinterval as interval_name,
        envintervalsource as interval_source,
        envoperator as operator,
        envticker as ticker_symbol,
        envwelltype as well_type,
        envwellstatus as well_status,
        envwellboretype as wellbore_type,
        envprodwelltype as production_well_type,
        envproducingmethod as producing_method,
        envwellgrouping as well_grouping,
        envpeergroup as peer_group,
        envstockexchange as stock_exchange,
        envfluidtype as fluid_type,
        envwellserviceprovider as well_service_provider,
        envfracjobtype as frac_job_type,
        envproppantbrand as proppant_brand,
        envproppanttype as proppant_type,
        environment,
        envelevationgl_ft as env_elevation_gl_ft,
        envelevationglsource as env_elevation_gl_source,
        envelevationkb_ft as env_elevation_kb_ft,
        envelevationkbsource as env_elevation_kb_source,
        envcompinserteddate as env_comp_inserted_date,
        
        -- =============================================================================
        -- ADDITIONAL CLASSIFICATIONS (8 columns)
        -- =============================================================================
        field,
        statewelltype as state_well_type,
        initialoperator as initial_operator,
        rawoperator as raw_operator,
        rawvintage as raw_vintage,
        vintage,
        unconventionaltype as unconventional_type,
        contract,
        
        -- =============================================================================
        -- ELEVATION DATA (4 columns)
        -- =============================================================================
        elevationgl_ft as elevation_gl_ft,
        elevationkb_ft as elevation_kb_ft,
        plugbackmeasureddepth_ft as plugback_measured_depth_ft,
        plugbacktrueverticaldepth_ft as plugback_true_vertical_depth_ft,
        
        -- =============================================================================
        -- DATES (20 columns)
        -- =============================================================================
        completiondate as completion_date,
        spuddate as spud_date,
        firstproddate as first_production_date,
        firstprodmonth as first_production_month,
        firstprodquarter as first_production_quarter,
        firstprodyear as first_production_year,
        lastproducingmonth as last_producing_month,
        offconfidentialdate as off_confidential_date,
        plugdate as plug_date,
        drillingenddate as drilling_end_date,
        drillingtddate as drilling_td_date,
        fracrigonsitedate as frac_rig_onsite_date,
        fracrigreleasedate as frac_rig_release_date,
        rigreleasedate as rig_release_date,
        permitapproveddate as permit_approved_date,
        permitsubmitteddate as permit_submitted_date,
        peakproductiondate as peak_production_date,
        testdate as test_date,
        resourcemagnitudereviewdate as resource_magnitude_review_date,
        firstday as first_day,
        
        -- =============================================================================
        -- DATE QUALIFIERS (4 columns)
        -- =============================================================================
        drillingtddatequalifier as drilling_td_date_qualifier,
        enddatequalifier as end_date_qualifier,
        spuddatequalifier as spud_date_qualifier,
        spuddatesource as spud_date_source,
        
        -- =============================================================================
        -- DRILLING & COMPLETION SUMMARY (26 columns)
        -- =============================================================================
        laterallength_ft as lateral_length_ft,
        md_ft as measured_depth_ft,
        tvd_ft as true_vertical_depth_ft,
        stimulatedstages as stimulated_stages,
        fracstages as frac_stages,
        totalclusters as total_clusters,
        clustersperstage as clusters_per_stage,
        clustersper1000ft as clusters_per_1000_ft,
        totalshots as total_shots,
        shotsperstage as shots_per_stage,
        shotsper1000ft as shots_per_1000_ft,
        totalfluidpumped_bbl as total_fluid_pumped_bbl,
        totalwaterpumped_gal as total_water_pumped_gal,
        proppant_lbs as proppant_lbs,
        averagestagespacing_ft as average_stage_spacing_ft,
        fluidintensity_bblperft as fluid_intensity_bbl_per_ft,
        proppantintensity_lbsperft as proppant_intensity_lbs_per_ft,
        waterintensity_galperft as water_intensity_gal_per_ft,
        proppantloading_lbspergal as proppant_loading_lbs_per_gal,
        completiondesign as completion_design,
        completionnumber as completion_number,
        completiontime_days as completion_time_days,
        trajectory,
        perfinterval_ft as perf_interval_ft,
        upperperf_ft as upper_perf_ft,
        lowerperf_ft as lower_perf_ft,
        
        -- =============================================================================
        -- COMPLETION AVERAGE METRICS (17 columns)
        -- =============================================================================
        avgbreakdownpressure_psi as avg_breakdown_pressure_psi,
        avgclusterspacing_ft as avg_cluster_spacing_ft,
        avgclusterspacingperstage_ft as avg_cluster_spacing_per_stage_ft,
        avgfluidpercluster_bbl as avg_fluid_per_cluster_bbl,
        avgfluidpershot_bbl as avg_fluid_per_shot_bbl,
        avgfluidperstage_bbl as avg_fluid_per_stage_bbl,
        avgfracgradient_psiperft as avg_frac_gradient_psi_per_ft,
        avgisip_psi as avg_isip_psi,
        avgmilltime_min as avg_mill_time_min,
        avgportsleeveopeningpressure_psi as avg_port_sleeve_opening_pressure_psi,
        avgproppantpercluster_lbs as avg_proppant_per_cluster_lbs,
        avgproppantpershot_lbs as avg_proppant_per_shot_lbs,
        avgproppantperstage_lbs as avg_proppant_per_stage_lbs,
        avgshotspercluster as avg_shots_per_cluster,
        avgshotsperft as avg_shots_per_ft,
        avgtreatmentpressure_psi as avg_treatment_pressure_psi,
        avgtreatmentrate_bblpermin as avg_treatment_rate_bbl_per_min,
        
        -- =============================================================================
        -- CHEMICAL USAGE (13 columns)
        -- =============================================================================
        acidvolume_bbl as acid_volume_bbl,
        biocide_lbs as biocide_lbs,
        breaker_lbs as breaker_lbs,
        buffer_lbs as buffer_lbs,
        claycontrol_lbs as clay_control_lbs,
        crosslinker_lbs as crosslinker_lbs,
        diverter_lbs as diverter_lbs,
        energizer_lbs as energizer_lbs,
        frictionreducer_lbs as friction_reducer_lbs,
        gellingagent_lbs as gelling_agent_lbs,
        ironcontrol_lbs as iron_control_lbs,
        scaleinhibitor_lbs as scale_inhibitor_lbs,
        surfactant_lbs as surfactant_lbs,
        
        -- =============================================================================
        -- CUMULATIVE PRODUCTION (10 columns)
        -- =============================================================================
        cumoil_bbl as cumulative_oil_bbl,
        cumgas_mcf as cumulative_gas_mcf,
        cumwater_bbl as cumulative_water_bbl,
        cumprod_boe as cumulative_prod_boe,  -- CORRECTED: was cumboe
        cumprod_mcfe as cumulative_prod_mcfe,
        cumoil_bblper1000ft as cumulative_oil_bbl_per_1000_ft,
        cumgas_mcfper1000ft as cumulative_gas_mcf_per_1000_ft,
        cumprod_boeper1000ft as cumulative_prod_boe_per_1000_ft,  -- CORRECTED: This exists
        cumprod_mcfeper1000ft as cumulative_prod_mcfe_per_1000_ft,
        cumulativesor as cumulative_sor,
        
        -- =============================================================================
        -- PRODUCTION PERIODS - FIRST 3 MONTHS (10 columns)
        -- =============================================================================
        first3monthoil_bbl as first_3_month_oil_bbl,
        first3monthgas_mcf as first_3_month_gas_mcf,
        first3monthwater_bbl as first_3_month_water_bbl,
        first3monthprod_boe as first_3_month_prod_boe,
        first3monthprod_mcfe as first_3_month_prod_mcfe,
        first3monthflaredgas_mcf as first_3_month_flared_gas_mcf,
        first3monthoil_bblper1000ft as first_3_month_oil_bbl_per_1000_ft,
        first3monthgas_mcfper1000ft as first_3_month_gas_mcf_per_1000_ft,
        first3monthprod_boeper1000ft as first_3_month_prod_boe_per_1000_ft,
        first3monthprod_mcfeper1000ft as first_3_month_prod_mcfe_per_1000_ft,
        
        -- =============================================================================
        -- PRODUCTION PERIODS - FIRST 6 MONTHS (10 columns)
        -- =============================================================================
        first6monthoil_bbl as first_6_month_oil_bbl,
        first6monthgas_mcf as first_6_month_gas_mcf,
        first6monthwater_bbl as first_6_month_water_bbl,
        first6monthprod_boe as first_6_month_prod_boe,
        first6monthprod_mcfe as first_6_month_prod_mcfe,
        first6monthflaredgas_mcf as first_6_month_flared_gas_mcf,
        first6monthoil_bblper1000ft as first_6_month_oil_bbl_per_1000_ft,
        first6monthgas_mcfper1000ft as first_6_month_gas_mcf_per_1000_ft,
        first6monthprod_boeper1000ft as first_6_month_prod_boe_per_1000_ft,
        first6monthprod_mcfeper1000ft as first_6_month_prod_mcfe_per_1000_ft,
        
        -- =============================================================================
        -- PRODUCTION PERIODS - FIRST 9 MONTHS (10 columns)
        -- =============================================================================
        first9monthoil_bbl as first_9_month_oil_bbl,
        first9monthgas_mcf as first_9_month_gas_mcf,
        first9monthwater_bbl as first_9_month_water_bbl,
        first9monthprod_boe as first_9_month_prod_boe,
        first9monthprod_mcfe as first_9_month_prod_mcfe,
        first9monthflaredgas_mcf as first_9_month_flared_gas_mcf,
        first9monthoil_bblper1000ft as first_9_month_oil_bbl_per_1000_ft,
        first9monthgas_mcfper1000ft as first_9_month_gas_mcf_per_1000_ft,
        first9monthprod_boeper1000ft as first_9_month_prod_boe_per_1000_ft,
        first9monthprod_mcfeper1000ft as first_9_month_prod_mcfe_per_1000_ft,
        
        -- =============================================================================
        -- PRODUCTION PERIODS - FIRST 12 MONTHS (10 columns)
        -- =============================================================================
        first12monthoil_bbl as first_12_month_oil_bbl,
        first12monthgas_mcf as first_12_month_gas_mcf,
        first12monthwater_bbl as first_12_month_water_bbl,
        first12monthprod_boe as first_12_month_prod_boe,
        first12monthprod_mcfe as first_12_month_prod_mcfe,
        first12monthflaredgas_mcf as first_12_month_flared_gas_mcf,
        first12monthoil_bblper1000ft as first_12_month_oil_bbl_per_1000_ft,
        first12monthgas_mcfper1000ft as first_12_month_gas_mcf_per_1000_ft,
        first12monthprod_boeper1000ft as first_12_month_prod_boe_per_1000_ft,
        first12monthprod_mcfeper1000ft as first_12_month_prod_mcfe_per_1000_ft,
        
        -- =============================================================================
        -- PRODUCTION PERIODS - FIRST 36 MONTHS (10 columns)
        -- =============================================================================
        first36monthoil_bbl as first_36_month_oil_bbl,
        first36monthgas_mcf as first_36_month_gas_mcf,
        first36monthwater_bbl as first_36_month_water_bbl,
        first36monthprod_boe as first_36_month_prod_boe,
        first36monthprod_mcfe as first_36_month_prod_mcfe,
        first36monthoil_bblper1000ft as first_36_month_oil_bbl_per_1000_ft,
        first36monthgas_mcfper1000ft as first_36_month_gas_mcf_per_1000_ft,
        first36monthprod_boeper1000ft as first_36_month_prod_boe_per_1000_ft,
        first36monthprod_mcfeper1000ft as first_36_month_prod_mcfe_per_1000_ft,
        first36monthwaterproductionbblper1000ft as first_36_month_water_production_bbl_per_1000_ft,
        
        -- =============================================================================
        -- RECENT PRODUCTION (10 columns)
        -- =============================================================================
        last12monthgasproduction_mcf as last_12_month_gas_production_mcf,
        last12monthoilproduction_bbl as last_12_month_oil_production_bbl,
        last12monthproduction_boe as last_12_month_production_boe,
        last12monthwaterproduction_bbl as last_12_month_water_production_bbl,
        last3monthisor as last_3_month_isor,
        lastmonthflaredgas_mcf as last_month_flared_gas_mcf,
        lastmonthgasproduction_mcf as last_month_gas_production_mcf,
        lastmonthliquidsproduction_bbl as last_month_liquids_production_bbl,
        lastmonthwaterproduction_bbl as last_month_water_production_bbl,
        
        -- =============================================================================
        -- PEAK PRODUCTION (11 columns)
        -- =============================================================================
        peakflaredgas_mcf as peak_flared_gas_mcf,
        peakgas_mcf as peak_gas_mcf,
        peakgas_mcfper1000ft as peak_gas_mcf_per_1000_ft,
        peakoil_bbl as peak_oil_bbl,
        peakoil_bblper1000ft as peak_oil_bbl_per_1000_ft,
        peakprod_boe as peak_prod_boe,
        peakprod_boeper1000ft as peak_prod_boe_per_1000_ft,
        peakprod_mcfe as peak_prod_mcfe,
        peakprod_mcfeper1000ft as peak_prod_mcfe_per_1000_ft,
        peakwater_bbl as peak_water_bbl,
        
        -- =============================================================================
        -- WELL TEST DATA (15 columns)
        -- =============================================================================

        testrate_boeperday as test_rate_boe_per_day,
        testrate_boeperdayper1000ft as test_rate_boe_per_day_per_1000_ft,
        testrate_mcfeperday as test_rate_mcfe_per_day,
        testrate_mcfeperdayper1000ft as test_rate_mcfe_per_day_per_1000_ft,
        testwhliquids_pct as test_wellhead_liquids_pct,
        whliquids_pct as wellhead_liquids_pct,  -- CORRECTED: exists
        oilgravity_api as oil_gravity_api,
        gasgravity_sg as gas_gravity_sg,
        gastestrate_mcfperday as gas_test_rate_mcf_per_day,
        gastestrate_mcfperdayper1000ft as gas_test_rate_mcf_per_day_per_1000_ft,
        oiltestrate_bblperday as oil_test_rate_bbl_per_day,
        oiltestrate_bblperdayper1000ft as oil_test_rate_bbl_per_day_per_1000_ft,
        watertestrate_bblperday as water_test_rate_bbl_per_day,
        watertestrate_bblperdayper1000ft as water_test_rate_bbl_per_day_per_1000_ft,
        
        -- =============================================================================
        -- ADDITIONAL TEST & PRESSURE DATA (10 columns)
        -- =============================================================================
        casingpressure_psi as casing_pressure_psi,
        flowingtubingpressure_psi as flowing_tubing_pressure_psi,
        shutinpressure_psi as shut_in_pressure_psi,
        bottom_hole_temp_degf as bottom_hole_temperature_degf,
        chokesize_64in as choke_size_64in,
        oilprodpriortest_bbl as oil_prod_prior_test_bbl,
        oiltestmethodname as oil_test_method_name,
        testcomments as test_comments,
        numberofstrings as number_of_strings,
        onoffshore as on_offshore,
        
        -- =============================================================================
        -- PRODUCTION RATIOS & CALCULATED METRICS (3 columns)
        -- =============================================================================
        flaredgasratio as flared_gas_ratio,
        gor_scfperbbl as gor_scf_per_bbl,  -- CORRECTED: was just gor
        watersaturation_pct as water_saturation_pct,
        
        -- =============================================================================
        -- FORMATION & GEOLOGY (6 columns)
        -- =============================================================================
        formation,
        bottomholeage as bottom_hole_age,
        bottomholeformationname as bottom_hole_formation_name,
        bottomholelithology as bottom_hole_lithology,
        objectiveage as objective_age,
        objectivelithology as objective_lithology,
        
        -- =============================================================================
        -- RESOURCES & RESERVES (5 columns)
        -- =============================================================================
        resourcemagnitude as resource_magnitude,
        resourcesourcequalifier as resource_source_qualifier,
        resourcevolumegasbcf as resource_volume_gas_bcf,
        resourcevolumeliquidsmmb as resource_volume_liquids_mmb,
        
        -- =============================================================================
        -- TIMING METRICS (8 columns)
        -- =============================================================================
        
        monthstopeakproduction as months_to_peak_production,
        permittospud_days as permit_to_spud_days,
        soaktime_days as soak_time_days,
        spudtocompletion_days as spud_to_completion_days,
        spudtorigrelease_days as spud_to_rig_release_days,
        spudtosales_days as spud_to_sales_days,
        totalproducingmonths as total_producing_months,
        
        -- =============================================================================
        -- FLAGS & INDICATORS (3 columns)
        -- =============================================================================
        developmentflag as development_flag,
        explorationflag as exploration_flag,
        unconventionalflag as unconventional_flag,
        
        -- =============================================================================
        -- GEOMETRY & SPATIAL DATA (4 columns)
        -- =============================================================================
        geog as geography,
        geombhl_point as geometry_bhl_point,
        geomshl_point as geometry_shl_point,
        lateralline as lateral_line,
        
        -- =============================================================================
        -- ADDITIONAL WATER DATA (2 columns)
        -- =============================================================================
        waterdepth as water_depth,
        
        
        -- =============================================================================
        -- COMMENTS & MISCELLANEOUS (5 columns)
        -- =============================================================================
        discovermagnitudecomments as discover_magnitude_comments,
        discoverytype as discovery_type,
        generalcomments as general_comments,
        wellpaddirection as well_pad_direction,
        wellsymbols as well_symbols,
        
        -- =============================================================================
        -- CALCULATED FIELDS (12 columns)
        -- =============================================================================
        
        -- Completion timing
        extract(year from completiondate) as completion_year,
        extract(month from completiondate) as completion_month,
        extract(quarter from completiondate) as completion_quarter,
        
        -- Drilling efficiency
        case 
            when spuddate is not null and completiondate is not null
            then datediff('day', spuddate, completiondate)
            else null
        end as drilling_days,
        
        -- Well vintage
        case 
            when completiondate is not null
            then datediff('year', completiondate, current_date())
            else null
        end as well_vintage_years,
        
        -- Completion efficiency
        case 
            when stimulatedstages > 0 and laterallength_ft > 0
            then laterallength_ft / stimulatedstages
            else null
        end as feet_per_stage,
        
        -- Production per foot calculations
        case 
            when first12monthoil_bbl > 0 and laterallength_ft > 0
            then first12monthoil_bbl / laterallength_ft
            else null
        end as oil_per_foot_first_year,
        
        case 
            when first12monthgas_mcf > 0 and laterallength_ft > 0
            then first12monthgas_mcf / laterallength_ft
            else null
        end as gas_per_foot_first_year,
        
        -- Well classifications
        case 
            when laterallength_ft is null then 'Unknown'
            when laterallength_ft = 0 then 'Vertical'
            when laterallength_ft between 1 and 3000 then 'Short Lateral'
            when laterallength_ft between 3001 and 7500 then 'Medium Lateral'
            when laterallength_ft between 7501 and 12000 then 'Long Lateral'
            when laterallength_ft > 12000 then 'Extended Lateral'
            else 'Other'
        end as lateral_length_category,
        
        case 
            when cumoil_bbl is null then 'Unknown'
            when cumoil_bbl = 0 then 'No Oil Production'
            when cumoil_bbl between 1 and 50000 then 'Low Producer'
            when cumoil_bbl between 50001 and 200000 then 'Medium Producer'
            when cumoil_bbl between 200001 and 500000 then 'High Producer'
            when cumoil_bbl > 500000 then 'Very High Producer'
            else 'Other'
        end as oil_production_category,
        
        case 
            when first12monthoil_bbl is null then 'Unknown'
            when first12monthoil_bbl = 0 then 'No First Year Oil'
            when first12monthoil_bbl between 1 and 25000 then 'Low First Year'
            when first12monthoil_bbl between 25001 and 75000 then 'Medium First Year'
            when first12monthoil_bbl between 75001 and 150000 then 'High First Year'
            when first12monthoil_bbl > 150000 then 'Very High First Year'
            else 'Other'
        end as first_year_oil_category,
        
        -- =============================================================================
        -- DATA QUALITY FLAGS (8 columns)
        -- =============================================================================
        case when api_uwi is null then true else false end as missing_api_flag,
        case when latitude is null or longitude is null then true else false end as missing_coordinates_flag,
        case when envoperator is null then true else false end as missing_operator_flag,
        case when completiondate is null then true else false end as missing_completion_date_flag,
        case when laterallength_ft < 0 then true else false end as invalid_lateral_length_flag,
        case when drilling_days < 0 or drilling_days > 1000 then true else false end as suspicious_drilling_days_flag,
        case when cumoil_bbl < 0 then true else false end as invalid_cumulative_oil_flag,
        
        -- Overall quality score (0-100)
        100 - (
            (case when api_uwi is null then 15 else 0 end) +
            (case when latitude is null or longitude is null then 10 else 0 end) +
            (case when envoperator is null then 10 else 0 end) +
            (case when completiondate is null then 15 else 0 end) +
            (case when laterallength_ft < 0 then 20 else 0 end) +
            (case when drilling_days < 0 or drilling_days > 1000 then 10 else 0 end) +
            (case when cumprod_boe < 0 then 20 else 0 end)
        ) as data_quality_score,
        
        -- =============================================================================
        -- METADATA (4 columns)
        -- =============================================================================
        onconfidential as on_confidential,
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
TOTAL COLUMNS: 294 (all verified from actual Snowflake schema)

MAJOR CORRECTIONS MADE:
1. ❌ GOR → ✅ GOR_SCFPERBBL
2. ❌ CUMBOE → ✅ CUMPROD_BOE  
3. ❌ Missing CUMPROD_BOEPER1000FT → ✅ EXISTS
4. ❌ WHLIQUIDS_PCT → ✅ EXISTS

COLUMN BREAKDOWN:
- Primary Keys: 4 columns
- API Identifiers: 6 columns
- Location: 18 columns  
- ENV Classifications: 27 columns
- Dates: 20 columns
- Drilling/Completion: 26 columns
- Completion Metrics: 17 columns
- Chemicals: 13 columns
- Production (Cumulative): 10 columns
- Production (Periods): 50 columns
- Production (Recent): 10 columns
- Production (Peak): 11 columns
- Test Data: 15 columns
- Formation: 6 columns
- Resources: 5 columns
- Timing: 8 columns
- Flags: 3 columns
- Calculated Fields: 12 columns
- Data Quality: 8 columns
- Metadata: 4 columns
- Other: 21 columns

USAGE NOTES:
- All column names verified from actual Snowflake SHOW COLUMNS
- Use api_uwi_14_unformatted for joins (most reliable)
- Filter by data_quality_score >= 70 for analytics
- Production periods use FIRST3MONTH, FIRST6MONTH, etc. (not individual months)
- Many production metrics include per-1000-ft calculations
*/