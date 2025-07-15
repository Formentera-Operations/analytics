-- =============================================================================
-- FOUNDATIONS_PRODUCTION STAGING MODEL - ACTUAL COLUMN NAMES ONLY
-- models/staging/enverus/foundations/stg_enverus__production.sql
-- 
-- Based on EXACT column names from Snowflake SHOW COLUMNS export
-- All 63 columns verified to exist in source system
-- =============================================================================

{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('enverus', 'FOUNDATIONS_PRODUCTION') }}
    where deleteddate is null
),

renamed as (
    select
        -- =============================================================================
        -- PRIMARY KEYS & IDENTIFIERS (6 columns)
        -- =============================================================================
        productionid as production_id,
        wellid as well_id,
        completionid as completion_id,
        envprodid as env_prod_id,
        
        -- =============================================================================
        -- API IDENTIFIERS (4 columns)
        -- =============================================================================
        api_uwi,
        api_uwi_unformatted,
        api_uwi_14,
        api_uwi_14_unformatted,  -- Best for joins
        
        -- =============================================================================
        -- LOCATION & GEOGRAPHY (4 columns)
        -- =============================================================================
        country,
        stateprovince as state_province,
        county,
        
        -- =============================================================================
        -- ENVERUS CLASSIFICATIONS (4 columns)
        -- =============================================================================
        envbasin as basin,
        envregion as region,
        envplay as play,
        envinterval as interval_name,
        
        -- =============================================================================
        -- PRODUCTION TIMING (3 columns)
        -- =============================================================================
        producingmonth as producing_month,
        producingdays as producing_days,
        producingoperator as producing_operator,
        
        -- =============================================================================
        -- CURRENT DAY PRODUCTION RATES (12 columns)
        -- =============================================================================
        cdflaredgas_mcfperday as cd_flared_gas_mcf_per_day,
        cdgas_mcfperday as cd_gas_mcf_per_day,
        cdinjectiongas_mcfperday as cd_injection_gas_mcf_per_day,
        cdinjectionother_bblperday as cd_injection_other_bbl_per_day,
        cdinjectionsolvent_bblperday as cd_injection_solvent_bbl_per_day,
        cdinjectionsteam_bblperday as cd_injection_steam_bbl_per_day,
        cdinjectionwater_bblperday as cd_injection_water_bbl_per_day,
        cdliquids_bblperday as cd_liquids_bbl_per_day,
        cdprod_boeperday as cd_prod_boe_per_day,
        cdprod_mcfeperday as cd_prod_mcfe_per_day,
        cdrepgas_mcfperday as cd_rep_gas_mcf_per_day,
        cdwater_bblperday as cd_water_bbl_per_day,
        
        -- =============================================================================
        -- PREVIOUS DAY PRODUCTION RATES (7 columns)
        -- =============================================================================
        pdflaredgas_mcfperday as pd_flared_gas_mcf_per_day,
        pdgas_mcfperday as pd_gas_mcf_per_day,
        pdliquids_bblperday as pd_liquids_bbl_per_day,
        pdprod_boeperday as pd_prod_boe_per_day,
        pdprod_mcfeperday as pd_prod_mcfe_per_day,
        pdrepgas_mcfperday as pd_rep_gas_mcf_per_day,
        pdwater_bblperday as pd_water_bbl_per_day,
        
        -- =============================================================================
        -- MONTHLY PRODUCTION VOLUMES (8 columns)
        -- =============================================================================
        flaredgasprod_mcf as flared_gas_prod_mcf,
        gasprod_mcf as gas_prod_mcf,
        liquidsprod_bbl as liquids_prod_bbl,
        prod_boe as prod_boe,
        prod_condensatebbl as prod_condensate_bbl,
        prod_mcfe as prod_mcfe,
        prod_oilbbl as prod_oil_bbl,
        waterprod_bbl as water_prod_bbl,
        
        -- =============================================================================
        -- MONTHLY INJECTION VOLUMES (5 columns)
        -- =============================================================================
        injectiongas_mcf as injection_gas_mcf,
        injectionother_bbl as injection_other_bbl,
        injectionsolvent_bbl as injection_solvent_bbl,
        injectionsteam_bbl as injection_steam_bbl,
        injectionwater_bbl as injection_water_bbl,
        
        -- =============================================================================
        -- CUMULATIVE PRODUCTION (8 columns)
        -- =============================================================================
        cumflaredgas_mcf as cumulative_flared_gas_mcf,
        cumgas_mcf as cumulative_gas_mcf,
        cumliquids_bbl as cumulative_liquids_bbl,
        cumprod_boe as cumulative_prod_boe,
        cumprod_mcfe as cumulative_prod_mcfe,
        cumrepgas_mcf as cumulative_rep_gas_mcf,
        cumwater_bbl as cumulative_water_bbl,
        repgasprod_mcf as rep_gas_prod_mcf,
        
        -- =============================================================================
        -- PRODUCTION METADATA (4 columns)
        -- =============================================================================
        productionreportedmethod as production_reported_method,
        totalcompletionmonths as total_completion_months,
        totalprodmonths as total_prod_months,
        
        -- =============================================================================
        -- CALCULATED FIELDS (10 columns)
        -- =============================================================================
        
        -- Production timing
        extract(year from producingmonth) as producing_year,
        extract(month from producingmonth) as producing_month_num,
        extract(quarter from producingmonth) as producing_quarter,
        date_trunc('month', producingmonth) as producing_month_start,
        
        -- Production efficiency metrics
        case 
            when producingdays > 0 and prod_boe > 0
            then prod_boe / producingdays
            else null
        end as avg_daily_boe,
        
        case 
            when producingdays > 0 and gasprod_mcf > 0
            then gasprod_mcf / producingdays
            else null
        end as avg_daily_gas_mcf,
        
        case 
            when producingdays > 0 and liquidsprod_bbl > 0
            then liquidsprod_bbl / producingdays
            else null
        end as avg_daily_liquids_bbl,
        
        -- Production ratios
        case 
            when liquidsprod_bbl > 0 and gasprod_mcf > 0
            then gasprod_mcf / liquidsprod_bbl
            else null
        end as gor_scf_per_bbl,
        
        case 
            when liquidsprod_bbl > 0 and waterprod_bbl > 0
            then waterprod_bbl / (liquidsprod_bbl + waterprod_bbl)
            else null
        end as water_cut_ratio,
        
        -- Flaring ratio
        case 
            when gasprod_mcf > 0 and flaredgasprod_mcf > 0
            then flaredgasprod_mcf / gasprod_mcf
            else null
        end as flared_gas_ratio,
        
        -- =============================================================================
        -- DATA QUALITY FLAGS (8 columns)
        -- =============================================================================
        case when api_uwi_14_unformatted is null then true else false end as missing_api_flag,
        case when producingmonth is null then true else false end as missing_month_flag,
        case when producingdays is null or producingdays <= 0 then true else false end as invalid_producing_days_flag,
        case when prod_boe < 0 then true else false end as negative_production_flag,
        case when producingdays > 31 then true else false end as excessive_producing_days_flag,
        case when gasprod_mcf < 0 or liquidsprod_bbl < 0 then true else false end as negative_volumes_flag,
        case when producingoperator is null then true else false end as missing_operator_flag,
        
        -- Overall quality score (0-100)
        100 - (
            (case when api_uwi_14_unformatted is null then 15 else 0 end) +
            (case when producingmonth is null then 20 else 0 end) +
            (case when producingdays is null or producingdays <= 0 then 15 else 0 end) +
            (case when prod_boe < 0 then 20 else 0 end) +
            (case when producingdays > 31 then 10 else 0 end) +
            (case when gasprod_mcf < 0 or liquidsprod_bbl < 0 then 15 else 0 end) +
            (case when producingoperator is null then 5 else 0 end)
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
order by well_id, completion_id , producing_month asc

-- =============================================================================
-- COLUMN SUMMARY
-- =============================================================================

/*
TOTAL COLUMNS: 63 (all verified from actual Snowflake schema)

COLUMN BREAKDOWN:
- Primary Keys & Identifiers: 4 columns
- API Identifiers: 4 columns
- Location: 3 columns
- ENV Classifications: 4 columns
- Production Timing: 3 columns
- Current Day Rates: 12 columns
- Previous Day Rates: 7 columns
- Monthly Production: 8 columns
- Monthly Injection: 5 columns
- Cumulative Production: 8 columns
- Production Metadata: 3 columns
- Calculated Fields: 10 columns
- Data Quality: 8 columns
- Metadata: 3 columns

USAGE NOTES:
- All column names verified from actual Snowflake SHOW COLUMNS
- Use api_uwi_14_unformatted for joins (most reliable)
- Filter by data_quality_score >= 70 for analytics
- Production data is at monthly grain (producing_month)
- CD = Current Day, PD = Previous Day production rates
- BOE = Barrels of Oil Equivalent, MCFE = Thousand Cubic Feet Equivalent
- Cumulative fields represent lifetime totals through producing_month
- Use producing_days to calculate average daily rates
*/