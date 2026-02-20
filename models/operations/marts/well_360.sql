{{
    config(
        materialized='table',
        tags=['well_360', 'mart'],
        unique_key='eid',
        post_hook=[
            "alter table {{ this }} add search optimization on equality(eid, api_10, api_14, cost_center_number)"
        ]
    )
}}

{#
    Well 360 - Master Well Dimension
    ================================

    PURPOSE:
    Single source of truth for well attributes, integrating data from five source systems.

    SOURCE SYSTEM HIERARCHY (varies by attribute domain):
    - Well Name: ProdView (engineer preference) → WellView → ODA → Combo Curve
    - Company Name/Code: ODA → ProdView (with standardization mapping) → WellView
    - Is Operated: ProdView → WellView → Combo Curve → inferred (ODA unreliable)
    - Drilling/Operations: WellView → Enverus
    - Reserves/Type Curves: Combo Curve
    - Gap Filler: Enverus (third-party, used LAST to fill nulls)

    DESIGN PATTERNS:
    1. Spine-based: Every EID from any internal source gets a row
    2. Golden Record: COALESCE priority based on data quality/authority per attribute
    3. Enverus joins via API-10 and fills gaps only (never overrides internal data)
    4. Source Lineage: Track which system provided each golden value
    5. Conflict Detection: Flag when sources disagree
    6. Company Mapping: Seed table standardizes ProdView AssetCo to proper names/codes

    KNOWN LIMITATIONS:
    - Point-in-time only (no SCD2 history yet)
    - Working Interest/NRI not fully integrated
    - Enverus may have multiple completions per API; we take most recent

    REFRESH: Daily incremental recommended
#}

with spine as (
    select * from {{ ref('int_well__spine') }}
),

oda as (
    select * from {{ ref('int_well__oda') }}
),

cc as (
    select * from {{ ref('int_well__combo_curve') }}
),

wv as (
    select * from {{ ref('int_well__wellview') }}
),

pv as (
    select * from {{ ref('int_well__prodview') }}
),

env as (
    select * from {{ ref('int_well__enverus') }}
),

-- Company name/code standardization lookup
company_map as (
    select * from {{ ref('seed_company_mapping') }}
),

-- =============================================================================
-- GOLDEN RECORD ASSEMBLY
-- =============================================================================
golden_record as (
    select
        -- =========================================================================
        -- IDENTIFIERS
        -- =========================================================================
        s.eid,

        -- ODA internal numeric well ID (used for fct_los and Cortex Analyst joins)
        oda.well_id as oda_well_id,

        -- API Numbers: Internal systems first, Enverus last
        coalesce(cc.api_10, oda.api_number, wv.api_10, pv.api_10, env.api_10) as api_10,

        -- API-14 with safe construction
        case
            when cc.api_14 is not null then cc.api_14
            when oda.api_number is not null and len(oda.api_number) = 10
                then oda.api_number || '0000'
            when wv.api_10 is not null and len(wv.api_10) = 10
                then wv.api_10 || '0000'
            when pv.api_10 is not null and len(pv.api_10) = 10
                then pv.api_10 || '0000'
            when env.api_14 is not null then env.api_14
        end as api_14,

        -- Cost Center: ODA is system of record for accounting
        coalesce(oda.cost_center, wv.cost_center, pv.property_number) as cost_center_number,

        -- External System IDs
        wv.well_id as wellview_id,
        cc.combo_curve_id,
        cc.aries_propnum,
        pv.unit_id as prodview_unit_id,
        env.enverus_well_id,
        env.enverus_completion_id,

        -- =========================================================================
        -- WELL IDENTIFICATION
        -- =========================================================================
        -- Name: ProdView preferred (engineer preference), then operational systems
        coalesce(pv.unit_name, wv.well_name, oda.well_name, cc.well_name, env.well_name) as well_name,
        coalesce(cc.lease_name, wv.lease_name, pv.lease_name, env.lease_name) as lease_name,

        -- =========================================================================
        -- COMPANY / OWNERSHIP
        -- =========================================================================
        -- Company Code: ODA first, then ProdView (mapped), then WellView
        coalesce(
            oda.company_code,
            cm.company_code_std::varchar,
            left(pv.property_number, 3),  -- Fallback: extract from property number
            wv.company_code
        ) as company_code,

        -- Company Name: ODA first, then ProdView (mapped to standardized names), then WellView
        coalesce(
            oda.company_name,
            cm.company_name_std,
            pv.asset_co,  -- Raw if no mapping exists
            cc.company_name,
            wv.asset_company
        ) as company_name,

        -- Keep raw AssetCo for reference
        pv.asset_co as prodview_asset_co_raw,

        coalesce(wv.division, pv.division) as division,

        -- Is Operated: ProdView > WellView > CC > inference > ODA fallback
        -- ODA is less reliable than internal ops systems but better than NULL for ODA-only wells
        coalesce(
            pv.is_operated,
            wv.is_operated,
            cc.is_operated,
            -- Inference from operator name when explicit flags are null
            case
                -- Any Formentera entity = operated
                when upper(coalesce(wv.operator_name, pv.operator_name, env.operator)) like '%FORMENTERA%' then true
                -- Other operator = non-op
                when coalesce(wv.operator_name, pv.operator_name, env.operator) is not null then false
            end,
            -- ODA fallback: preserves backward-compat for ODA-only wells not in other systems
            oda.is_operated
        ) as is_operated,

        -- Operator name (Enverus can fill if internal is null)
        coalesce(wv.operator_name, pv.operator_name, env.operator) as operator_name,

        -- =========================================================================
        -- ODA OPERATIONAL ATTRIBUTES
        -- =========================================================================

        -- Standardized operated status string (richer than boolean is_operated)
        case
            when oda.op_nonop_code = 'NON-OPERATED' then 'NON-OPERATED'
            when oda.op_nonop_code in ('OPERATED', 'Operated') then 'OPERATED'
            when oda.op_nonop_code = 'CONTRACT_OP' then 'CONTRACT OPERATED'
            when oda.op_nonop_code in ('DNU', 'ACCOUNTING', 'Accounting') then 'NON-WELL'
            when oda.op_nonop_code in ('OTHER', 'Other') then 'OTHER'
            when oda.op_nonop_code = 'MIDSTREAM' then 'MIDSTREAM'
            else 'UNKNOWN'
        end as op_ref,

        -- Financial control flags
        coalesce(oda.is_hold_all_billing, false) as is_hold_billing,
        coalesce(oda.is_suspend_all_revenue, false) as is_suspend_revenue,

        -- Revenue generating: active + producing + not on hold
        coalesce(
            oda.oda_status = 'Producing'
            and oda.production_status_name = 'Active'
            and not oda.is_hold_all_billing
            and not oda.is_suspend_all_revenue,
            false
        ) as is_revenue_generating,

        -- =========================================================================
        -- LOCATION
        -- =========================================================================
        -- Raw values (as-is from sources)
        coalesce(oda.state_code, cc.state, wv.state_province, pv.state_province, env.state_province) as state_raw,
        -- nullif skips known bad ProdView value ('USA' = country entered as county)
        coalesce(
            oda.county_name,
            cc.county,
            wv.county_parish,
            nullif(upper(pv.county), 'USA'),
            env.county
        ) as county_raw,
        coalesce(wv.country, pv.country, oda.country_name, env.country) as country,

        -- Standardized state (2-char uppercase via UDF)
        {{ function('normalize_state_abbrev') }}(
            coalesce(oda.state_code, cc.state, wv.state_province, pv.state_province, env.state_province)
        ) as state,

        -- Standardized county (InitCap); nullif skips bad ProdView 'USA' value
        initcap(coalesce(
            oda.county_name,
            cc.county,
            wv.county_parish,
            nullif(upper(pv.county), 'USA'),
            env.county
        )) as county,

        -- BASIN: Separate focus area from geological basin (semantic mismatch fix)
        cc.basin as combo_curve_focus_area,  -- Project names like FP_GOLDSMITH-SLANT
        coalesce(env.basin, wv.basin_name) as geological_basin,  -- Actual basins like PERMIAN OTHER
        coalesce(cc.basin, wv.basin_name, env.basin) as basin,  -- Legacy field (mixed semantics)

        coalesce(wv.field_name, env.field) as field_name,
        coalesce(wv.regulatory_field_name, pv.regulatory_field_name) as regulatory_field_name,
        coalesce(wv.district, pv.foreman_area, env.district) as district,
        pv.foreman_area,
        coalesce(wv.field_office, pv.field_office) as field_office,
        coalesce(wv.route, pv.route) as route,
        coalesce(pv.facility_name, wv.pad_name) as facility_name,
        coalesce(wv.pad_name, pv.pad_name) as pad_name,
        oda.legal_description,

        -- Enverus play/subplay (actual geological classification)
        env.play as enverus_play,
        env.sub_play as enverus_sub_play,

        -- Coordinates: WellView preferred (surveyed), then ProdView, CC, Enverus last
        coalesce(wv.latitude_degrees, pv.surface_latitude, cc.surface_latitude, env.latitude) as surface_latitude,
        coalesce(wv.longitude_degrees, pv.surface_longitude, cc.surface_longitude, env.longitude) as surface_longitude,
        wv.lat_long_datum,

        -- Bottom hole location (Enverus only)
        env.latitude_bottom_hole,
        env.longitude_bottom_hole,

        -- =========================================================================
        -- WELL CHARACTERISTICS
        -- =========================================================================
        -- Raw well configuration (as-is)
        coalesce(wv.well_configuration_type, env.trajectory) as well_configuration_type_raw,

        -- Standardized well configuration (via UDF)
        {{ function('normalize_well_config') }}(
            coalesce(wv.well_configuration_type, env.trajectory)
        ) as well_configuration_type,

        coalesce(cc.well_type, env.well_type) as well_type,
        coalesce(cc.lateral_length, env.lateral_length_ft) as lateral_length_ft,
        coalesce(cc.measured_depth, env.measured_depth_ft) as measured_depth_ft,
        coalesce(cc.true_vertical_depth, env.true_vertical_depth_ft) as true_vertical_depth_ft,
        wv.unwrapped_displacement_ft,
        cc.reserve_category,
        oda.is_stripper_well,

        -- ODA cost center classification
        oda.cost_center_type_code,
        oda.cost_center_type_name,
        coalesce(oda.cost_center_type_name = 'Well', false) as is_well,

        -- ODA operating group hierarchy
        oda.operating_group_code,
        oda.operating_group_name,


        -- Raw producing method (as-is)
        coalesce(pv.producing_method, env.producing_method) as producing_method_raw,

        -- Standardized producing method (via UDF)
        {{ function('normalize_producing_method') }}(
            coalesce(pv.producing_method, env.producing_method)
        ) as producing_method,

        env.fluid_type as enverus_fluid_type,
        env.perf_interval_ft,

        -- =========================================================================
        -- COMPLETION METRICS (Enverus is often best source for these)
        -- =========================================================================
        env.stimulated_stages,
        env.frac_stages,
        env.total_clusters,
        env.proppant_lbs,
        env.total_fluid_pumped_bbl,
        env.completion_design,

        -- =========================================================================
        -- STATUS (keep all raw for comparison)
        -- =========================================================================
        oda.oda_status,
        oda.production_status_name,
        pv.completion_status as prodview_completion_status,
        pv.prodview_status_raw,
        pv.prodview_status_clean,  -- Engineer-approved mapping from stg_prodview__status
        pv.prodview_status_date,   -- Date of the status record
        cc.combo_curve_status,
        wv.current_well_status as wellview_status,
        env.well_status as enverus_status,

        -- Unified status: Source priority via COALESCE, normalization via UDF
        -- Priority: ProdView (already normalized) → ODA → Enverus → Combo Curve
        coalesce(
            pv.prodview_status_clean,
            {{ function('normalize_well_status') }}(oda.oda_status),
            {{ function('normalize_well_status') }}(env.well_status),
            {{ function('normalize_well_status') }}(cc.combo_curve_status),
            'UNKNOWN'
        ) as unified_status,

        -- Track which system provided unified_status
        case
            when pv.prodview_status_clean is not null then 'prodview'
            when oda.oda_status is not null then 'oda'
            when env.well_status is not null then 'enverus'
            when cc.combo_curve_status is not null then 'combo_curve'
        end as unified_status_source,

        -- =========================================================================
        -- KEY DATES
        -- =========================================================================
        coalesce(wv.permit_date, env.permit_approved_date) as permit_date,
        coalesce(wv.spud_date, env.spud_date) as spud_date,
        coalesce(wv.rig_release_date, env.rig_release_date) as rig_release_date,
        env.completion_date,

        -- First Production Date: Use earliest date from any source (true first prod)
        -- LEAST gets the minimum; we use coalesce to handle nulls
        nullif(
            least(
                coalesce(pv.prodview_first_production_date, '9999-12-31'::date),
                coalesce(wv.on_production_date, '9999-12-31'::date),
                coalesce(env.first_production_date, '9999-12-31'::date)
            ),
            '9999-12-31'::date
        ) as first_production_date,

        -- Keep raw dates for comparison/audit
        pv.prodview_first_production_date,  -- When Formentera started tracking (may be acquisition)
        wv.on_production_date as wellview_first_production_date,
        env.first_production_date as enverus_first_production_date,

        -- ODA operational status dates
        oda.shut_in_date,
        oda.inactive_date,

        -- =========================================================================
        -- ENVERUS PRODUCTION BENCHMARKS (for comparison/validation)
        -- =========================================================================
        env.cumulative_oil_bbl as enverus_cumulative_oil_bbl,
        env.cumulative_gas_mcf as enverus_cumulative_gas_mcf,
        env.first_12_month_oil_bbl as enverus_first_12_month_oil_bbl,
        env.first_12_month_gas_mcf as enverus_first_12_month_gas_mcf,

        -- =========================================================================
        -- WORKING INTEREST / NRI (WellView user fields)
        -- =========================================================================
        wv.working_interest,
        wv.nri_total,

        -- =========================================================================
        -- ODA USERFIELD ATTRIBUTES
        -- =========================================================================
        oda.search_key,
        oda.pv_field,

        -- =========================================================================
        -- SOURCE PRESENCE FLAGS (from spine + Enverus match)
        -- =========================================================================
        s.in_oda,
        s.in_combo_curve,
        s.in_wellview,
        s.in_prodview,
        coalesce(env.api_10 is not null, false) as in_enverus,
        s.source_system_count + case when env.api_10 is not null then 1 else 0 end as source_system_count,
        case
            when env.api_10 is not null
                then s.source_systems || ', enverus'
            else s.source_systems
        end as source_systems

    from spine s
    left join oda on s.eid = oda.eid
    left join cc on s.eid = cc.eid
    left join wv on s.eid = wv.eid
    left join pv on s.eid = pv.eid
    -- Enverus joins on API-10 (doesn't have EID)
    left join env on s.api_10_for_enverus_match = env.api_10
    -- Company name/code standardization from ProdView AssetCo
    left join company_map cm on lower(pv.asset_co) = cm.asset_co_raw
),

-- =============================================================================
-- SOURCE LINEAGE & CONFLICT DETECTION
-- =============================================================================
with_lineage as (
    select
        g.*,

        -- Source lineage for key fields
        case
            when cc.api_10 is not null then 'combo_curve'
            when oda.api_number is not null then 'oda'
            when wv.api_10 is not null then 'wellview'
            when pv.api_10 is not null then 'prodview'
            when env.api_10 is not null then 'enverus'
        end as api_10_source,

        case
            when pv.unit_name is not null then 'prodview'
            when wv.well_name is not null then 'wellview'
            when oda.well_name is not null then 'oda'
            when cc.well_name is not null then 'combo_curve'
            when env.well_name is not null then 'enverus'
        end as well_name_source,

        case
            when pv.is_operated is not null then 'prodview'
            when wv.is_operated is not null then 'wellview'
            when cc.is_operated is not null then 'combo_curve'
            when coalesce(wv.operator_name, pv.operator_name, env.operator) is not null then 'inferred_from_operator'
        end as is_operated_source,

        case
            when wv.latitude_degrees is not null then 'wellview'
            when pv.surface_latitude is not null then 'prodview'
            when cc.surface_latitude is not null then 'combo_curve'
            when env.latitude is not null then 'enverus'
        end as coordinates_source,

        case
            when wv.spud_date is not null then 'wellview'
            when env.spud_date is not null then 'enverus'
        end as spud_date_source,

        -- Track which source provided the earliest (minimum) first production date
        case
            when
                pv.prodview_first_production_date is not null
                and (wv.on_production_date is null or pv.prodview_first_production_date <= wv.on_production_date)
                and (
                    env.first_production_date is null or pv.prodview_first_production_date <= env.first_production_date
                )
                then 'prodview_daily_volumes'
            when
                wv.on_production_date is not null
                and (
                    pv.prodview_first_production_date is null
                    or wv.on_production_date <= pv.prodview_first_production_date
                )
                and (env.first_production_date is null or wv.on_production_date <= env.first_production_date)
                then 'wellview'
            when
                env.first_production_date is not null
                and (
                    pv.prodview_first_production_date is null
                    or env.first_production_date <= pv.prodview_first_production_date
                )
                and (wv.on_production_date is null or env.first_production_date <= wv.on_production_date)
                then 'enverus'
        end as first_production_date_source,

        case
            when cc.lateral_length is not null then 'combo_curve'
            when env.lateral_length_ft is not null then 'enverus'
        end as lateral_length_source,

        -- Conflict flags (internal systems only - Enverus is just gap filler)
        coalesce(
            cc.well_name is not null
            and oda.well_name is not null
            and trim(upper(cc.well_name)) != trim(upper(oda.well_name)),
            false
        ) as well_name_conflict,

        coalesce(
            cc.api_10 is not null
            and oda.api_number is not null
            and cc.api_10 != oda.api_number,
            false
        ) as api_conflict,

        coalesce(
            cc.is_operated is not null
            and oda.is_operated is not null
            and cc.is_operated != oda.is_operated,
            false
        ) as is_operated_conflict,

        coalesce(
            (
                cc.state is not null
                and oda.state_code is not null
                and upper(cc.state) != upper(oda.state_code)
            )
            or (
                cc.county is not null
                and oda.county_name is not null
                and upper(cc.county) != upper(oda.county_name)
            ),
            false
        ) as location_conflict,

        -- Enverus vs internal discrepancy flag (informational, not a "conflict")
        coalesce(
            env.well_name is not null
            and g.well_name is not null
            and trim(upper(env.well_name)) != trim(upper(g.well_name)),
            false
        ) as enverus_name_differs,

        -- Overall conflict indicator (internal systems only)
        coalesce(
            (
                cc.well_name is not null
                and oda.well_name is not null
                and trim(upper(cc.well_name)) != trim(upper(oda.well_name))
            )
            or (
                cc.api_10 is not null
                and oda.api_number is not null
                and cc.api_10 != oda.api_number
            )
            or (
                cc.is_operated is not null
                and oda.is_operated is not null
                and cc.is_operated != oda.is_operated
            ),
            false
        ) as has_any_conflict

    from golden_record g
    -- Re-join sources for conflict detection
    left join oda on g.eid = oda.eid
    left join cc on g.eid = cc.eid
    left join wv on g.eid = wv.eid
    left join pv on g.eid = pv.eid
    left join env on g.api_10 = env.api_10
),

-- =============================================================================
-- DATA QUALITY SCORING
-- =============================================================================
final as (
    select
        *,

        -- Formentera canonical basin classification using golden state/county
        -- Covers all wells (ODA + non-ODA) — state is 100% populated across sources
        -- Uses 2-char state abbrev (normalize_state_abbrev UDF) and upper(county)
        -- Distinct from geological_basin (Enverus/WellView technical classification)
        case
            -- Permian Basin (West Texas)
            when state = 'TX' and upper(county) in (
                'ECTOR', 'CRANE', 'WINKLER', 'ANDREWS', 'MARTIN', 'GLASSCOCK',
                'GAINES', 'PECOS', 'REEVES', 'COCHRAN', 'HOCKLEY', 'CROCKETT',
                'STERLING', 'UPTON', 'MIDLAND', 'HOWARD', 'WARD', 'LOVING'
            ) then 'Permian Basin'

            -- Eagle Ford / South Texas
            when state = 'TX' and upper(county) in (
                'FRIO', 'ZAVALA', 'DIMMIT', 'KARNES', 'DEWITT', 'GONZALES',
                'LAVACA', 'MCMULLEN', 'LASALLE', 'ATASCOSA', 'WILSON'
            ) then 'Eagle Ford'

            -- Fort Worth Basin / Barnett Shale (North Central Texas)
            when state = 'TX' and upper(county) in (
                'JOHNSON', 'TARRANT', 'PARKER', 'HOOD', 'HILL',
                'WISE', 'PALO PINTO', 'ERATH', 'BOSQUE', 'SOMERVELL',
                'JACK', 'DENTON', 'MONTAGUE', 'COOKE'
            ) then 'Fort Worth Basin'

            -- Texas Panhandle
            when state = 'TX' and upper(county) in (
                'WHEELER', 'HEMPHILL', 'ROBERTS', 'GRAY', 'HUTCHINSON'
            ) then 'Texas Panhandle'

            -- SCOOP/STACK / Anadarko Basin (Oklahoma)
            when state = 'OK' and upper(county) in (
                'OKLAHOMA', 'CANADIAN', 'GRADY', 'MCCLAIN', 'LOGAN',
                'GARFIELD', 'KINGFISHER', 'GRANT', 'NOBLE', 'BLAINE',
                'CUSTER', 'CADDO', 'DEWEY', 'MAJOR'
            ) then 'SCOOP/STACK'

            -- Williston Basin / Bakken (North Dakota)
            when state = 'ND' and upper(county) in (
                'DIVIDE', 'BURKE', 'BOTTINEAU', 'WILLIAMS', 'MOUNTRAIL',
                'MCKENZIE', 'DUNN', 'STARK'
            ) then 'Williston Basin'

            -- State-level basins (county not needed)
            when state = 'MS' then 'Mississippi'
            when state = 'LA' then 'Louisiana'
            when state = 'PA' then 'Appalachian Basin'
            when state = 'AR' then 'Arkansas'

            -- Known state but county outside any basin list
            else 'Other'
        end as basin_name,

        -- op_ref_effective: use ODA classification where available, fall back to
        -- is_operated inference (from ProdView/WellView/CC) for non-ODA wells.
        -- Use this for reporting/dashboards. Use op_ref for ODA reconciliation.
        case
            when op_ref != 'UNKNOWN' then op_ref
            when is_operated = true then 'OPERATED'
            when is_operated = false then 'NON-OPERATED'
        end as op_ref_effective,

        -- True when op_ref_effective was derived from is_operated rather than ODA
        op_ref = 'UNKNOWN' as is_op_ref_inferred,

        -- Well type from name patterns (separate from well_type from CC/Enverus)
        -- Placed here so we can reference the golden well_name alias resolved above
        -- NOTE: Name-pattern matching misses many horizontals whose names don't end in 'H'
        -- (e.g. Bakken BMB/RSU names, SCOOP/STACK location-coded names). Use
        -- well_type_canonical below for all analytics and dashboards.
        case
            when upper(well_name) like '%SWD%' or upper(well_name) like '%DISPOSAL%' then 'SWD'
            -- Injector: name token OR unified status (catches status-based injectors without INJ in name)
            when unified_status = 'INJECTING' or upper(well_name) like '%INJ%' then 'Injector'
            when regexp_like(upper(well_name), '.*[0-9]+[MW]?X?H(-[A-Z0-9]+)?$') then 'Horizontal'
            when upper(well_name) like '%H-LL%' or upper(well_name) like '%H-SL%' then 'Horizontal'
            when upper(well_name) like '%UNIT%' then 'Unit Well'
            when cost_center_type_name = 'Well' then 'Vertical/Conventional'
            else 'Other'
        end as well_type_oda,

        -- Canonical well type: uses WellView/ProdView completion evidence to override
        -- name-pattern misclassifications in well_type_oda.
        -- 361 operated wells are classified "Vertical/Conventional" by name pattern
        -- but have laterals >=3K ft with 40+ stages — unambiguously horizontal.
        -- These represent $468M cumulative revenue hidden in the "vertical" bucket.
        --
        -- Priority order:
        -- 1. SWD / Injector — status-based, not overridden by completion data
        -- 2. Unit Well — structural classification, not overridden
        -- 3. Horizontal evidence (lateral >= 3000 ft OR stages >= 10) → Horizontal
        -- 4. Fall through to well_type_oda name-pattern result
        case
            when upper(well_name) like '%SWD%' or upper(well_name) like '%DISPOSAL%' then 'SWD'
            when unified_status = 'INJECTING' or upper(well_name) like '%INJ%' then 'Injector'
            -- Completion evidence overrides all name patterns (incl. UNIT names):
            -- "UNIT" in STX Eagle Ford is a location/acreage naming convention,
            -- not a well type. A well with a 12K ft lateral is Horizontal regardless
            -- of whether it's named "XXXXX-UNIT" or ends in H.
            when coalesce(lateral_length_ft, 0) >= 3000 then 'Horizontal'
            when coalesce(stimulated_stages, 0) >= 10 then 'Horizontal'
            -- No lateral/stage evidence — fall through to name-pattern classification
            when upper(well_name) like '%UNIT%' then 'Unit Well'
            when regexp_like(upper(well_name), '.*[0-9]+[MW]?X?H(-[A-Z0-9]+)?$') then 'Horizontal'
            when upper(well_name) like '%H-LL%' or upper(well_name) like '%H-SL%' then 'Horizontal'
            when cost_center_type_name = 'Well' then 'Vertical/Conventional'
            else 'Other'
        end as well_type_canonical,

        -- Activity status: human-friendly label derived from unified_status
        -- unified_status uses normalized UPPER_SNAKE_CASE from normalize_well_status() UDF
        case
            when unified_status = 'PRODUCING' then 'Producing'
            when unified_status = 'SHUT_IN' then 'Shut In'
            when unified_status = 'PLUGGED_ABANDONED' then 'Plugged & Abandoned'
            when unified_status = 'TEMP_ABANDONED' then 'Temporarily Abandoned'
            when unified_status = 'PLANNED' then 'Planned'
            when unified_status = 'INJECTING' then 'Injector'
            when unified_status = 'SOLD' then 'Sold'
            else 'Other'
        end as activity_status,

        -- Completeness score (0-100) based on key fields populated
        round(
            (
                case when api_10 is not null then 10 else 0 end
                + case when well_name is not null then 10 else 0 end
                + case when cost_center_number is not null then 15 else 0 end
                + case when company_code is not null then 10 else 0 end
                + case when is_operated is not null then 10 else 0 end
                + case when state is not null then 5 else 0 end
                + case when county is not null then 5 else 0 end
                + case when surface_latitude is not null then 10 else 0 end
                + case when well_configuration_type is not null then 5 else 0 end
                + case when oda_status is not null or prodview_status_clean is not null then 10 else 0 end
                + case when spud_date is not null then 10 else 0 end
            ), 0
        ) as completeness_score,

        -- Track how much Enverus contributed (0-100)
        round(
            (
                case when api_10_source = 'enverus' then 15 else 0 end
                + case when well_name_source = 'enverus' then 15 else 0 end
                + case when coordinates_source = 'enverus' then 20 else 0 end
                + case when spud_date_source = 'enverus' then 15 else 0 end
                + case when lateral_length_source = 'enverus' then 15 else 0 end
                + case when stimulated_stages is not null and not in_combo_curve then 20 else 0 end
            ), 0
        ) as enverus_contribution_score,

        -- Record metadata
        current_timestamp() as dbt_updated_at,
        '{{ invocation_id }}' as dbt_batch_id

    from with_lineage
)

select * from final
