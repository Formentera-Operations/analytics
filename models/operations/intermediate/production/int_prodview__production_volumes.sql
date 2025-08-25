{{
    config(
        materialized='view'
    )
}}

WITH unitalloc AS (
    SELECT*
    FROM {{ ref('stg_prodview__daily_allocations') }}
),

compdowntime AS (
    SELECT*
    FROM {{ ref('stg_prodview__completion_downtimes') }}
),

compparam AS (
    SELECT *
    FROM {{ ref('stg_prodview__completion_parameters') }}
),

unitstatus AS (
    SELECT*
    FROM {{ ref('stg_prodview__status') }}
),

source AS (
    SELECT
        a."Allocated Gas Equivalent of HCLiq mcf"
        ,a."Allocated Gas mcf"
        ,a."Allocated HCLiq bbl"
        ,a."Allocated NGL bbl"
        ,a."Allocated Oil bbl"
        ,a."Allocated Sand bbl"
        ,a."Allocated Water bbl"
        ,a."Allocation Day of Month"
        ,a."Allocation Factor Gas"
        ,a."Allocation Factor HCLiq"
        ,a."Allocation Factor Sand"
        ,a."Allocation Factor Water"
        ,a."Allocation Month"
        ,a."Allocation Record ID"
        ,a."Allocation Year"
        ,p."Bottomhole Pressure psi"
        ,p."Bottomhole Temperature F"
        ,p."Casing Pressure psi"
        ,a."Change In Inventory Gas Equivalent Oil Cond mcf"
        ,a."Change In Inventory Oil Condensate bbl"
        ,a."Change In Inventory Sand bbl"
        ,a."Change In Inventory Water bbl"
        ,p."Choke Size 64ths"
        ,a."Closing Inventory Gas Equiv Oil Condensate mcf"
        ,a."Closing Inventory Oil Condensate bbl" as "Tank Oil INV."
        ,a."Closing Inventory Sand bbl"
        ,a."Closing Inventory Water bbl"
        ,a."Created At (UTC)"
        ,a."Created By"
        ,a."Cumulated Condensate bbl"
        ,a."Cumulated Gas mcf"
        ,a."Cumulated Hcliq bbl"
        ,a."Cumulated Ngl bbl"
        ,a."Cumulated Oil bbl"
        ,a."Cumulated Sand bbl"
        ,a."Cumulated Water bbl"
        ,a."Deferred Gas Production mcf"
        ,a."Deferred Oil Condensate Production bbl"
        ,a."Deferred Sand Production bbl"
        ,a."Deferred Water Production bbl"
        ,a."Difference From Target Condensate bbl"
        ,a."Difference From Target Gas mcf"
        ,a."Difference From Target Hcliq bbl"
        ,a."Difference From Target Ngl bbl"
        ,a."Difference From Target Oil bbl"
        ,a."Difference From Target Sand bbl"
        ,a."Difference From Target Water bbl"
        ,a."Disposed Allocated Flare Gas mcf"
        ,a."Disposed Allocated Fuel Gas mcf"
        ,a."Disposed Allocated Incineration Gas mcf"
        ,a."Disposed Allocated Injected Gas mcf"
        ,a."Disposed Allocated Injected Water bbl"
        ,a."Disposed Allocated Sales Condensate bbl"
        ,a."Disposed Allocated Sales Gas mcf" as "Gross Allocated Sales Gas"
        ,a."Disposed Allocated Sales Hcliq bbl" as "Gross Allocated Sales Oil"
        ,a."Disposed Allocated Sales Ngl bbl"
        ,a."Disposed Allocated Sales Oil bbl"
        ,a."Disposed Allocated Vent Gas mcf"
        ,a."Downtime Hours" as "Down Hours"
        ,d."Downtime Code 2"
        ,d."Downtime Code 3"
        ,d."Downtime Code"
        ,a."Downtime Record ID"
        ,p."Dynamic Viscosity Pascal Seconds"
        ,a."Gathered Gas mcf"
        ,a."Gathered HCLiq bbl"
        ,a."Gathered Sand bbl"
        ,a."Gathered Water bbl"
        ,COALESCE(a."New Production HCLiq bbl", 0 ) + (COALESCE(a."New Production Gas mcf", 0)/ 6) as "Gross Allocated BOE"
        ,p."H2s Daily Reading ppm"
        ,a."Injected Lift Gas bbl"
        ,a."Injected Load Oil Condensate bbl"
        ,a."Injected Load Water bbl"
        ,a."Injected Sand bbl"
        ,p."Injection Pressure psi"
        ,a."Injection Well Gas mcf"
        ,a."Injection Well Oil Cond bbl"
        ,a."Injection Well Sand bbl"
        ,a."Injection Well Water bbl"
        ,p."Kinematic Viscosity In2 Per S"
        ,GREATEST(
            NVL(a."Last Mod At (UTC)", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ,NVL(d."Last Mod At (UTC)", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ,NVL(p."Last Mod At (UTC)", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ,NVL(s."Last Mod At (UTC)", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ) AS "Last Mod At (UTC)"
        ,a."Last Mod By"
        ,a."Last Completion Parameter Record ID"
        ,a."Last Pump Entry Record ID"
        ,a."Last Pump Entry Table"
        ,a."Last Test Record ID"
        ,p."Line Pressure psi"
        ,(a."Disposed Allocated Sales Gas mcf" * a."Net Revenue Interest Gas pct") / 100 as "Net Gas Sales"
        ,(a."New Production HCLiq bbl" * a."Net Revenue Interest Oil Cond pct") / 100 as "Net Oil Prod"
        ,a."Net Revenue Interest Gas pct"
        ,a."Net Revenue Interest Oil Cond pct"
        ,a."Net Revenue Interest Sand pct"
        ,a."Net Revenue Interest Water pct"
        ,a."New Production Condensate bbl"
        ,a."New Production Gas mcf" as "Gross Allocated WH New Gas"
        ,a."New Production HCLiq bbl" as "Gross Allocated WH Oil"
        ,a."New Production Hcliq Gas Equivalent mcf"
        ,a."New Production Ngl bbl"
        ,a."New Production Oil bbl"
        ,a."New Production Sand bbl"
        ,a."New Production Water bbl"
        ,a."Opening Inventory Gas Equivalent Oil Cond mcf"
        ,a."Opening Inventory Oil Condensate bbl"
        ,a."Opening Inventory Sand bbl"
        ,a."Opening Inventory Water bbl"
        ,a."Operating Time Hours"
        ,p."PH Level"
        ,a."Allocation Date" as "Prod Date"
        ,s."Status Record ID"
        ,s."Status" as "Prod Status"
        ,a."Pump Efficiency pct"
        ,a."Recovered Lift Gas mcf"
        ,a."Recovered Load Oil Condensate bbl"
        ,a."Recovered Load Water bbl"
        ,a."Recovered Sand bbl"
        ,a."Remaining Lift Gas mcf"
        ,a."Remaining Load Oil Condensate bbl"
        ,a."Remaining Load Water bbl"
        ,a."Remaining Sand bbl"
        ,a."Reporting Facility Record ID"
        ,p."Shut In Casing Pressure psi"
        ,p."Shut In Tubing Pressure psi"
        ,a."Starting Lift Gas mcf"
        ,a."Starting Load Oil Condensate bbl"
        ,a."Starting Load Water bbl"
        ,a."Starting Sand bbl"
       -- ,a."Status Record ID"
        ,p."Tubing Pressure psi"
        ,a."Unit Record ID"
        ,p."Wellhead Pressure psi"
        ,p."Wellhead Temperature F"
        ,a."Working Interest Gas pct"
        ,a."Working Interest Oil Cond pct"
        ,a."Working Interest Sand pct"
        ,a."Working Interest Water pct"
        --,IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitagreemt', 'AGREEMENT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitagreemtpartner', 'AGREEMENTPARTNER_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompressor', 'COMPRESSOR_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompressorentry', 'COMPRESSORENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvfacilitymonthdaycalc', 'DAILYFACILITY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodemonthdaycalc', 'DAILYNODE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankmonthdaycalc', 'DAILYTANK_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitequip', 'EQUIPMENT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitequipservice', 'EQUIPMENTSERVICE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitequipservicerec', 'EQUIPMENTSERVICEREC_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitequipdowntm', 'EQUIPMENTDOWNTIME_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitevent', 'EVENT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitevententry', 'EVENTENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvfacility', 'FACILITY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvfacrecdispcalc', 'FACILITYRECDISP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvflownetheader', 'FLOWNETWORK_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvgasanalysis', 'GASANALYSES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvgasanalysiscomp', 'GASANALYSESCOMP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvgasanaly', 'GASANALYSISGROUP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvhcliqanalysis', 'HCLIQANALYSES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvhcliqanalysiscomp', 'HCLIQANALYSESCOMP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvhcliqanaly', 'HCLIQANALYSISGROUP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterpdgas', 'METERGASPD_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterpdgasentry', 'METERGASPDENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterliquid', 'METERLIQUID_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterliquidentry', 'METERLIQUIDENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterliquidfact', 'METERLIQUIDFACTOR_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterorifice', 'METERORIFICE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterorificeecf', 'METERORIFICEECF_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterorificeentry', 'METERORIFICEENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterrate', 'METERRATE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterrateentry', 'METERRATEENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodemonthcalc', 'MONTHLYNODE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeaspt', 'OTHERMEASUREMENTPOINT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeasptentry', 'OTHERMEASUREMENTPOINTENTRY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumpesp', 'PUMPESP_V2', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumpespentry', 'PUMPESPENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumpjet', 'PUMPJET_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumpjetentry', 'PUMPJETENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumppcp', 'PUMPPCP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumppcpentry', 'PUMPPCPENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumprod', 'PUMPROD_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumprodentry', 'PUMPRODENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitregbodykey', 'REGBODYKEYS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitremark', 'REMARKS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvrespteam', 'RESPONSIBLETEAMS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvroutesetrouteuserid', 'ROUTEUSERS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitseal', 'SEAL_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitsealentry', 'SEALENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittank', 'TANK_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankentry', 'TANKENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankfactht', 'TANKHEIGHTFACTOR_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankstartinv', 'TANKSTARTINV_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankstrap', 'TANKSTRAP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankstrapdata', 'TANKSTRAPDETAILS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvticket', 'TICKETS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunit', 'UNIT_V2', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitallocmonthday', 'UNITDAILYALLOCEXTENDED_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitdispmonthday', 'UNITDAILYDISP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitallocmonth', 'UNITMONTHLYALLOC_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnode', 'UNITNODE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodeanaly', 'UNITNODEANALY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodecorr', 'UNITNODECORR_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodenetfact', 'UNITNODESHRINK_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitothertag', 'UNITOTHERTAG_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvroutesetrouteunit', 'UNITROUTE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompgathmonthdaycalc', 'WELLDAILYGATHERED_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompdowntm', 'WELLDOWNTIME_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompfluidlevel', 'WELLFLUIDLEVEL_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompwhcut', 'WELLHEADCUT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompmeasmeth', 'WELLMEASMETHOD_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompgathmonthcalc', 'WELLMONTHLYGATHERED_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompparam', 'WELLPARAM_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompratios', 'WELLRATIOS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptestreqexcalc', 'WELLREQUIREDTESTSREMAINING_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompstatus', 'WELLSTATUS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptarget', 'WELLTARGET_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptargetday', 'WELLTARGETDAILY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptest', 'WELLTEST_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptestreq', 'WELLTESTINGREQUIREMENT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompzone', 'WELLZONE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompcmnglratio', 'WELLZONERATIO_V1', '')))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) AS PUMPTK
        /*CASE
            WHEN a."Last Pump Entry Table" = 'pvunitagreemt' THEN 'AGREEMENT_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitagreemtpartner' THEN 'AGREEMENTPARTNER_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompressor' THEN 'COMPRESSOR_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompressorentry' THEN 'COMPRESSORENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvfacilitymonthdaycalc' THEN 'DAILYFACILITY_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitnodemonthdaycalc' THEN 'DAILYNODE_V1'
            WHEN a."Last Pump Entry Table" = 'pvunittankmonthdaycalc' THEN 'DAILYTANK_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitequip' THEN 'EQUIPMENT_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitequipservice' THEN 'EQUIPMENTSERVICE_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitequipservicerec' THEN 'EQUIPMENTSERVICEREC_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitequipdowntm' THEN 'EQUIPMENTDOWNTIME_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitevent' THEN 'EVENT_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitevententry' THEN 'EVENTENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvfacility' THEN 'FACILITY_V1'
            WHEN a."Last Pump Entry Table" = 'pvfacrecdispcalc' THEN 'FACILITYRECDISP_V1'
            WHEN a."Last Pump Entry Table" = 'pvflownetheader' THEN 'FLOWNETWORK_V1'
            WHEN a."Last Pump Entry Table" = 'pvgasanalysis' THEN 'GASANALYSES_V1'
            WHEN a."Last Pump Entry Table" = 'pvgasanalysiscomp' THEN 'GASANALYSESCOMP_V1'
            WHEN a."Last Pump Entry Table" = 'pvgasanaly' THEN 'GASANALYSISGROUP_V1'
            WHEN a."Last Pump Entry Table" = 'pvhcliqanalysis' THEN 'HCLIQANALYSES_V1'
            WHEN a."Last Pump Entry Table" = 'pvhcliqanalysiscomp' THEN 'HCLIQANALYSESCOMP_V1'
            WHEN a."Last Pump Entry Table" = 'pvhcliqanaly' THEN 'HCLIQANALYSISGROUP_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterpdgas' THEN 'METERGASPD_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterpdgasentry' THEN 'METERGASPDENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterliquid' THEN 'METERLIQUID_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterliquidentry' THEN 'METERLIQUIDENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterliquidfact' THEN 'METERLIQUIDFACTOR_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterorifice' THEN 'METERORIFICE_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterorificeecf' THEN 'METERORIFICEECF_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterorificeentry' THEN 'METERORIFICEENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterrate' THEN 'METERRATE_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeterrateentry' THEN 'METERRATEENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitnodemonthcalc' THEN 'MONTHLYNODE_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeaspt' THEN 'OTHERMEASUREMENTPOINT_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitmeasptentry' THEN 'OTHERMEASUREMENTPOINTENTRY_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomppumpesp' THEN 'PUMPESP_V2'
            WHEN a."Last Pump Entry Table" = 'pvunitcomppumpespentry' THEN 'PUMPESPENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomppumpjet' THEN 'PUMPJET_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomppumpjetentry' THEN 'PUMPJETENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomppumppcp' THEN 'PUMPPCP_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomppumppcpentry' THEN 'PUMPPCPENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomppumprod' THEN 'PUMPROD_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomppumprodentry' THEN 'PUMPRODENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitregbodykey' THEN 'REGBODYKEYS_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitremark' THEN 'REMARKS_V1'
            WHEN a."Last Pump Entry Table" = 'pvrespteam' THEN 'RESPONSIBLETEAMS_V1'
            WHEN a."Last Pump Entry Table" = 'pvroutesetrouteuserid' THEN 'ROUTEUSERS_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitseal' THEN 'SEAL_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitsealentry' THEN 'SEALENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunittank' THEN 'TANK_V1'
            WHEN a."Last Pump Entry Table" = 'pvunittankentry' THEN 'TANKENTRIES_V1'
            WHEN a."Last Pump Entry Table" = 'pvunittankfactht' THEN 'TANKHEIGHTFACTOR_V1'
            WHEN a."Last Pump Entry Table" = 'pvunittankstartinv' THEN 'TANKSTARTINV_V1'
            WHEN a."Last Pump Entry Table" = 'pvunittankstrap' THEN 'TANKSTRAP_V1'
            WHEN a."Last Pump Entry Table" = 'pvunittankstrapdata' THEN 'TANKSTRAPDETAILS_V1'
            WHEN a."Last Pump Entry Table" = 'pvticket' THEN 'TICKETS_V1'
            WHEN a."Last Pump Entry Table" = 'pvunit' THEN 'UNIT_V2'
            WHEN a."Last Pump Entry Table" = 'a' THEN 'UNITDAILYALLOCEXTENDED_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitdispmonthday' THEN 'UNITDAILYDISP_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitallocmonth' THEN 'UNITMONTHLYALLOC_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitnode' THEN 'UNITNODE_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitnodeanaly' THEN 'UNITNODEANALY_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitnodecorr' THEN 'UNITNODECORR_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitnodenetfact' THEN 'UNITNODESHRINK_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitothertag' THEN 'UNITOTHERTAG_V1'
            WHEN a."Last Pump Entry Table" = 'pvroutesetrouteunit' THEN 'UNITROUTE_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompgathmonthdaycalc' THEN 'WELLDAILYGATHERED_V1'
            WHEN a."Last Pump Entry Table" = 'd' THEN 'WELLDOWNTIME_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompfluidlevel' THEN 'WELLFLUIDLEVEL_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompwhcut' THEN 'WELLHEADCUT_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompmeasmeth' THEN 'WELLMEASMETHOD_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompgathmonthcalc' THEN 'WELLMONTHLYGATHERED_V1'
            WHEN a."Last Pump Entry Table" = 'p' THEN 'WELLPARAM_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompratios' THEN 'WELLRATIOS_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomptestreqexcalc' THEN 'WELLREQUIREDTESTSREMAINING_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompstatus' THEN 'WELLSTATUS_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomptarget' THEN 'WELLTARGET_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomptargetday' THEN 'WELLTARGETDAILY_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomptest' THEN 'WELLTEST_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcomptestreq' THEN 'WELLTESTINGREQUIREMENT_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompzone' THEN 'WELLZONE_V1'
            WHEN a."Last Pump Entry Table" = 'pvunitcompcmnglratio' THEN 'WELLZONERATIO_V1'
        ELSE null
        END AS "Last Pump Entry Table"*/
    FROM  unitalloc a
    LEFT JOIN  compdowntime d 
        ON a."Downtime Record ID" = d."Completion Downtime Record ID"
    LEFT JOIN compparam p 
        ON a."Last Completion Parameter Record ID" = p."Completion Parameter Record ID"
    LEFT JOIN unitstatus s 
        ON a."Status Record ID" = s."Status Record ID"
)

SELECT 
    *
    ,COALESCE("Net Oil Prod", 0) + (COALESCE("Net Gas Sales", 0)/6) as "Net 2-Stream Sales BOE"
FROM source