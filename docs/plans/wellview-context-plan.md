# WellView Context Documentation Plan

## Goal
Parse 14 WellView data model Word documents and build contextual documentation matching the ProdView format in `context/sources/prodview/`.

## Source Documents
Location: `~/Documents/WellView Data Model/`

| Document | Size | Domain |
|----------|------|--------|
| General Data Tree.docx | 21 KB | Well header, reference data |
| Wellbore, Surveys & Formations Data Tree.docx | 42 KB | Wellbore geometry, directional surveys, formations |
| Casing, Cement & Wellheads Data Tree.docx | 38 KB | Casing strings, cement jobs, wellhead components |
| Geological Evaluations Data Tree.docx | 31 KB | Geology, formations, evaluations |
| Perfs, Stims & Swabs Data Tree.docx | 37 KB | Completions work, stimulation data, swabs |
| Zones & Completions Data Tree.docx | 23 KB | Zone definitions, completion data |
| Tubing, Rods & Other Equipment Data Tree.docx | 59 KB | Downhole equipment strings |
| Surface Equipment Data Model.docx | 22 KB | Surface facilities/equipment |
| Reservoir & Equipment Tests Data Tree.docx | 41 KB | Well tests, flow periods |
| Production Operations & Failures Data Tree.docx | 39 KB | Production failures, equipment history |
| Asset Management Data Tree.docx | 23 KB | Asset tracking |
| Integrity & Barriers Data Tree.docx | 20 KB | Well integrity, barrier tracking |
| Other Data Tree.docx | 21 KB | Miscellaneous tables |
| **Operations Data Tree.docx** | **238 KB** | **Drilling ops, daily reports, costs — THE MONSTER** |

## Snowflake Sources
- **Database:** `PELOTON_FORMENTERAOPS_FORMENTERAOPS_WV120`
- **Schema (data):** `FORMENTERAOPS_WV120_DBO` — actual WellView tables
- **Schema (calc):** `FORMENTERAOPS_WV120_CALC` — precalculated reporting tables (table names end in `Calc`)
- **Ingestion:** Fivetran (CDC)
- **Soft delete pattern:** `_fivetran_deleted = true`
- **Deduplication:** `qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)`

## Existing Staging Models
75 models already built in `models/operations/staging/wellview/`:
- 58 `stg_wellview__*` models
- 17 `stg_wiserock__wv_*` models (Wiserock app-specific views)

## Target Output
All files go in `context/sources/wellview/`:

### wellview.md (Session 1)
System overview matching `context/sources/prodview/prodview.md` format:
- System overview (vendor, version, Snowflake details, ingestion)
- Core hierarchy tree (WellView's entity relationships)
- Key join patterns (IDREC/IDRECPARENT patterns)
- Calc tables explanation (precalculated reporting views from CALC schema, useful for future metrics/semantic layer)
- Unit conventions and gotchas
- Schema reference pointer to domain YAML files

### _index.yaml (Session 1)
Domain index matching `context/sources/prodview/_index.yaml` format:
- One line per domain YAML with table count and token estimate
- Instructions for loading only needed domains

### Domain YAML Files (Sessions 2-3)
Column-level schemas matching ProdView YAML format (e.g., `context/sources/prodview/allocations.yaml`):
- Compact notation: `ColumnName(type) #Description`
- Common fields omitted (IDRec, IDRecParent, sys* columns, *TK columns)
- Calc tables included with `# CALC TABLE` tag, noting they come from FORMENTERAOPS_WV120_CALC
- Each file notes which tables already have staging models built

## Session Breakdown

### Session 1: Overview + Hierarchy
- Quick scan all 14 docs (headers/structure only, not full columns)
- Build `wellview.md` — system overview, hierarchy, join patterns, gotchas
- Build `_index.yaml` — domain index
- Establish naming conventions for domain files

### Session 2: Smaller Domain YAMLs (13 docs)
Process all non-Operations documents:
1. general.yaml — General Data Tree
2. wellbore_surveys.yaml — Wellbore, Surveys & Formations
3. casing_cement.yaml — Casing, Cement & Wellheads
4. geological_evaluations.yaml — Geological Evaluations
5. perfs_stims.yaml — Perfs, Stims & Swabs
6. zones_completions.yaml — Zones & Completions
7. tubing_rods_equipment.yaml — Tubing, Rods & Other Equipment
8. surface_equipment.yaml — Surface Equipment
9. reservoir_tests.yaml — Reservoir & Equipment Tests
10. production_operations.yaml — Production Operations & Failures
11. asset_management.yaml — Asset Management
12. integrity_barriers.yaml — Integrity & Barriers
13. other.yaml — Other Data Tree

### Session 3: Operations Data Tree (the monster)
Break into 3 sub-domain YAMLs:
1. operations_jobs.yaml — `wvJob*` tree (drilling ops, daily reports, costs, mud, BHAs, safety, kicks, etc.)
2. operations_tasks.yaml — task management subtree
3. operations_inspections.yaml — inspection subtree

## Conventions

### Calc Table Documentation
- Include in their respective domain YAML (not a separate file)
- Mark with `# CALC TABLE — from FORMENTERAOPS_WV120_CALC schema`
- These are precalculated reporting views, not source-of-truth
- Document for future entity modeling, metrics, and semantic layer work

### Cross-reference with Staging Models
- Each domain YAML header lists which tables already have `stg_wellview__*` models
- Helps identify coverage gaps for future staging work

### ProdView Format Reference
- Main doc: `context/sources/prodview/prodview.md`
- Index: `context/sources/prodview/_index.yaml`
- Domain YAML example: `context/sources/prodview/allocations.yaml`

## Starter Prompts

### Session 1
```
Pick up Session 1 from docs/plans/wellview-context-plan.md — build the WellView context overview doc and _index.yaml. The 14 source Word docs are in ~/Documents/WellView Data Model/. Reference context/sources/prodview/ for the target format.
```

### Session 2
```
Pick up Session 2 from docs/plans/wellview-context-plan.md — build the 13 smaller domain YAML files for WellView. The source Word docs are in ~/Documents/WellView Data Model/. Reference context/sources/prodview/allocations.yaml for the YAML format.
```

### Session 3
```
Pick up Session 3 from docs/plans/wellview-context-plan.md — build the 3 Operations Data Tree sub-domain YAMLs (jobs, tasks, inspections). The source doc is ~/Documents/WellView Data Model/Operations Data Tree.docx.
```
