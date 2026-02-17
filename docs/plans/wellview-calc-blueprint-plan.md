# Plan: WellView Calc Table Analytics Blueprint

## Context

We've completed 16 domain YAML files documenting 498 WellView tables (146 calc tables). The calc tables are WellView's pre-computed rollups — they reveal what the vendor considers worth aggregating, at what grain, and with what measures. This is a blueprint for our entity models, metrics, and future semantic layer.

No semantic layer exists today. All reporting flows through explicit fact/dim tables. The `context/domains/` and `context/entities/` directories are empty — this would be the first analytics abstraction document in the project.

## Deliverable

Create `context/sources/wellview/calc_table_blueprint.md` — a reference document that abstracts the 146 calc tables into:

1. **Entity hierarchy** — what entities the calc tables define and how they nest
2. **Metric catalog** — what KPIs/measures are pre-computed, grouped by business domain
3. **Grain map** — what grain each calc table operates at, with the naming convention decoded
4. **Semantic layer roadmap** — which calc tables would power future `semantic_models` and `metrics` definitions
5. **Staging priorities** — which calc tables to stage first for maximum business value

## File: `context/sources/wellview/calc_table_blueprint.md`

### Structure

```markdown
# WellView Calc Table Analytics Blueprint
# Reference for building entity models, metrics, and semantic layer definitions

## How to Use This Document
- Load when planning intermediate/mart models or semantic layer work
- Cross-reference with domain YAMLs for column-level detail
- Calc tables are pre-computed — use as validation, not source-of-truth

## Entity Hierarchy (from calc tables)

### Operations Entity Tree
Job (49 J*Calc tables)
  ├── Daily Report (44 JR*Calc)
  │   ├── Time Log (JTL*Calc)
  │   └── Rig Crew (JRRC*Calc)
  ├── Program Phase (17 JPP*Calc)
  │   └── Phase Type (8 JPPCode1*Calc)
  ├── Drill String (6 JDS*Calc)
  │   └── Stand (Stand*Calc at multiple grains)
  └── Operational Aggregates
      ├── Rig Activity (sensor-derived states)
      ├── NPT Category
      ├── Cost Codes (6-level hierarchy)
      └── Vendors

### Well Infrastructure Entity Tree
Wellbore (12 WB*/WBF*Calc)
  ├── Wellbore Section (WellboreSummaryCalc)
  ├── Formation (WBFRigActivity*Calc)
  ├── Directional Survey (WCompDirSurvey*Calc)
  └── Stand (WBStandCalc, WBFStandCalc)

Zone (3 Zone*Calc)
  ├── Formation mapping (ZoneFormationCalc)
  └── Production by activity type (ZoneProdTyp*Calc)

Stimulation (2 Stim*Calc)
  └── Perforation linkage + proppant summary

AFE (2 AFE*Calc in other.yaml)
  └── Well-level cost authorization + code breakdown

## Grain Map & Naming Convention
[Table decoding the wvJ/wvJR/wvJPP/wvJDS/wvJTL/wvWB/wvWBF prefixes]

## Metric Catalog by Business Domain

### Drilling Cost KPIs (Tier 1 — highest business value)
[Tables, measures, grain, suggested metric names]

### Drilling Performance KPIs (Tier 1)
[ROP, NPT, phase duration variance, etc.]

### Safety & HSE (Tier 1)
[Incident counts, hazard IDs, safety checks]

### Well Construction (Tier 2)
[Wellbore sections, casing, directional surveys]

### Production (Tier 2)
[Zone production, activity states, volumes]

### Completions & Stimulation (Tier 2)
[Proppant summary, perforation linkage]

### Materials & Equipment (Tier 3)
[Fluids, mud, supplies, vendors]

### Personnel (Tier 3)
[Headcount by company/type/crew]

## Semantic Layer Roadmap
[Which calc tables → which semantic_model entities → which metrics]
[Grouped by priority tier]

## Staging Priorities
[Top 10-15 calc tables to stage first, with rationale]
```

## Implementation Steps

1. [x] **Write the blueprint document** at `context/sources/wellview/calc_table_blueprint.md`
   - Pull entity/measure analysis from the exploration results
   - Organize into the structure above
   - Keep it concise and actionable (target ~400 lines)

2. [x] **Update `wellview.md`** — add a pointer to the blueprint in the "Calc Tables" section (one line: "See `calc_table_blueprint.md` for the full analytics abstraction")

3. [x] **Commit and push** on `docs/wellview-context` branch

## What This Does NOT Do

- Does not create dbt models, semantic models, or metrics YAML (that's the next step, informed by this doc)
- Does not modify any existing code
- Stays in the `context/` directory as documentation

## Verification

- Document reads coherently as a standalone reference
- All 146 calc tables are accounted for in the entity hierarchy
- Metric catalog covers the core O&G business domains
- Staging priorities are defensible (Tier 1 = cost + performance + safety)

## Starter Prompt

```
Pick up the WellView Calc Table Analytics Blueprint plan from docs/plans/wellview-calc-blueprint-plan.md — build context/sources/wellview/calc_table_blueprint.md. Read all 16 domain YAML files in context/sources/wellview/ to extract the 146 calc tables, then organize into entity hierarchies, metric catalogs, grain maps, semantic layer roadmap, and staging priorities. Add a pointer from wellview.md. Commit and push on docs/wellview-context branch.
```
