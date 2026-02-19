---
title: "docs: ProdView Context Sprint 2 — Remaining 8 Domain YAML Upgrades"
type: docs
date: 2026-02-19
deepened: 2026-02-19
---

# ProdView Context Sprint 2 — Remaining 8 Domain YAML Upgrades

## Enhancement Summary

**Deepened:** 2026-02-19
**Sections enhanced:** Domain inventory, structural anomalies, agent split, acceptance criteria, per-agent templates
**Research agents used:** repo-research-analyst, learnings-researcher, plan-reviewer, writing-reviewer

### Key Improvements Over Original Plan
1. Discovered 3rd anomalous root entity: `pvOpenState` in `reference` (same class as `pvFacility`)
2. Agent D split into D1 + D2 — original D had 46 tables across 3 root entity types (too complex)
3. Replaced `dbt parse` with `yamllint` in acceptance criteria (context docs aren't dbt YAML)
4. Added pre-step: fix entity_model.md Sprint 2 list (currently missing 3 of 8 domains)
5. Added per-agent prompt templates (agents at execution time only see their spawn prompt)
6. Documented 8 new domain-specific gotchas not in the original plan

---

## Overview

Sprint 2 completes the ProdView context documentation alignment started in Sprint 1. Eight remaining domain YAML files need `relationships:` and `key_patterns:` comment sections added, matching the ODA-format standard established in Sprint 1. After this sprint, all 12 ProdView domain YAMLs will have full relationship documentation — unblocking future mart builders who need to understand joins and table structure.

**Brainstorm:** `docs/brainstorms/2026-02-19-prodview-context-entity-model-brainstorm.md`
**Sprint 1 output:** commit `7c31437` — entity_model.md + 4 domain upgrades + orphan cleanup
**Format reference:** `context/sources/prodview/domains/completions.yaml` (Sprint 1 template)

---

## Problem Statement

`entity_model.md` documents 5 domains as "Sprint 2 TBD" — but the plan covers 8 domains. The entity model also needs its Sprint 2 list corrected before execution (see Pre-Step below).

The 8 remaining domain files are table catalogs only — no relationship structure, no join patterns. Any agent or developer building a mart that touches these domains lacks the critical join guidance that Sprint 1 established for completions/allocations/meters/flow_network.

---

## Pre-Step (Before Spawning Agents)

The `entity_model.md` Sprint 2 TBD section (lines 340-349) currently lists only 5 domains:
`artificial_lift`, `facilities`, `tanks`, `routes`, `admin`. Three are missing: `equipment`, `fluid_analysis`, `reference`.

**Before spawning any agents, add the three missing domains to entity_model.md:**

```bash
# Verify the gap
grep -A 20 "Sprint 2 (TBD)" context/sources/prodview/entity_model.md
```

Edit `entity_model.md` to add `equipment`, `fluid_analysis`, `reference` to the Sprint 2 TBD list. This ensures the "update from TBD to Complete" step at the end works correctly.

---

## Domain Inventory & Complexity Analysis

| Domain | Tables | Root Entity | Effort | Mart Relevance | Staging Models Exist |
|--------|--------|-------------|--------|----------------|---------------------|
| `tanks` | 12 | pvUnit | Medium | **High** | ✓ (7 models) |
| `facilities` | 12 | **pvFacility** ⚠️ | **High** | **High** | ✓ (3 models) |
| `artificial_lift` | 11 | pvUnitComp | Medium | Medium | ✓ (5 models) |
| `equipment` | 8 | pvUnit | Low | Low | ✗ context-only |
| `routes` | 8 | **pvRouteSet** ⚠️ (mixed) | Low | Low | ✓ (2 models) |
| `admin` | 14 | **Mixed** ⚠️ | Medium | Low | ✓ (5 models) |
| `reference` | 27 | pvUnit + **pvOpenState** ⚠️ | **High** | Medium | ✓ partial |
| `fluid_analysis` | 21 | pvUnitComp + standalone | **High** | Low | ✗ context-only |

### Critical Structural Anomalies

**1. `facilities` — `pvFacility` is a separate root entity (NOT a pvUnit child)**

`pvFacility` does not use `idrecparent/idflownet` to link to pvUnit. It is a standalone production grouping entity. Join path:
- `pvFacility → pvFacilityUnit` (bridge table) → `pvUnit` / `pvUnitComp`
- `pvFacility → pvFacilityMonthCalc` etc. via standard `idrecparent` (but parent is pvFacility, not pvUnit)
- `pvFacility.IDPa` = EID — directly mart-relevant for production analytics
- `pvFacilityMonthCalc` is an 83-column calc table (opening/closing inventory, production, receipts, dispositions for all streams) — primary mart target for facility-level balance reporting
- `pvFacRecDispCalc` has no `+parent` tag — may link to pvFacility via non-standard FK. Agent B must check `tables/pvFacilityUnit.yaml` for the actual FK column name before writing the relationships section.

**2. `routes` — mixed root (`pvRouteSet` + standard pvUnit children)**

`pvRouteSet → pvRouteSetRoute → pvRouteSetRouteUnit` is an independent hierarchy. BUT the domain also contains two pvUnit children: `pvUnitUserIDCalc` and `pvUnitDateListCalc` — unit-scoped calculated tables for route-based security. Both sub-hierarchies must be documented.

**3. `admin` — mixed root (pvUnit children + independent reference entities)**

`pvUnitAgreemt` and `pvUnitAgreemtPartner` are standard pvUnit children (via idrecparent). BUT `pvApproval`, `pvApprovalStep`, `pvTask`, `pvRespTeam`, `pvPartner`, `pvPartnerContact` have NO `+parent` annotation — they are standalone workflow/partner entities NOT anchored to pvUnit via idrecparent/idflownet. Agent D1 must document both groups separately, NOT apply the standard pvUnit child pattern to all tables.

**4. `reference` — `pvOpenState` is a second root entity**

`pvOpenState` is NOT a pvUnit child. It is a standalone period-boundary entity (production accounting period opening inventories). Its children (`pvOpenStateComp`, `pvOpenStateUnit`, `pvOpenStateTank`, `pvOpenStateCompLoad`) hang off `pvOpenState.idrec` — not pvUnit.idrec. This is a different root entity class from the standard domain pattern. Agent D2 must document both the pvUnit sub-hierarchy AND the pvOpenState sub-hierarchy.

**5. `fluid_analysis` — THREE sub-hierarchies (not two)**

The original plan described two. There are three:
1. **Completion-level links**: `pvUnitComp → pvUnitCompGasAnaly` (FK pointer to analysis group)
2. **Unit-level links**: `pvUnit → pvUnitAnaly` (unit-level fluid analysis, e.g., for tank origin fluids)
3. **Standalone analysis groups**: `pvGasAnaly → pvGasAnalysis → pvGasAnalysisComp` (shared groups; many completions can point to the same group)

**6. `tanks` — `pvTicket` is a cross-network, cross-domain entity**

`pvTicket` (truck tickets) appears in tanks.yaml but has NO `+parent` tag. It is NOT a standard idrecparent child. It references: tank via `IDRecTank`, units via `IDRecUnitSend`/`IDRecUnitRec`, facilities via `IDRecFacilitySend`/`IDRecFacilityRec`, and routes via route calc columns. Note also: `stg_prodview__tickets` is filed under `staging/admin/` — the staging folder boundary is inconsistent with the domain YAML. The relationships section must document pvTicket explicitly as a cross-network entity, not as a standard tank child.

**7. `artificial_lift` — 3-level chain with type asymmetry**

The hierarchy is: `pvUnitComp → pvUnitCompPump → pvUnitCompPump{Type}(1:1 ext) → pvUnitCompPump{Type}Entry`

Type asymmetry:
- Rod, ESP, Plunger: have both a `+ext,parent` 1:1 config extension AND entry tables
- PCP: only an entry table (`pvUnitCompPumpPCPEntry`) — joins directly to `pvUnitCompPump.idrec` (no intermediate extension)
- Jet: entry + fluid tables (`pvUnitCompPumpJetEntry`, `pvUnitCompPumpJetFluid`) — also joins directly to pvUnitCompPump (no extension header)

This asymmetry MUST be documented explicitly in the relationships section — it's where mart builders will make join mistakes.

**8. `equipment` — naming confusion risk**

`pvUnitCompressor` is a pvUnit child (same level as pvUnitEquip), NOT related to pvUnitComp (completions). The `pvUnitCompressor` prefix could mislead. Note in key_patterns.

---

## Implementation Plan

### Agent Split (5 agents — split from original 4)

**Agent A: `artificial_lift` + `tanks`**
Both are pvUnitComp-rooted with standard idrecparent patterns. Primary gotcha: artificial_lift 3-level chain + type asymmetry. pvTicket special handling in tanks.

**Agent B: `facilities` + `routes`**
Both have anomalous root entities. Keeping them together builds consistent "non-standard root" mental model.

**Agent C: `fluid_analysis`**
21 tables with 3 sub-hierarchies. Standalone.

**Agent D1: `equipment` + `admin`**
Low-to-medium complexity. Standard pvUnit children (equipment) + mixed-root documentation (admin).

**Agent D2: `reference`**
27 tables, two root entities (pvUnit + pvOpenState). High effort — warrants its own agent.

### Per-Agent Prompt Templates

**CRITICAL:** At execution time, agents only see their spawn prompt. All anomaly context must be embedded in the prompt. The orchestrator MUST paste the domain-specific anomaly notes from this plan into each agent's prompt.

#### Agent A Prompt Template

```
Task: Add relationships and key_patterns sections to two ProdView domain YAML files:
1. context/sources/prodview/domains/artificial_lift.yaml
2. context/sources/prodview/domains/tanks.yaml

Read order:
1. context/sources/prodview/domains/artificial_lift.yaml (target — understand structure first)
2. context/sources/prodview/domains/tanks.yaml (target)
3. context/sources/prodview/domains/completions.yaml (FORMAT REFERENCE ONLY)
4. context/sources/prodview/prodview.md lines 55-73 (join patterns)

Critical anomalies for artificial_lift:
- The hierarchy is 3 levels deep: pvUnitComp → pvUnitCompPump → pvUnitCompPump{Type}(1:1 ext) → Entry
- Rod, ESP, Plunger have both a 1:1 +ext,parent config table AND entry tables
- PCP and Jet have NO extension config table — their entry tables join DIRECTLY to pvUnitCompPump.idrec
- Document the 1:1 vs 1:many distinction explicitly in key_patterns

Critical anomalies for tanks:
- pvTicket has NO +parent tag — it is a cross-network entity linking tanks, units, facilities, and routes
- pvTicket is NOT a standard pvUnitTank child — document it as a standalone cross-domain entity
- pvUnitTankFactHt and pvUnitTankStrap are +ext,parent (1:1 extensions of pvUnitTank, NOT 1:many)
- pvUnitTank is at pvUnit level (not pvUnitComp level) — a common mart design mistake to document
```

#### Agent B Prompt Template

```
Task: Add relationships and key_patterns sections to two ProdView domain YAML files:
1. context/sources/prodview/domains/facilities.yaml
2. context/sources/prodview/domains/routes.yaml

Read order:
1. context/sources/prodview/domains/facilities.yaml (target — structure first)
2. context/sources/prodview/domains/routes.yaml (target)
3. context/sources/prodview/domains/completions.yaml (FORMAT REFERENCE ONLY)
4. context/sources/prodview/prodview.md lines 55-73 (join patterns)
5. context/sources/prodview/tables/pvFacilityUnit.yaml (look up actual FK column for facilities bridge join)

Critical anomalies for facilities:
- pvFacility is NOT a pvUnit child — it is a separate hierarchy root (NOT joined via idrecparent/idflownet to pvUnit)
- pvFacilityUnit is a BRIDGE table linking facilities to units (document the bridge, not a standard parent/child)
- pvFacility.IDPa = EID (key for mart joins to well_360)
- pvFacilityMonthCalc is the primary mart target (calculated volumes by facility)
- pvFacRecDispCalc has no +parent tag — check tables/pvFacilityUnit.yaml for actual join column before writing

Critical anomalies for routes:
- pvRouteSet is a separate hierarchy root (not joined to pvUnit as parent)
- BUT the domain also contains pvUnitUserIDCalc and pvUnitDateListCalc — standard pvUnit children
- Document BOTH sub-hierarchies: pvUnit children AND pvRouteSet chain
- pvRouteSetRouteUnit → pvUnit is a cross-reference join (route stop points back to a unit)
```

#### Agent C Prompt Template

```
Task: Add relationships and key_patterns sections to:
1. context/sources/prodview/domains/fluid_analysis.yaml

Read order:
1. context/sources/prodview/domains/fluid_analysis.yaml (target)
2. context/sources/prodview/domains/completions.yaml (FORMAT REFERENCE ONLY)
3. context/sources/prodview/prodview.md lines 55-73

Critical structure — THREE sub-hierarchies (not two):
1. Completion-level links: pvUnitComp → pvUnitComp{Gas|HCLiq|Oil|Water}Analy → analysis group (FK pointer)
2. Unit-level links: pvUnit → pvUnitAnaly (unit-level analysis, e.g. for tank origin fluids)
3. Standalone analysis groups: pvGasAnaly → pvGasAnalysis → pvGasAnalysisComp → pvGasAnalysisCompCalc
   (shared: many completions/units can point to the same group; Gas/HCLiq/Oil/Water each have parallel sub-trees)

Document all three hierarchies explicitly. Use sub-headings in the relationships section to separate them.
Unit conversion note: pvGasAnalysis.GrossHeatMoistFree is in joules/m3 → pv_joules_to_mmbtu()
```

#### Agent D1 Prompt Template

```
Task: Add relationships and key_patterns sections to:
1. context/sources/prodview/domains/equipment.yaml
2. context/sources/prodview/domains/admin.yaml

Read order:
1. context/sources/prodview/domains/equipment.yaml (target)
2. context/sources/prodview/domains/admin.yaml (target)
3. context/sources/prodview/domains/completions.yaml (FORMAT REFERENCE ONLY)
4. context/sources/prodview/prodview.md lines 55-73

Critical anomaly for equipment:
- pvUnitCompressor is a pvUnit child (same level as pvUnitEquip), NOT related to pvUnitComp (completions)
- The pvUnitComp* prefix is used for completion-level tables — pvUnitCompressor breaks this pattern
- Document this naming confusion risk explicitly in key_patterns

Critical anomaly for admin:
- pvUnitAgreemt and pvUnitAgreemtPartner ARE standard pvUnit children (via idrecparent)
- pvApproval, pvApprovalStep, pvTask, pvRespTeam, pvPartner, pvPartnerContact have NO +parent annotation
  — these are standalone workflow/partner entities NOT anchored to pvUnit
- Document BOTH groups separately: pvUnit-rooted tables AND standalone reference entities
- Do NOT apply the standard idrecparent/idflownet join pattern to the standalone entities
```

#### Agent D2 Prompt Template

```
Task: Add relationships and key_patterns sections to:
1. context/sources/prodview/domains/reference.yaml

Read order:
1. context/sources/prodview/domains/reference.yaml (target)
2. context/sources/prodview/domains/allocations.yaml (FORMAT REFERENCE for multi-chain documentation)
3. context/sources/prodview/domains/completions.yaml (FORMAT REFERENCE for single-chain documentation)
4. context/sources/prodview/prodview.md lines 55-73

Critical structure — TWO root entities and 6 functional clusters:
ROOT 1 — pvUnit children (standard):
  - Events: pvUnitEvent → pvUnitEventEntry → pvUnitEventVol
  - Chemicals: pvUnitChemRecur + entry; pvUnitChemInt
  - Jobs/Costs: pvUnitJob, pvUnitCost (pvUnit children)
  - Checklists: pvUnitChkList → pvUnitChkListItem
  - Tags/Reference: pvUnitRemark, pvUnitOtherTag, pvUnitSeal → pvUnitSealEntry, pvUnitProb, pvUnitAssignOrig
  - Regulatory: pvUnitRegBody → pvUnitRegBodyKey → pvUnitRegBodySubmission

ROOT 2 — pvOpenState (SEPARATE ROOT, NOT a pvUnit child):
  - pvOpenState → pvOpenStateComp + pvOpenStateUnit + pvOpenStateTank + pvOpenStateCompLoad
  - pvOpenState is a production accounting period boundary entity (cumulative opening inventories)
  - Do NOT document pvOpenState children as pvUnit children — they are pvOpenState children

STANDALONE tables (no +parent): pvAttachment, pvOtherTag, pvComment
  - Note: pvOtherTag (global reference) vs pvUnitOtherTag (unit-scoped instance) — both exist

Organize the relationships section with sub-groups by cluster, not a flat list.
```

---

## Technical Considerations

- **Format reference:** `context/sources/prodview/domains/completions.yaml` — read TARGET domain FIRST to understand its structure, THEN read completions.yaml only for format wrapper
- **Join pattern source:** `context/sources/prodview/prodview.md` lines 55-73 — universal idrec+idflownet compound join
- **Verify FK column names:** Before writing any relationship, check the per-table YAML in `context/sources/prodview/tables/{TableName}.yaml` for the actual FK column name — never guess from table names (the ODA "drop entity prefix" pattern applies here too)
- **idflownet scope rule:** The compound `idrec + idflownet` join is required at the completion level and below. Unit-level children (pvUnit → pvUnitTank, pvUnit → pvUnitEquip) may use simpler joins — verify per domain
- **Fivetran dedup:** Context docs don't need to document the staging dedup pattern — point to prodview.md for ingestion details
- **File placement:** Always edit `context/sources/prodview/domains/{domain}.yaml` — root-level duplicates were deleted in Sprint 1
- **Preserve existing content:** Only ADD the two new sections; do not modify type legends or table lists
- **Parallel Write failure:** Each agent must write files one at a time (parallel Write calls cause cascading failures per MEMORY.md)

---

## Acceptance Criteria

### Correctness
- [ ] All 8 domain YAML files have a `# Relationships:` comment section
- [ ] All 8 domain YAML files have a `# Key patterns:` comment section
- [ ] `facilities` documents `pvFacility` as a separate root entity (NOT pvUnit child)
- [ ] `routes` documents both sub-hierarchies: `pvRouteSet` chain AND `pvUnit` calc children
- [ ] `admin` documents both groups: pvUnit-rooted tables AND standalone reference entities
- [ ] `reference` documents both root entities: pvUnit children AND pvOpenState sub-hierarchy
- [ ] `fluid_analysis` documents all THREE sub-hierarchies (completion-links, unit-links, standalone groups)
- [ ] `artificial_lift` documents the 1:1 extension pattern AND PCP/Jet asymmetry (no extension header)
- [ ] `tanks` documents `pvTicket` as a cross-network entity (NOT a standard pvUnitTank child)

### Format Quality
- [ ] Format matches Sprint 1 (completions.yaml) — same comment style, indentation, spacing
- [ ] Each domain's `key_patterns` section includes at least one domain-specific note beyond the universal `idrec/idflownet/idrecparent` boilerplate (e.g., mart grain, calc table warning, naming pitfall, cross-domain reference)
- [ ] All existing YAML content preserved unchanged

### Validation
- [ ] `yamllint context/sources/prodview/domains/` passes for all 8 modified files
- [ ] `dbt parse --warn-error --no-partial-parse` passes (no model regressions from unrelated changes)
- [ ] entity_model.md Sprint Coverage section updated: all 8 domains moved from TBD to Complete

### Completeness
- [ ] Commit + push to main

---

## Recommended Execution Sequence

Given effort and mart relevance, spawn agents in this order (all in parallel):

1. **Agent A** — `artificial_lift` + `tanks` (MEDIUM effort, HIGH relevance)
2. **Agent B** — `facilities` + `routes` (HIGH effort for facilities, anomalous roots)
3. **Agent C** — `fluid_analysis` (HIGH effort, 3 hierarchies)
4. **Agent D1** — `equipment` + `admin` (LOW-MEDIUM effort)
5. **Agent D2** — `reference` (HIGH effort, dual roots, 27 tables)

All 5 agents run simultaneously. After all complete, a single cleanup step: update entity_model.md Sprint Coverage to Complete.

---

## Dependencies & Risks

**Dependencies:**
- Sprint 1 complete ✓ — entity_model.md and 4 domain upgrades merged
- Pre-step required: fix entity_model.md Sprint 2 list before agent execution

**Risks:**
- `pvFacility` anomaly: agent might apply standard pvUnit pattern. Mitigation: explicit prompt template
- `pvOpenState` anomaly in reference: not obvious from YAML scan alone. Mitigation: explicit prompt template
- `pvTicket` in tanks: standalone entity will confuse agents. Mitigation: explicit call-out
- `admin` mixed-root: agent might apply idrecparent to standalone entities. Mitigation: explicit group separation in prompt
- `fluid_analysis` 3rd hierarchy: `pvUnitAnaly` is easily missed. Mitigation: listed explicitly in Agent C prompt
- Agent D2 (reference, 27 tables): largest single-agent task. Monitor for truncated output.

---

## References

### Sprint 1 Precedent
- `context/sources/prodview/domains/completions.yaml` — format reference (recently upgraded)
- `context/sources/prodview/domains/allocations.yaml` — format reference (multi-chain pattern)
- `context/sources/prodview/entity_model.md:330-349` — Sprint Coverage section to update

### Join Pattern Source
- `context/sources/prodview/prodview.md:55-73` — universal idrec+idflownet compound join

### Format Standards
- `context/sources/oda/domains/accounts_receivable.yaml` — ODA format reference

### Institutional Learnings Applied
- `docs/solutions/refactoring/oda-context-fk-column-naming-pattern.md` — verify FK column names against table YAMLs
- `docs/solutions/refactoring/oda-context-documentation-sprint-0.md` — swarm coordination: sequential writes, no in-team file writes
- `docs/solutions/refactoring/prodview-staging-5-cte-pattern.md` — existing staging models per domain listed
