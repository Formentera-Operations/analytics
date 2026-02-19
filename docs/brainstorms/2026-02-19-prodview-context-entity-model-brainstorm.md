# Brainstorm: ProdView Context Docs — Entity Model & Domain YAML Alignment

**Date:** 2026-02-19
**Author:** Rob Stover
**Status:** Approved — proceed to planning

---

## What We're Building

Align ProdView's context documentation with the standard established for WellView and ODA:

1. **`entity_model.md`** — A new business-centric entity relationship document showing ProdView's core hierarchy, table relationships, cardinalities, and mart design guidance. Seeded from the existing `prodview.md` system overview (the core hierarchy tree is already documented there).

2. **Upgraded domain YAMLs** — Upgrade the existing lightweight domain YAML files from "table catalog with type legend" format to ODA format: add structured `relationships:` blocks, `key_patterns:` business notes, row counts, and join guidance.

### Scope by Sprint

**Sprint 1 (this work):** `entity_model.md` skeleton + top-4 domain YAML upgrades:
- `completions` (26 tables) — pvUnitComp grain + all completion-level measurements: downtime (pvUnitCompDownTm), tests (pvUnitCompTest), params (pvUnitCompParam), monthly calcs (pvUnitCompGathMonthCalc). This IS the production operations domain in ProdView.
- `allocations` (10 tables) — volume allocation math; two sub-chains: allocation (planned/actual) + distribution (downstream assignment)
- `meters` (18 tables) — 5 meter types × config/fact/ECF/daily-entry subtables; key input to allocation
- `flow_network` (12 tables) — network hierarchy and routing; needed to understand the FlowNet → Unit relationship at the top of the hierarchy

**Sprint 2 (follow-on):** Remaining 8 domain YAML upgrades:
- `admin`, `artificial_lift`, `equipment`, `facilities`, `fluid_analysis`, `reference`, `routes`, `tanks`

**Note:** `production_operations` does NOT exist in ProdView — that's a WellView domain. In ProdView, completion-level production measurements (downtime, tests, daily calcs) all live under `completions` as pvUnitComp-prefixed child tables.

---

## Why This Approach

### Two-track over sequential

The `entity_model.md` skeleton can be written from `prodview.md`'s existing Core Hierarchy section — the backbone is already there. Writing it first gives mart designers immediate navigation context without waiting for all 12 domain YAML upgrades to finish.

Domain YAML upgrades and the entity model reinforce each other: YAML upgrades add relationship depth that the entity model references; the entity model provides the orientation that makes individual YAML files navigable.

### Don't impose WellView's framing

WellView uses a "Physical Well / Well Work" split (asset-centric vs. event-centric). We deliberately chose **not** to pre-impose that framing on ProdView. ProdView's hierarchy is meaningfully different — the key entity is the **Unit** (a producing entity = well pad/lease), not the well itself. Let the table relationships define the natural groupings.

### Priority domains chosen by mart relevance

The top-4 domains cover the tables that underpin the most likely near-term mart work:
- `completions` → production volume facts (fct_well_production_monthly expansion)
- `production_operations` → downtime events, production tests, daily volumes
- `allocations` → volume allocation facts
- `meters` → meter reading time series

---

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| entity_model.md format | Markdown (same as WellView/ODA) | Matches established standard; rich narrative + ASCII trees |
| Domain YAML target format | ODA format (relationships + key_patterns) | More lightweight than WellView's; appropriate for ProdView's table density |
| Mental model framing | Discover from data, not imposed | ProdView's hierarchy differs from WellView's asset/event split |
| entity_model.md timing | Write skeleton first | prodview.md already has the core hierarchy; skeleton has immediate value |
| Sprint 1 domain scope | 4 of 12 domains | Avoid 12-file wall; deliver mart-relevant context now |
| Sprint 2 domain scope | Remaining 8 | Lower mart priority but needed for full alignment |

---

## ProdView Core Hierarchy (Input to entity_model.md)

The backbone already exists in `prodview.md`:

```
Flow Network (pvFlowNetHeader)
  └── Unit (pvUnit)                  ← "Unit" = well pad/lease/producing entity
       ├── Completion (pvUnitComp)   ← producing interval within a unit [MART GRAIN]
       │    ├── Status, Parameters, Production Tests, Downtimes
       │    ├── Artificial Lift (pvUnitCompPump)
       │    └── Targets
       ├── Tank (pvUnitTank)
       ├── Meters (5 types: Liquid, Gas PD, Orifice, Rate, Virtual)
       ├── Nodes (pvUnitNode) with Connections, Corrections, Volumes
       ├── Equipment & Compressors
       └── Allocations (Monthly/Daily)
```

**Critical join:** `pvUnit.IdRec` → `well_360.prodview_unit_id` (primary EID resolution path).
The `pvUnitComp` grain = the natural fact table grain for production volume data.

---

## Open Questions

1. **Natural entity partitions**: Once we read through all 12 domain files, do any natural "physical" vs. "operational" or "configuration" vs. "measurement" groupings emerge? The entity_model.md can reflect those rather than forcing a WellView-style split.

2. **Allocation table complexity**: The `allocations` domain likely has a chain of tables (monthly allocation header → allocation detail → allocation formula). Are these similar in structure to WellView's calc rollup tables, or simpler?

3. **Meter cardinality**: A single Unit can have multiple meter types (5 types listed in `prodview.md`). Is meter readings a separate entity model section, or a measurement input to allocations?

4. **Node tables**: `pvUnitNode` appears in the hierarchy but isn't clearly in any of the 12 domain files listed. Which domain covers it, and is it relevant to mart work?

5. **entity_model.md living doc strategy**: As domain YAMLs are upgraded in Sprint 2, should the entity_model.md be updated concurrently, or treated as a stable doc with an addendum?

---

## Files To Create / Modify

**New:**
- `context/sources/prodview/entity_model.md`

**Modified (add relationships + key_patterns to these 4 in Sprint 1):**
- `context/sources/prodview/domains/completions.yaml`
- `context/sources/prodview/domains/allocations.yaml`
- `context/sources/prodview/domains/meters.yaml`
- `context/sources/prodview/domains/flow_network.yaml`

*(Note: Files exist at both root level and in `domains/` subdirectory — currently duplicated. See "Domain File Duplication" section below.)*

---

## Priority Domain YAML Content Analysis

**Investigated 2026-02-19.** Summary of what the 4 priority domain files currently contain and what the upgrade requires:

### What's Already There (Reuse)

- **Core hierarchy + join patterns**: Already documented in `prodview.md` (lines 55-73) — IDREC/IDRECPARENT chain, IDFLOWNET compound key requirement, artificial lift extension→entry pattern. This is the source material for the domain `relationships:` blocks.
- **Type legend**: Present and complete in all domain files. Keep as-is.
- **Unit conversions**: Excellently documented in `prodview.md` (lines 79-101). Reference from entity_model.md.
- **Table lists**: Present in all 4 files. completions (21 tables), allocations (10), meters (18).

### What Needs to Be Added (Upgrade Work)

Each domain YAML needs two new structured sections:

1. **`relationships:`** block — domain-scoped join patterns. Source material is in `prodview.md`; needs to be extracted and scoped per domain.
2. **`key_patterns:`** block — primary key, parent FK, natural business keys for the domain.

**Completions domain scope**: pvUnitComp is the grain — 26 child tables cover tests, downtime, params, monthly calc rollups, commingled production, targets, zones. This is ProdView's equivalent of WellView's operations domain.

**Allocation domain complexity**: The allocation chain (pvUnitAllocMonth → pvUnitAllocMonthDay, pvUnitDistribMonth → pvUnitDistribMonthDay, pvUnitBalanceMonthCalc) is the most complex. Two sub-chains: allocation (planned/actual splits) and distribution (how volume is assigned downstream). Important for future allocation facts.

**Meters domain structure**: 5 meter types × 3-4 subtables each (config header + fact + ECF + daily entry). The `+ext,parent` tag in the YAML indicates an extension pattern (1:1 parent link) used for meter-type-specific fields.

**Flow network domain**: 12 tables covering the FlowNet → Unit → Node hierarchy and network connections. Needed for the entity model's top-level hierarchy section.

**Bottom line for upgrade effort**: The join patterns exist in `prodview.md` and just need to be moved into structured YAML blocks. This is a restructure + elaboration task, not a research task. ~1-2 hours per domain YAML.

---

## Domain File Duplication — Resolved

**Finding (investigated 2026-02-19):** All 12 domain YAML files exist at two paths:
- Root: `context/sources/prodview/{domain}.yaml`
- Canonical: `context/sources/prodview/domains/{domain}.yaml`

**History:** The original Feb 13 commit created files at root. A subsequent harness restructure commit (`78533f9`) added the `domains/` subdirectory and copied them — but never deleted the originals. All 12 pairs are byte-for-byte identical. Nothing actively references the root-level copies.

**Resolution:** Delete the 12 root-level domain YAMLs in Sprint 1 cleanup. `domains/` is the canonical location — matches WellView/ODA standard. Safe change with no downstream impact.

---

## Next Step

Run `/workflows:plan` to translate this into a sprint plan with atomic tasks and acceptance criteria.
