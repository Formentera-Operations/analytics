---
title: "ODA Context Documentation — Sprint 0 Source Audit Methodology"
category: refactoring
tags: [oda, estuary, context-docs, source-audit, sprint-0, information-schema, cdc, batch, swarm]
module: operations/staging/oda
symptoms:
  - 46 staging models with no context documentation
  - ~11 staging models referencing old/mismatched Snowflake table names
  - No definitive CDC vs batch classification
  - Multiple connector eras creating duplicate tables in same schema
  - Context-first sprint pattern not yet documented as repeatable methodology
date_solved: 2026-02-17
---

# ODA Context Documentation — Sprint 0 Source Audit Methodology

## Problem

ODA (Quorum OnDemand Accounting) had 46 staging models with zero context documentation, ~11 models referencing old table names from previous Estuary connector configurations, and no definitive mapping of which tables were CDC vs batch. The Estuary materialization compounds this by putting 3 separate systems (ODA, OpenInvoice, AFE Data) into a single Snowflake schema (`ESTUARY_DB.ESTUARY_SCHEMA`), with ODA itself having 3 different table naming patterns from connector evolution.

## Solution: Sprint 0 Context-First Pattern

Run a documentation-only sprint before any SQL model changes. This pattern has been validated across WellView, ComboCurve, and now ODA.

### Step 1: Query Snowflake for Ground Truth

```sql
-- Get all ODA tables with row counts
select table_name, row_count, last_altered, created
from estuary_db.information_schema.tables
where table_schema = 'ESTUARY_SCHEMA'
  and (table_name like '%ODA%' or table_name = 'GL' or table_name = 'MDM_CALENDAR')
order by table_name;
```

**Why this matters**: Context docs describe what a source system *can* expose. `information_schema` shows what actually exists in Snowflake. These diverge — Sprint 3 of the WellView refactor failed because we built 4 models against tables that don't exist.

### Step 2: Map Staging Models to Source Tables

Extract every `{{ source('oda', 'TABLE_NAME') }}` reference from staging SQL and cross-reference with the Snowflake query results:

```bash
# Extract all source references
grep -r "source('oda'," models/operations/staging/oda/ | grep -oP "source\('oda', '([^']+)'\)" | sort
```

For each reference, determine:
- **Does the table exist?** → Valid reference
- **Does it NOT exist?** → Orphaned (needs re-pointing or removal)
- **Are there NEW tables with no staging model?** → Flag for future sprints

### Step 3: Classify CDC vs Batch by Connector Config

**Critical gotcha**: The `_meta/op` column exists on BOTH CDC and batch tables in Estuary. Batch tables only ever have `'c'` values (never `'d'`). Use the materialization config path — not column presence — to classify:

| Connector Path | Snowflake Naming | Type | Delete Filtering |
|---------------|-----------------|------|-----------------|
| `FormenteraOps/ODA/oda/*` | `GL` or `ODA_*` | CDC | Required (`_operation_type != 'd'`) |
| `FormenteraOps/ODA_BATCH/oda/*` | `ODA_*` | Batch | Not needed |
| `FormenteraOps/ODA_BATCH/oda_*` | `ODA_BATCH_ODA_*` | Batch | Not needed |

### Step 4: Resolve Orphaned References

The ~11 "orphaned" references turned out to all point to tables that DO exist in Snowflake — they were from older Estuary materializations that are still running. Resolution categories:

| Category | Count | Action |
|----------|-------|--------|
| Table exists, actively updated | 8 | No change needed |
| Table exists, stale (old connector) | 2 | Keep for now, evaluate deprecation |
| Table exists, newer version available | 1 | Re-point in future sprint |

**Key learning**: "Not in current Estuary config" ≠ "doesn't exist in Snowflake". Old materializations may still be running or their tables may persist.

### Step 5: Generate Context Documentation

Query `information_schema.columns` for every in-scope table and generate:

1. **System overview** (`oda.md`) — architecture, hierarchy, ingestion patterns, source mapping, gotchas
2. **Per-table YAMLs** (`tables/*.yaml`) — column definitions with types and descriptions
3. **Domain files** (`domains/*.yaml`) — cross-table relationships and join patterns
4. **Table index** (`_index.yaml`) — catalog with domain groupings and metadata

Template: Follow the structure in `context/sources/combo_curve/` exactly.

## Estuary-Specific Gotchas Discovered

1. **Three connector eras in one schema** — Old ODA connector (stale since Jan 11), current CDC connector (7 tables), current batch connector (28 tables). `LAST_ALTERED` in `information_schema.tables` reveals staleness.

2. **Duplicate tables across eras** — `ODA_VENDOR_V2` (4,509 rows, new) AND `ODA_BATCH_ODA_VENDOR_V2` (4,006 rows, old) coexist. Use the newer, more complete version.

3. **ODA_GL vs GL** — Two GL tables exist. `GL` (180M, actively updated by CDC) is correct. `ODA_GL` (135M, stale) is from a different connector path.

4. **V1/V2 variant rule** — Use V2 where both exist and V2 has data. V1 tables are typically empty (0 rows) or superseded.

5. **Naming pattern collision** — Both CDC and batch "clean path" tables use `ODA_*` prefix, making them indistinguishable by name alone. Must use connector config or `LAST_ALTERED` freshness.

## Swarm Agent Coordination Learnings

Sprint 0 was executed using a 4-agent parallel swarm. Key learnings:

1. **Sequential file writes are mandatory** — Parallel `Write` calls from different agents cause cascading "Sibling tool call errored" failures. Each agent must write files one at a time.

2. **Delegate mode restricts all agents on a team** — Teammates spawned within a team inherit delegate mode and lose access to Bash, Read, Write, etc. The commit/push agent must be spawned standalone (outside the team) or the team must be dissolved first.

3. **Embed audit data in spawn prompts** — Since agents can't share files in real-time, pass the Snowflake query results directly in each agent's initial prompt rather than writing to a shared file.

4. **4-agent decomposition worked well for docs**:
   - Agent 1: System overview doc (completed first, ~5 min)
   - Agent 2: Table YAMLs batch 1 — 17 tables (~8 min)
   - Agent 3: Table YAMLs batch 2 — 29 tables (~12 min)
   - Agent 4: Domain files + index — 8 files (~10 min)

## Prevention / Best Practices

- **Always run Sprint 0 before model refactors** — documentation-first prevents building against nonexistent tables
- **Always query `information_schema.tables`** before assuming a source table exists — `dbt parse` validates syntax, not Snowflake object existence
- **Use `LAST_ALTERED` to detect stale tables** — tables from old connector eras may exist but not be actively refreshed
- **Classify CDC by connector config, not column presence** — `_meta/op` exists on both CDC and batch Estuary tables

## Results

| Deliverable | Count |
|------------|-------|
| System overview | 1 file (370 lines) |
| Per-table YAMLs | 46 files |
| Domain relationship files | 7 files |
| Table index | 1 file |
| **Total** | **55 files, 2,804 lines** |

PR: #269 (documentation-only, no SQL changes)
Linear: FOR-259, FOR-260, FOR-261, FOR-262, FOR-263
