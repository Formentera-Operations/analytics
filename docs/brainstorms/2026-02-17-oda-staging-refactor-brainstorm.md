# ODA Staging Refactor — Brainstorm

**Date:** 2026-02-17
**Status:** Approved for planning
**Scope:** 46 ODA staging models → 5-CTE pattern + YAML docs + context documentation

---

## What We're Building

A full refactor of the ODA (Quorum OnDemand Accounting) staging layer to match the patterns established in the WellView and ComboCurve refactors. ODA is Formentera's accounting system — the source of truth for GL, AR, AP, AFEs, revenue/expense decks, and master data (companies, entities, vendors, owners).

### Current State

| Metric | Current | Target |
|--------|---------|--------|
| 5-CTE pattern | 2/46 (4%) | 46/46 |
| YAML docs/tests | 4/46 (9%) | 46/46 |
| Context docs | None | Full (system overview + per-table YAMLs + domain files) |
| Domain directories | Flat (all in `staging/oda/`) | 7 domain subdirectories |
| CDC soft-delete handling | 1/7 CDC tables (GL only) | All 7 CDC tables |
| Boolean `is_` prefix | Inconsistent | Consistent |
| Source references correct | ~35/46 | 46/46 |

### Downstream Impact

~30 downstream models consume ODA staging across:
- **Finance intermediates**: int_oda_gl, int_oda_ar_*, int_gl_enhanced, int_accounts_classified
- **Finance marts**: general_ledger, dim_accounts, dim_companies, dim_vendors, fct_ar_aging, etc.
- **Well 360**: int_well__oda, int_well__spine
- **Applications**: wiserock_app__companies

---

## Why This Approach

**Drivers (all four):** Consistency across all source staging layers, data quality (missing delete handling, untested models), onboarding/documentation, and foundation for new mart work.

**Approach: Context-first, domain-by-domain** — matches the proven WellView refactor pattern. Context docs first to inform the refactor; domain sprints keep PRs focused and reviewable.

**Sprint ordering: CDC-first** — the 7 true CDC tables are the highest data quality risk (6 are leaking soft-deleted records). Prioritize domains with CDC exposure, then batch-only domains.

---

## Key Decisions

### 1. Estuary Architecture: Three Systems, One Schema

The Estuary materialization puts three distinct systems into `ESTUARY_DB.ESTUARY_SCHEMA`:

| System | Capture | Tables | In Scope? |
|--------|---------|--------|-----------|
| **ODA** (Quorum OnDemand) | `FormenteraOps/ODA` + `FormenteraOps/ODA_BATCH` | 35 | **Yes** |
| **OpenInvoice** (SAP) | `FormenteraOps/openinvoice_data_batch` | 63 | No — separate project |
| **AFE Data** | `FormenteraOps/afe_data` | 11 | No — separate project |

Only ODA tables are in scope for this refactor.

### 2. ODA Has Two Connectors, Three Path Patterns

The naming inconsistency comes from `targetNaming: "prefixNonDefaultSchema"`:

| Connector | Estuary Path | Snowflake Table Naming | Tables | Delete Filtering |
|-----------|-------------|----------------------|--------|-----------------|
| **CDC** | `FormenteraOps/ODA/oda/*` | `oda_*` or `gl` | **7** | **Required** (`_operation_type != 'd'`) |
| **Batch (clean)** | `FormenteraOps/ODA_BATCH/oda/*` | `oda_*` | 16 | Not needed |
| **Batch (prefixed)** | `FormenteraOps/ODA_BATCH/oda_*` | `ODA_BATCH_oda_*` | 12 | Not needed |

**True CDC tables (only 7):**

| Snowflake Table | Rows | Staging Model | Filters Deletes? |
|----------------|------|---------------|-----------------|
| GL | 180,288,799 | stg_oda__gl | **Yes** (only one!) |
| ODA_APINVOICEDETAIL | 6,813,538 | stg_oda__apinvoicedetail | No |
| ODA_JIB | 63,210,996 | stg_oda__jib | No |
| ODA_JIBDETAIL | 63,213,576 | stg_oda__jibdetail | No |
| ODA_ARINVOICE_V2 | 436,113 | stg_oda__arinvoice_v2 | No |
| ODA_APINVOICE | 334,335 | stg_oda__apinvoice | No |
| ODA_ARINVOICEDETAIL | 611 | stg_oda__arinvoicedetail | No |

**Critical**: `_meta/op` column exists on BOTH CDC and batch tables. Batch tables only ever have `'c'` values (never `'d'`). The materialization config — not column presence — is the source of truth for connector type.

### 3. Source Reference Cleanup (~11 models)

Due to evolving Estuary configs, ~11 staging models reference OLD table names. Part of the refactor is re-pointing them to current materialization targets:

| Staging Model | Currently References (OLD) | Should Reference (CURRENT) |
|--------------|--------------------------|--------------------------|
| stg_oda__vendor_v2 | ODA_BATCH_ODA_VENDOR_V2 | ODA_VENDOR_V2 |
| stg_oda__apcheck | ODA_APCHECK | **Orphaned** — needs new source or removal |
| stg_oda__checkrevenue | ODA_CHECKREVENUE | **Orphaned** — needs new source or removal |
| stg_oda__company_v2 | ODA_BATCH_ODA_COMPANY_V2 | **Not in config** — investigate |
| stg_oda__owner_v2 | ODA_BATCH_ODA_OWNER_V2 | **Not in config** — investigate |
| stg_oda__wells | ODA_BATCH_ODA_WELL | **Not in config** — investigate |
| stg_oda__source_module | ODA_BATCH_ODA_SOURCEMODULE | **Not in config** — investigate |
| stg_oda__afe_v2 | ODA_BATCH_ODA_AFE_V2 | **Not in config** — investigate |
| stg_oda__interest_type | ODA_BATCH_ODA_INTERESTTYPE | **Not in config** — investigate |
| stg_oda__product | ODA_BATCH_ODA_PRODUCT | **Not in config** — investigate |
| stg_oda__expense_deck_revision | ODA_BATCH_ODA_EXPENSEDECKREVISIONVIEW | ODA_BATCH_ODA_EXPENSEDECKREVISION (no VIEW) |

Sprint 0 must produce the definitive mapping. Tables marked "not in config" may be from an older Estuary materialization that's still running — or they may be orphaned.

### 4. Duplicate/Legacy Tables

Several V1/V2 variants and orphaned tables exist in Snowflake:

| Legacy Table | Rows | Current Table | Rows | Action |
|-------------|------|--------------|------|--------|
| ODA_BATCH_ODA_ENTITY | 57,782 | ODA_BATCH_ODA_ENTITY_V2 | 56,024 | Use V2 |
| ODA_BATCH_ODA__OWNER | 42,255 | ODA_BATCH_ODA_OWNER_V2 | 42,040 | Use V2 |
| ODA_BATCH_ODA_OWNER | 0 | ODA_BATCH_ODA_OWNER_V2 | 42,040 | Use V2 |
| ODA_ARINVOICE | 0 | ODA_ARINVOICE_V2 | 436,113 | V1 empty |
| ODA_BATCH_ODA_ACCOUNT | 0 | ODA_BATCH_ODA_ACCOUNT_V2 | 1,998 | V1 empty |

### 5. Large Table Materialization

| Table | Rows | Connector | Current Mat. | Decision |
|-------|------|-----------|-------------|----------|
| ODA_REVENUEDECKPARTICIPANT | 196M | Batch | view | Flag for Sprint 3 (decks) — may need table |
| GL | 180M | CDC | view | **Keep as view** |
| ODA_JIB | 63M | CDC | view | Evaluate in Sprint 1 |
| ODA_JIBDETAIL | 63M | CDC | view | Evaluate in Sprint 1 |
| ODA_APINVOICEDETAIL | 6.8M | CDC | view | Keep as view |

### 6. GL Materialization: View

Keep `stg_oda__gl` as a view despite 180M rows. Snowflake handles the filtering at query time. Downstream intermediates do the heavy aggregation. Avoids incremental maintenance complexity.

### 7. Domain Organization (7 Domains)

| Domain | Directory | Models | CDC Tables |
|--------|-----------|--------|-----------|
| **Accounts Payable** | `accounts_payable/` | 3 | 3/3 (100%) |
| **Supporting** | `supporting/` | 7 | 2/7 (JIB, JIBDetail) |
| **Revenue & Expense Decks** | `decks/` | 8 | 0 (all batch) |
| **AFE/Budgeting** | `afe_budgeting/` | 4 | 0 (all batch) |
| **General Ledger** | `general_ledger/` | 5 | 1/5 (GL — already done) |
| **Master Data** | `master_data/` | 10 | 0 (all batch) |
| **Accounts Receivable** | `accounts_receivable/` | 9 | 2/9 (ARInvoice_V2, ARInvoiceDetail) |

### 8. Downstream Breaking Changes: Fix in Same PR

- Apply snake_case conventions in staging/intermediate
- Cascade renames into downstream models in the same PR (like CC refactor)
- ODA source columns are API-style (camelCase), not UI display names — minimal rename divergence expected
- If a mart model column name materially changes, alias for backward compatibility

### 9. Context Documentation: Reverse-Engineered First Pass

- No Quorum vendor docs available yet (being gathered separately)
- Build `context/sources/oda/` by inspecting Snowflake `information_schema.columns` + existing staging SQL + downstream usage
- Supplement with vendor docs when available

---

## Sprint Plan

### Sprint 0: Context Documentation + Source Audit
- **Source mapping**: Build definitive staging model → current source table mapping from materialization config
- **Connector audit**: Confirm which 7 tables are CDC vs batch (use materialization config, not `_meta/op` presence)
- **Orphan investigation**: Determine status of ~11 tables referenced by staging but not in current config (still being materialized by old config? stale?)
- Create `context/sources/oda/oda.md` — system overview, Estuary architecture (3 systems, 2 ODA connectors, path patterns)
- Create `context/sources/oda/_index.yaml` — table catalog with domain groupings + connector type + current vs legacy status
- Query Snowflake `information_schema.columns` for all ODA source tables
- Create per-table YAMLs in `context/sources/oda/tables/` (reverse-engineered column definitions)
- Create domain relationship files in `context/sources/oda/domains/`
- **Deliverable**: Complete context directory, definitive source mapping, orphan table report

### Sprint 1: Accounts Payable (3) + Supporting CDC (JIB/JIBDetail, 2) = 5 CDC models
- These 5 models are the highest CDC risk (AP invoices 6.8M + JIB 63M rows leaking deletes)
- Refactor to 5-CTE with CDC variant (filter `_operation_type != 'd'`)
- Update source references if needed (apcheck/checkrevenue are orphaned — resolve)
- Create domain YAML docs + tests
- Fix downstream consumers (int_oda_gl joins, finance marts)
- Organize into `accounts_payable/` directory + partial `supporting/`
- **Validates**: CDC 5-CTE pattern, source reference updates, downstream cascade

### Sprint 2: Remaining Supporting (5 batch) + Revenue & Expense Decks (8) = 13 models
- All batch — no delete filtering needed
- Revenue deck participant (196M rows) — evaluate materialization, flag if view is too slow
- Update source references (expense_deck_revision VIEW → non-VIEW, expense_deck_set, expense_deck_participant)
- Create domain YAML docs + tests
- Organize into `supporting/` and `decks/` directories

### Sprint 3: General Ledger (5) + AFE/Budgeting (4) = 9 models
- GL already has 5-CTE + tests — extend pattern to remaining 4 GL domain models
- AFE models: update source references (afe_v2 not in current config — resolve)
- Create/extend YAML docs + tests
- Preserve existing Elementary monitoring on GL
- Organize into `general_ledger/` and `afe_budgeting/` directories

### Sprint 4: Master Data (10) = 10 models
- Core reference tables (company, entity, vendor, owner, wells, purchaser, etc.)
- Highest downstream impact — most-referenced by finance intermediates/marts
- Update source references (~6 models point to old table names)
- Create comprehensive YAML docs + tests
- Fix downstream consumers (dim_companies, dim_vendors, dim_owners, int_well__oda, etc.)
- Organize into `master_data/` directory

### Sprint 5: Accounts Receivable (9) = 9 models
- 2 CDC tables (arinvoice_v2, arinvoicedetail) + 7 batch tables
- Most complex domain: invoice → detail → payment → adjustment → netting chains
- Create domain YAML docs + tests
- Fix downstream consumers (int_oda_ar_*, fct_ar_aging, dim_ar_summary)
- Organize into `accounts_receivable/` directory

---

## Open Questions

1. **~11 orphaned source references**: Which tables referenced by staging models are from old Estuary configs that are still running vs truly stale? Sprint 0 audit must resolve.
2. **ODA_APCHECK / ODA_CHECKREVENUE**: Confirmed old config — do we need to add these to the current materialization, or can downstream consumers live without them?
3. **MDM_CALENDAR**: From `FormenteraOps/ODA_BATCH/mdm/calendar` — shared MDM table. Is this ODA-specific or cross-system?
4. **EXPENSEDECKREVISIONVIEW vs EXPENSEDECKREVISION**: Current config has the non-VIEW version. Is the VIEW a Quorum convenience view with different columns?
5. **New tables without staging models**: `ODA_ARINVOICENETTEDDETAILLINEITEM` (861K), `ODA_BATCH_ODA_ARADJUSTMENTTYPE` (3), `ODA_BATCH_ODA_ARADVANCECLOSEOUTOWNER` (0) — add staging models?
6. **Revenue deck participant (196M rows)**: Batch table, currently a view — needs materialization evaluation in Sprint 2
7. **Vendor docs**: When Quorum data dictionary becomes available, how much rework will context docs need?
8. **Elementary monitoring**: Currently only on GL. Extend to all models or just key transactional tables?

---

## Success Criteria

- [ ] All 46 ODA staging models follow 5-CTE pattern (CDC or batch variant)
- [ ] All models have YAML column documentation + appropriate tests
- [ ] Context documentation complete at `context/sources/oda/`
- [ ] Models organized into 7 domain subdirectories
- [ ] All 7 CDC tables filter soft deletes (`_operation_type != 'd'`)
- [ ] All source references point to current materialization targets (no orphaned refs)
- [ ] All downstream models updated — zero broken refs
- [ ] `dbt build --select +stg_oda+` passes with 0 errors
- [ ] `python scripts/validate_staging.py` passes on all refactored models

---

*Next: Run `/workflows:plan` to generate detailed implementation plan starting with Sprint 0.*
