---
title: "refactor(oda): Sprint 3 — GL Lookups + Master Data (14 models)"
type: refactor
date: 2026-02-17
sprint: 3
series: oda-staging-refactor
---

# ♻️ ODA Staging Refactor — Sprint 3: GL Lookups + Master Data (14 models)

## Overview

Refactor 14 ODA staging models from the legacy 2-CTE pattern to the standard 5-CTE pattern. All 14 are batch tables (no CDC, no delete filtering). Create two new domain subdirectories (`general_ledger/`, `master_data/`), add YAML column documentation + tests, and update downstream consumers.

These 14 models are the **most heavily referenced** staging models in ODA — company_v2 alone has 16 downstream consumers. Column names are already snake_case, so downstream impact is limited to audit column renames (`create_date` → `created_at`, `update_date` → `updated_at`) and boolean conversions.

**Branch:** `feature/oda-staging-refactor-sprint-3`
**Base:** `main` (after Sprint 2 PR #274 merge)

## Models in Scope

### GL Lookups (4 models → new `general_ledger/` directory)

| # | Model | Source Table | Rows | Downstream Refs |
|---|-------|-------------|------|----------------|
| 1 | stg_oda__voucher_v2 | ODA_VOUCHER_V2 | 370K | 12 |
| 2 | stg_oda__gl_reconciliation_type | ODA_GLRECONCILIATIONTYPE | 8 | 2 |
| 3 | stg_oda__account_types | ODA_ACCOUNTTYPE | 2 | 2 |
| 4 | stg_oda__account_sub_types | ODA_BATCH_ODA_ACCOUNTSUBTYPE | 5 | 2 |

### Master Data (10 models → new `master_data/` directory)

| # | Model | Source Table | Rows | Downstream Refs |
|---|-------|-------------|------|----------------|
| 5 | stg_oda__company_v2 | ODA_BATCH_ODA_COMPANY_V2 | 39 | 16 |
| 6 | stg_oda__entity_v2 | ODA_BATCH_ODA_ENTITY_V2 | 56K | 15 |
| 7 | stg_oda__vendor_v2 | ODA_BATCH_ODA_VENDOR_V2 | 4K | 5 |
| 8 | stg_oda__owner_v2 | ODA_BATCH_ODA_OWNER_V2 | 42K | 10 |
| 9 | stg_oda__purchaser_v2 | ODA_BATCH_ODA_PURCHASER_V2 | 183 | 3 |
| 10 | stg_oda__wells | ODA_BATCH_ODA_WELL | 9.4K | 13 |
| 11 | stg_oda__account_v2 | ODA_BATCH_ODA_ACCOUNT_V2 | 2K | 6 |
| 12 | stg_oda__interest_type | ODA_BATCH_ODA_INTERESTTYPE | 5 | 2 |
| 13 | stg_oda__product | ODA_BATCH_ODA_PRODUCT | 9 | 1 |
| 14 | stg_oda__payment_type | ODA_BATCH_ODA_PAYMENTTYPE | 8 | 4 |

## Technical Approach

### 5-CTE Pattern (batch, no CDC)

Every model follows the pattern from `docs/conventions/staging.md`:

```sql
{{ config(materialized='view', tags=['oda', 'staging', 'formentera']) }}

{# Staging model for ODA [Entity]. Source: [TABLE] (Estuary batch, ~N rows). Grain: One row per [entity] (id). #}

with
source as (
    select * from {{ source('oda', 'TABLE_NAME') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        -- ... type-cast columns grouped by category ...
        -- flags (boolean conversions)
        coalesce(IS_ACTIVE = 1, false) as is_active,
        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,
        -- ingestion metadata
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at
    from source
),

filtered as (
    select * from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as {model}_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- explicit column list grouped by category (no SELECT *)
    from enhanced
)

select * from final
```

### Directory Structure

```
models/operations/staging/oda/
├── accounts_payable/          ← Sprint 1 (done)
├── supporting/                ← Sprints 1-2 (done)
├── decks/                     ← Sprint 2 (done)
├── general_ledger/            ← NEW Sprint 3
│   ├── stg_oda__voucher_v2.sql
│   ├── stg_oda__gl_reconciliation_type.sql
│   ├── stg_oda__account_types.sql
│   ├── stg_oda__account_sub_types.sql
│   ├── _src_oda__general_ledger.yml
│   └── _stg_oda__general_ledger.yml
├── master_data/               ← NEW Sprint 3
│   ├── stg_oda__company_v2.sql
│   ├── stg_oda__entity_v2.sql
│   ├── stg_oda__vendor_v2.sql
│   ├── stg_oda__owner_v2.sql
│   ├── stg_oda__purchaser_v2.sql
│   ├── stg_oda__wells.sql
│   ├── stg_oda__account_v2.sql
│   ├── stg_oda__interest_type.sql
│   ├── stg_oda__product.sql
│   ├── stg_oda__payment_type.sql
│   ├── _src_oda__master_data.yml
│   └── _stg_oda__master_data.yml
├── src_oda.yml                ← Remove 14 table entries
└── (remaining models stay until Sprint 4+)
```

### Downstream Consumer Impact

**CRITICAL: Business column names are already snake_case.** Impact is limited to:

1. **Audit column renames** (if any downstream model references `create_date`/`update_date` directly):
   - `create_date` → `created_at`
   - `update_date` → `updated_at`

2. **New columns added** (non-breaking): `_loaded_at`, surrogate keys

3. **Boolean conversions** (if downstream compares raw integers):
   - Check `dim_ap_check_register` patterns from Sprint 1 fix

**Downstream consumers to verify** (grepped `ref()` calls):

| Consumer | Models Referenced | Risk Level |
|----------|-----------------|------------|
| int_general_ledger_enhanced | 10 of 14 | HIGH — verify all joins |
| int_gl_enhanced | 10 of 14 | HIGH — mirror of above |
| int_oda_wells | wells, company_v2 | MEDIUM — uses ALL CAPS refs |
| dim_wells | wells | MEDIUM — extensive classification logic |
| dim_ap_check_register | vendor_v2, entity_v2, owner_v2, purchaser_v2, company_v2 | MEDIUM |
| dim_revenue_check_register | entity_v2, owner_v2, company_v2 | LOW |
| int_oda_latest_company_NRI | interest_type | LOW — currently fails (missing model) |
| int_oda_latest_company_WI | interest_type | LOW — currently fails (missing model) |
| dim_accounts | account_v2, account_types, account_sub_types | LOW |
| dim_companies | company_v2 | LOW |
| dim_vendors | vendor_v2, entity_v2 | LOW |
| dim_owners | owner_v2 | LOW |
| dim_purchasers | purchaser_v2, entity_v2 | LOW |
| dim_entities | entity_v2 | LOW |
| int_well__oda | wells | LOW |
| general_ledger | voucher_v2 | LOW |

## Implementation Phases

### Phase 1: Directory Structure + Source YAML Reorganization
- [ ] Create `general_ledger/` and `master_data/` directories
- [ ] Create `_src_oda__general_ledger.yml` with 4 table entries (split-source pattern: `name: oda`)
- [ ] Create `_src_oda__master_data.yml` with 10 table entries
- [ ] `git mv` all 14 SQL files to their new directories
- [ ] Remove 14 table entries from `src_oda.yml`
- [ ] Verify `dbt parse` passes (no broken refs)
- [ ] Commit: `refactor(oda): Sprint 3 Phase 1 — directory structure + source YAML reorganization`

### Phase 2: GL Lookup Models (4 models)
- [ ] Refactor `stg_oda__voucher_v2` to 5-CTE (370K rows, batch)
- [ ] Refactor `stg_oda__gl_reconciliation_type` to 5-CTE (8 rows, batch)
- [ ] Refactor `stg_oda__account_types` to 5-CTE (2 rows, batch)
- [ ] Refactor `stg_oda__account_sub_types` to 5-CTE (5 rows, batch)
- [ ] Load context: `context/sources/oda/tables/ODA_VOUCHER_V2.yaml`, `ODA_GLRECONCILIATIONTYPE.yaml`, `ODA_ACCOUNTTYPE.yaml`, `ODA_BATCH_ODA_ACCOUNTSUBTYPE.yaml`
- [ ] Write `_stg_oda__general_ledger.yml` with column docs + tests for all 4 models

### Phase 3: Master Data — Small Lookups (5 models)
- [ ] Refactor `stg_oda__interest_type` to 5-CTE (5 rows — unblocks NRI/WI!)
- [ ] Refactor `stg_oda__product` to 5-CTE (9 rows)
- [ ] Refactor `stg_oda__payment_type` to 5-CTE (8 rows)
- [ ] Refactor `stg_oda__purchaser_v2` to 5-CTE (183 rows)
- [ ] Refactor `stg_oda__account_v2` to 5-CTE (2K rows)
- [ ] Load context: per-table YAMLs from `context/sources/oda/tables/`
- [ ] Write `_stg_oda__master_data.yml` (partial — 5 models)

### Phase 4: Master Data — Core Dimensions (5 models)
- [ ] Refactor `stg_oda__company_v2` to 5-CTE (39 rows, 16 downstream refs!)
- [ ] Refactor `stg_oda__entity_v2` to 5-CTE (56K rows, entity.code is NUMBER)
- [ ] Refactor `stg_oda__vendor_v2` to 5-CTE (4K rows)
- [ ] Refactor `stg_oda__owner_v2` to 5-CTE (42K rows)
- [ ] Refactor `stg_oda__wells` to 5-CTE (9.4K rows, 13 downstream refs)
- [ ] Load context: per-table YAMLs
- [ ] Extend `_stg_oda__master_data.yml` (remaining 5 models)

### Phase 5: Downstream Consumer Updates
- [ ] Grep ALL downstream consumers for audit column references (`create_date`, `update_date`)
- [ ] Update any references to renamed audit columns
- [ ] Verify `int_oda_wells` works (uses ALL CAPS Snowflake-resolved refs — should still work)
- [ ] Verify `dim_wells` works (extensive basin/classification logic)
- [ ] Verify `int_general_ledger_enhanced` works (refs 10 of 14 models)
- [ ] Verify `int_gl_enhanced` works (mirror of above)
- [ ] Verify `dim_ap_check_register` works
- [ ] Verify `int_oda_latest_company_NRI` works (now that interest_type exists!)
- [ ] Verify `int_oda_latest_company_WI` works (now that interest_type exists!)
- [ ] Verify all other downstream consumers

### Phase 6: Validation + Commit
- [ ] Run `python scripts/validate_staging.py` on all 14 models
- [ ] Run `sqlfluff lint` on all modified files
- [ ] Run `dbt parse --warn-error --no-partial-parse`
- [ ] Run `dbt build --select` on all 14 staging models (verify 0 errors)
- [ ] Run `dbt build --select` on all downstream consumers
- [ ] Fix any pre-commit hook failures (sqlfluff auto-fix + manual noqa)
- [ ] Commit and push
- [ ] Create PR on `feature/oda-staging-refactor-sprint-3`

## Known Gotchas (from Sprints 0-2)

| Gotcha | Mitigation |
|--------|-----------|
| sqlfluff ST06 reorders boolean conversions | Add `-- noqa: ST06` on renamed CTE `select` line |
| Snowflake reserved words (WEEK, DATE, etc.) | Double-quote + `-- noqa: RF06`. Check `information_schema.columns` for actual types |
| CI --defer stale column names | Transient after merge — prod rebuild fixes it |
| Join chain NULL propagation | Use `left join` throughout if FK can be NULL |
| FLOW_DOCUMENT exclusion | Always exclude large JSON metadata column |
| entity_v2.code is NUMBER | Downstream casts to VARCHAR — preserve NUMBER type in staging |
| Tag order | Must be `['oda', 'staging', 'formentera']` |
| dbt 1.11+ YAML syntax | Use `arguments:` for relationships, `config:` for severity |
| `data_tests:` not `tests:` | Deprecated in 1.5+ |
| Split-source YAML | Each `_src_oda__*.yml` uses `name: oda` (shared source name) |

## Acceptance Criteria

- [ ] All 14 models follow 5-CTE pattern (source, renamed, filtered, enhanced, final)
- [ ] No CDC filtering — filtered CTE: `WHERE id IS NOT NULL`
- [ ] All models: `materialized='view'`, `tags=['oda', 'staging', 'formentera']`
- [ ] Surrogate key + `_loaded_at` in enhanced CTE
- [ ] Explicit column list in final CTE (no `SELECT *`)
- [ ] Models in `general_ledger/` (4) and `master_data/` (10) subdirectories
- [ ] Source entries moved from `src_oda.yml` to domain-specific YAML files
- [ ] YAML documentation with tests for all 14 models
- [ ] All downstream consumers updated — zero broken refs
- [ ] `entity_v2.code` NUMBER type preserved
- [ ] `stg_oda__interest_type` buildable (unblocks NRI/WI)
- [ ] `validate_staging.py` passes on all 14 models
- [ ] `dbt build` passes with 0 errors on all 14 + downstream consumers
- [ ] `-- noqa: ST06` on renamed CTEs with boolean conversions
- [ ] PR created on `feature/oda-staging-refactor-sprint-3`

## References

### Sprint Reference Models
- `models/operations/staging/oda/accounts_payable/stg_oda__apcheck.sql` — batch 5-CTE reference
- `models/operations/staging/oda/supporting/stg_oda__source_module.sql` — tiny lookup reference

### Context Files
- `context/sources/oda/domains/general_ledger.yaml` — GL domain relationships
- `context/sources/oda/domains/master_data.yaml` — Master data relationships
- `context/sources/oda/tables/*.yaml` — Per-table column definitions (14 files)

### Conventions
- `docs/conventions/staging.md` — 5-CTE pattern, tag schema, type casting
- `docs/conventions/testing.md` — YAML test patterns

### Institutional Learnings
- `docs/solutions/build-errors/sqlfluff-st06-breaks-comment-grouping.md` — ST06 noqa pattern
- `docs/solutions/build-errors/snowflake-reserved-word-column-cast-failure.md` — Reserved word quoting
- `docs/solutions/build-errors/ci-defer-stale-column-names.md` — CI defer failures
- `docs/solutions/logic-errors/inner-join-after-left-join-drops-null-rows.md` — Join chain NULLs
- `docs/solutions/refactoring/oda-context-documentation-sprint-0.md` — Source audit methodology

### Prior PRs
- PR #269 — Sprint 0: Context documentation
- PR #270 — Sprint 1: AP + Supporting CDC (5 models)
- PR #274 — Sprint 2: Remaining Supporting + Decks (13 models)
