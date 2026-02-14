---
title: "Simplify EID Resolution in WellView Models via well_360.wellview_id"
category: refactoring
tags: [drilling-dimensions, entity-resolution, well-360, wellview, eid, join-simplification]
module: operations/marts/drilling
symptoms:
  - Duplicated 2-CTE well_header + wells pattern across multiple models
  - Two left joins needed to resolve WellView GUID to EID
  - stg_wellview__well_header dependency pulled into models that only need EID
date_solved: 2026-02-13
---

# Simplify EID Resolution in WellView Models via well_360.wellview_id

## Problem

WellView staging models use GUIDs (32-char hex strings from the `idwell` column) as well identifiers. The analytics layer needs 6-character EIDs (from ODA/ProdView) as the canonical well FK. The initial implementation used a 2-hop join pattern that was duplicated across models.

### Old Pattern (2-hop, 2 CTEs)

```sql
well_header as (
    select
        "Well ID" as well_id,
        EID as eid
    from {{ ref('stg_wellview__well_header') }}
    where
        EID is not null
        and len(EID) = 6
),

wells as (
    select eid from {{ ref('well_360') }}
),

-- In the joined CTE:
    from source_table as s
    left join well_header as wh
        on s.well_id = wh.well_id     -- hop 1: GUID -> EID
    left join wells as w
        on wh.eid = w.eid             -- hop 2: validate EID exists
```

**Problems:**
- 2 CTEs and 2 joins for a single FK resolution
- Pulls `stg_wellview__well_header` into the DAG unnecessarily
- Duplicated identically in every model needing EID
- The `len(EID) = 6` filter belongs in the entity layer, not every consumer

## Root Cause

`well_360` already stores the WellView GUID as `wellview_id`. This column is populated by `int_well__wellview`, which extracts and deduplicates well IDs from `stg_wellview__well_header`. The intermediate hop through `stg_wellview__well_header` is redundant because `well_360` already did that work.

## Solution

Join directly to `well_360` using the `wellview_id` column:

```sql
well_360 as (
    select
        wellview_id,
        eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

-- In the joined CTE:
    from source_table as s
    left join well_360 as w360
        on s.well_id = w360.wellview_id   -- single hop: GUID -> EID
```

### Key Discovery

`well_360.wellview_id` stores the exact same GUID values as `stg_wellview__jobs."Well ID"` and `stg_wellview__wellbores.well_id`. Both originate from WellView's `idwell` column.

## Verification

```sql
-- Confirm join produces identical results
select
    'old_pattern' as method,
    count(*) as total,
    count(eid) as with_eid
from dim_job  -- before refactoring
union all
select
    'new_pattern',
    count(*),
    count(w360.eid)
from stg_wellview__jobs as j
left join well_360 as w360 on j."Well ID" = w360.wellview_id
-- Result: both return 42,823 total, 16,022 with EID
```

Verified for:
- `dim_job`: 42,823 rows, 16,022 with EID
- `dim_wellbore`: 6,528 rows, 2,430 with EID

## Models Refactored

| Model | Change |
|-------|--------|
| `dim_job` | Replaced well_header + wells with well_360 |
| `dim_wellbore` | Replaced well_header + wells with well_360 |

## Models Still Using Old Pattern (Future Refactoring)

| Model | Priority |
|-------|----------|
| `int_wellview_job` | High -- directly joins stg_wellview__well_header |
| `int_wellview__well_header` | High -- the old intermediate doing 2-hop joins |
| `int_wellview__canonical_wells` | High -- uses the old intermediate |

## Prevention

When building new WellView mart models that need EID:
1. Join to `well_360` on `wellview_id` (not through `stg_wellview__well_header`)
2. Filter `where wellview_id is not null` in the CTE
3. Select only `wellview_id` and `eid` to keep the CTE narrow

## Related

- `models/operations/marts/well_360.sql` -- the canonical well entity model
- `models/operations/intermediate/well_360/int_well__wellview.sql` -- sources wellview_id
- `context/sources/wellview/wellview.md` -- WellView data model reference
- `docs/plans/sprint-1-implementation-script.md` -- Sprint 1 implementation guide
