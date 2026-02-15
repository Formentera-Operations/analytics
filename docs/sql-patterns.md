# Preferred SQL Patterns (Snowflake)

Use these modern SQL features instead of verbose workarounds. All are Snowflake-supported.

## QUALIFY — filter on window function results

Eliminates the CTE-wrap-and-filter pattern. Use for deduplication, top-N-per-group, and any window-based filtering.

```sql
-- Dedup: keep latest record per entity (preferred)
select *
from source_table
qualify 1 = row_number() over (partition by entity_id order by updated_at desc)

-- Instead of the verbose CTE wrapper:
-- with ranked as (select *, row_number() over (...) as rn from ...) select * from ranked where rn = 1
```

This is our standard dedup pattern in staging models. Always prefer `qualify` over a wrapping CTE when filtering window results.

## IS [NOT] DISTINCT FROM — NULL-safe comparison

Treats NULL as a comparable value. Use for CDC change detection, nullable column joins, and merge logic.

```sql
-- Change detection in incremental/merge logic
where old.value is distinct from new.value

-- NULL-safe join (both sides can be NULL)
on table_a.nullable_key is not distinct from table_b.nullable_key

-- Instead of: CASE WHEN a IS NULL AND b IS NULL THEN ... WHEN a IS NULL OR b IS NULL THEN ... ELSE a != b END
```

## ASOF JOIN — point-in-time lookups

Matches each row to the closest row in another table by timestamp/date. Use for effective-dated lookups: revenue deck revisions, expense deck effective dates, interest rate history, market pricing.

```sql
-- Get the effective revenue deck for each GL entry
select
    gl.*,
    deck.revision_number,
    deck.effective_date
from gl_entries as gl
    asof join revenue_deck_history as deck
        match_condition (gl.journal_date >= deck.effective_date)
        on gl.entity_id = deck.entity_id
```

Snowflake uses `MATCH_CONDITION` syntax (not inequality in `ON` clause). Falls back to `qualify row_number()` pattern if ASOF is awkward for the use case.

## RANGE window frames — value-based boundaries

Use `RANGE` instead of `ROWS` when window boundaries should be based on value distance (especially time intervals) rather than row count.

```sql
-- 30-day rolling average production (by date value, not row count)
select
    production_date,
    oil_volume,
    avg(oil_volume) over (
        order by production_date
        range between interval '30 days' preceding and current row
    ) as rolling_30d_avg_oil
from daily_production
```

`ROWS BETWEEN 30 PRECEDING` gives exactly 30 rows; `RANGE BETWEEN INTERVAL '30 days' PRECEDING` gives all rows within 30 calendar days — correct even with gaps in production data.
