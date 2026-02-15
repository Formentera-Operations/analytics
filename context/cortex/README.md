# Cortex Analyst Semantic Models

Snowflake Cortex Analyst semantic model definitions for natural-language querying of Formentera's analytics marts.

## Directory Structure

```
context/cortex/
├── README.md              # This file
├── drilling/              # Drilling & completions domain (2 models)
│   ├── drilling_cost_time.yaml       # Cost tracking, time logs, NPT
│   └── completions_safety.yaml       # Stim, AFEs, wellbores, safety
├── production/            # Production volumes & allocation (future)
├── finance/               # GL, AFEs, revenue/expense (future)
└── economics/             # EUR, NPV, type curves (future)
```

## Drilling Domain Models

The drilling domain is split into two focused models to stay within Snowflake's recommended 50-100 column limit per semantic model.

### drilling_cost_time.yaml (99 columns, 30 metrics)

Cost tracking, time accounting, and non-productive time analysis.

| Table | Role | Columns | Metrics |
|-------|------|---------|---------|
| `DIM_JOB` | Central dimension (trimmed) | 23 | 7 |
| `DIM_PHASE` | Phase plan vs actual | 17 | 4 |
| `FCT_DAILY_DRILLING_COST` | Cost line items | 17 | 4 |
| `FCT_DRILLING_TIME` | Activity time log | 22 | 7 |
| `FCT_NPT_EVENTS` | Problem events | 20 | 8 |

### completions_safety.yaml (99 columns, 29 metrics)

Completions, stimulation performance, AFE budgeting, wellbore profiles, and safety.

| Table | Role | Columns | Metrics |
|-------|------|---------|---------|
| `DIM_JOB` | Join target (slim) | 15 | 0 |
| `DIM_WELLBORE` | Wellbore characteristics | 18 | 3 |
| `BRIDGE_JOB_AFE` | Budget tracking | 15 | 4 |
| `FCT_STIMULATION` | Frac performance | 32 | 13 |
| `FCT_SAFETY_EVENTS` | HSE checks & incidents | 19 | 9 |

**DIM_JOB** appears in both models but is trimmed per domain. The cost/time model includes drilling-specific fields (rig info, depths, ROP). The completions/safety model includes only join keys, classification, and cost summary.

## Conventions

- Semantic models point to **mart-layer tables** (not staging or intermediate)
- Database/schema references use prod targets (`FO_PRODUCTION_DB.MARTS`)
- Each model should have 80-100 columns (dimensions + time_dimensions + measures)
- Verified queries should cover common business questions for each domain
- Synonyms should include oil & gas industry terminology
- Descriptions should be understandable by non-technical field engineers and analysts

## Deployment

Semantic models are uploaded to a Snowflake stage and referenced by Cortex Analyst:

```sql
-- Create stage for semantic models
CREATE STAGE IF NOT EXISTS cortex_analyst_stage
  DIRECTORY = (ENABLE = TRUE);

-- Upload drilling models
PUT file://context/cortex/drilling/drilling_cost_time.yaml
  @cortex_analyst_stage/drilling/
  AUTO_COMPRESS = FALSE
  OVERWRITE = TRUE;

PUT file://context/cortex/drilling/completions_safety.yaml
  @cortex_analyst_stage/drilling/
  AUTO_COMPRESS = FALSE
  OVERWRITE = TRUE;
```

## Linking to dbt Models

Each `base_table` in the YAML maps to a dbt mart model:

| Semantic Table | dbt Model | Semantic Model | Materialization |
|----------------|-----------|----------------|-----------------|
| `dim_job` | `models/operations/marts/drilling/dim_job.sql` | both (trimmed) | table |
| `dim_wellbore` | `models/operations/marts/drilling/dim_wellbore.sql` | completions_safety | table |
| `dim_phase` | `models/operations/marts/drilling/dim_phase.sql` | drilling_cost_time | table |
| `bridge_job_afe` | `models/operations/marts/drilling/bridge_job_afe.sql` | completions_safety | table |
| `fct_daily_drilling_cost` | `models/operations/marts/drilling/fct_daily_drilling_cost.sql` | drilling_cost_time | incremental |
| `fct_drilling_time` | `models/operations/marts/drilling/fct_drilling_time.sql` | drilling_cost_time | table |
| `fct_npt_events` | `models/operations/marts/drilling/fct_npt_events.sql` | drilling_cost_time | table |
| `fct_stimulation` | `models/operations/marts/drilling/fct_stimulation.sql` | completions_safety | table |
| `fct_safety_events` | `models/operations/marts/drilling/fct_safety_events.sql` | completions_safety | table |
