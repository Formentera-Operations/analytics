# Formentera Analytics

dbt project for Formentera Operations (FO) and Formentera Partners (FP) analytics.

## Getting Started with Paradime

This repository is designed to be used via [Paradime](https://www.paradime.io/). Follow these steps to set up your development environment.

### Snowflake Connection Setup

When configuring your Snowflake connection in Paradime, use the following settings:

| Setting | FO Members | FP Members |
|---------|------------|------------|
| **Database** | `FO_DEV_DB` | `FP_DEV_DB` |
| **Schema** | `DBT_{USERNAME}` (e.g., `DBT_MIGUEL`) | `DBT_{USERNAME}` (e.g., `DBT_MIGUEL`) |
| **Warehouse** | `FO_DEV_WH_XS` | `FP_DEV_WH_XS` |
| **Role** | Use your assigned role (see below) | Use your assigned role (see below) |

**Roles:** Use the role that was assigned to you. Only Analyst, Engineer, and Admin roles have write access for development:
- FO: `FO_ANALYST_ROLE`, `FO_ENGINEER_ROLE`, `FO_ADMIN_ROLE`
- FP: `FP_ANALYST_ROLE`, `FP_ENGINEER_ROLE`, `FP_ADMIN_ROLE`

> ⚠️ Reader roles (e.g., `FO_READER_ROLE`) will not work as they lack write permissions.

> **Note:** Dev models and seeds are written to a single database (`FO_DEV_DB` or `FP_DEV_DB`) regardless of the model layer (staging, marts, etc.). This is driven by the database variable in your Snowflake connection.

## Project Structure

```
models/
├── operations/          # Formentera Operations (Tenant 1)
│   ├── staging/         # Cleaned data, 1:1 with source
│   ├── intermediate/    # Business logic layers (ephemeral)
│   ├── marts/           # Business-facing data models
│   └── applications/    # Application-specific models
└── partners/            # Formentera Partners (Tenant 2)
    ├── staging/
    ├── intermediate/
    └── marts/
```

## CI/CD Pipeline

When you open a Pull Request, a CI pipeline automatically validates your changes.

### Workflow Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Create Branch  │ ──▶ │  Make Changes   │ ──▶ │  Open PR        │ ──▶ │  CI Pipeline    │
│  (from main)    │     │  & Push         │     │  in Paradime    │     │  Runs           │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                                                 │
                        ┌─────────────────┐     ┌─────────────────┐              │
                        │  Merged to      │ ◀── │  PR Approved    │ ◀────────────┘
                        │  main           │     │  & Merged       │
                        └─────────────────┘     └─────────────────┘
```

### Branch Naming Conventions

- `feat/short-description` — for new features or models (e.g., `feat/add-well-360-model`)
- `fix/short-description` — for bug fixes (e.g., `fix/prodview-null-handling`)

> ⚠️ **Important:** Always create a new branch for each piece of work. Do not reuse existing feature branches for unrelated work.

### What the CI Pipeline Does

| Step | Description |
|------|-------------|
| **Setup** | Installs Python, dbt, and project dependencies |
| **Parse & Validate** | Runs `dbt parse --warn-error` to validate project configuration |
| **Generate Prod Artifacts** | Checks out `main` branch and generates production manifest for comparison |
| **Detect Modified Models** | Uses `state:modified+` to identify changed models and their downstream dependencies |
| **Initialise Elementary** | Sets up Elementary for data observability |
| **Build (Full Refresh)** | Runs `dbt build --full-refresh` on modified models to test full table rebuilds |
| **Build (Incremental)** | Runs `dbt build` on modified models to test incremental logic |

### CI Databases and Schemas

The pipeline writes all modified models to isolated CI environments:

| Tenant | CI Database | CI Schema |
|--------|-------------|-----------|
| Operations | `FO_CI_DB` | `DBT_CI_<PR_NUMBER>` |
| Partners | `FP_CI_DB` | `DBT_CI_<PR_NUMBER>` |

For example, PR #179 would write to `FO_CI_DB.DBT_CI_179`.

### How `--defer` Works

The pipeline uses `--defer --state prod-artifacts/ --favor-state` which means:
- **Only modified models are built** in CI
- **Unmodified upstream models** are referenced from production (read-only)
- This makes CI fast and cost-effective

### Viewing CI Data

You can query the CI schema directly in Snowflake to inspect the data:

```sql
USE DATABASE FO_CI_DB;  -- Or FP_CI_DB
USE SCHEMA DBT_CI_179;  -- Replace with your PR number

SELECT * FROM <model_name> LIMIT 100;
```

### Checking Pipeline Results

1. Go to the **GitHub Actions** tab in the repository
2. Find the workflow run for your PR
3. Click into the job to see detailed logs

### Merging

Once the CI pipeline passes:
1. Request a review from a team member
2. Address any feedback and get approval
3. Click **Merge** in Paradime/GitHub
4. Delete your feature branch to keep the repository clean

> **Note:** PRs should not be merged until the CI pipeline passes and approval is granted.

## Resources

- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [dbt CI/CD Pipeline Handbook](https://www.notion.so/tasman/dbt-CICD-Pipeline-Handbook-2d9fc68e836980129452e1a8014c156e) (internal)
