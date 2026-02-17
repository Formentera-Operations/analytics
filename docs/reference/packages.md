# dbt Packages

| Package | Version | Use For |
|---------|---------|---------|
| `dbt_utils` | 1.3.2 | `surrogate_key`, `star`, test utilities |
| `dbt_expectations` | 0.10.10 | Advanced data quality tests |
| `elementary` | 0.21.0 | Data observability, anomaly detection |
| `dbt_snow_mask` | 0.2.7 | Column-level masking policies (prod only) |

Do not add packages without discussing with the team first.

Versions are pinned in `packages.yml`. Run `dbt deps` after any changes.
