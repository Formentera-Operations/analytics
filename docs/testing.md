# Testing Patterns

## Test Types in Use

| Type | Package | Examples |
|------|---------|---------|
| **Generic tests** | dbt core | `unique`, `not_null`, `relationships`, `accepted_values` |
| **dbt_expectations** | `dbt_expectations` | `expect_column_values_to_be_between`, `expect_column_value_lengths_to_be_between` |
| **Elementary** | `elementary` | `volume_anomalies`, `freshness_anomalies`, `column_anomalies` |

## When Adding Tests to a Model

1. **Primary key:** always add `unique` + `not_null`
2. **Foreign keys:** add `relationships` to the referenced model
3. **Enumerated columns:** add `accepted_values`
4. **Financial columns on critical models:** add Elementary `column_anomalies` (sum, zero_count)
5. **High-volume models:** add Elementary `volume_anomalies` with appropriate time bucket

## Schema YAML Organization

- One YAML file per directory or logical source grouping
- Source definitions: `_src_{source_name}.yml` in the staging directory for that source
- Model definitions: `_stg_{source_name}.yml` or `schema.yml` colocated in the model directory
