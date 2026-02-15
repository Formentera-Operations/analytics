# Source Systems

## Source Catalog

| Source | System | Domain | Key Tables | Ingestion |
|--------|--------|--------|------------|-----------|
| `oda` | Quorum OnDemand Accounting | Finance (GL, AP, AR, AFEs, JIB, revenue/expense decks) | `GL`, `ODA_BATCH_ODA_*` | Estuary CDC |
| `prodview` | Peloton ProdView | Production volumes, allocations, completions | `FORMENTERAOPS_PV30_DBO.*` | Fivetran |
| `wellview` | Peloton WellView | Well master data, drilling, surveys, costs | Multiple schemas | Fivetran |
| `procount` | IFS Procount | Barnett Shale (Griffin acquisition) production | `FP_GRIFFIN.PUBLIC.*` | Fivetran |
| `combo_curve` | Combo Curve | Economics forecasting (EUR, NPV, type curves) | Economic runs, wells, projects | Portable |
| `enverus` | Enverus | Third-party well/production data | `PUBLIC.*` | Portable |
| `aegis` | Aegis | Market pricing data (commodities) | 6 tables | — |
| `hubspot` | HubSpot | CRM contacts | 1 table | Fivetran |

## CDC and Ingestion Patterns

### Estuary CDC (ODA)

- Soft deletes indicated by `"_meta/op" = 'd'` — filter in staging `filtered` CTE
- `FLOW_PUBLISHED_AT` column tracks CDC timestamps
- No deduplication needed in source CTE (CDC guarantees one row per change)

### Fivetran (ProdView, WellView, Procount, HubSpot)

- Soft deletes indicated by `_fivetran_deleted = true`
- `_fivetran_synced` tracks sync timestamps
- Deduplicate in source CTE: `qualify 1 = row_number() over (partition by {pk} order by _fivetran_synced desc)`

### Portable (Combo Curve, Enverus)

- Soft deletes indicated by `deleteddate is not null` — filter in source CTE
- `_portable_extracted` tracks extraction timestamps
- No deduplication needed

## Deep Context Files

For detailed source system documentation (data model hierarchy, join patterns, unit conversions, user-defined field mappings), see:

| Source | Context File |
|--------|-------------|
| ProdView | `context/sources/prodview/prodview.md` + 13 domain YAML files |
| WellView | `context/sources/wellview/wellview.md` + 16 domain YAML files |

**Always read the relevant context file before building or modifying models in that domain.**
