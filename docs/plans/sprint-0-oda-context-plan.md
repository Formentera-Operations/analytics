# ODA Staging Refactor — Sprint 0: Context Documentation + Source Audit

## Implementation Plan

### Phase 1: Source Audit (FOR-259 + FOR-260)

**Goal:** Build the definitive mapping of every staging model to its Snowflake source table.

#### 1.1 Query Snowflake for ODA tables
- Run `dbt show --inline` query against `ESTUARY_DB.INFORMATION_SCHEMA.TABLES` for all tables matching `%ODA%`, `GL`, and `MDM_CALENDAR`
- Capture: TABLE_NAME, ROW_COUNT, LAST_ALTERED, CREATED
- This gives us ground truth of what actually exists in Snowflake

#### 1.2 Map staging models to source tables
- For each of the 46 staging models in `models/operations/staging/oda/`, extract the `{{ source('oda', 'TABLE_NAME') }}` reference
- Cross-reference with the Snowflake tables query to determine:
  - Does the referenced table exist? → Valid reference
  - Does it NOT exist? → Orphaned reference (needs fix)
  - Are there new tables with NO staging model? → Flag for future work

#### 1.3 Classify CDC vs Batch
- Use the brainstorm's definitive CDC list (7 tables): GL, ODA_APINVOICEDETAIL, ODA_JIB, ODA_JIBDETAIL, ODA_ARINVOICE_V2, ODA_APINVOICE, ODA_ARINVOICEDETAIL
- All remaining ODA tables are batch
- Do NOT use `_meta/op` column presence to classify — it exists on both CDC and batch tables

#### 1.4 Resolve orphaned references (~11 models)
- For each model listed in brainstorm §3, determine:
  - Does the OLD table still exist in Snowflake? With how many rows?
  - Does a NEW table exist? What's the correct current name?
  - Action: re-point / deprecate / flag for Estuary config update
- Document each resolution in the system overview

#### 1.5 Document V1/V2 decisions
- For tables with both V1 and V2 variants, confirm row counts and data freshness
- Decision rule: Use V2 where both exist and V2 has data

**Output:** Definitive mapping table embedded in `context/sources/oda/oda.md`

---

### Phase 2: System Overview (FOR-261)

**Goal:** Create `context/sources/oda/oda.md` following the ComboCurve/WellView template structure.

#### Sections:
1. **System Overview** — What ODA is, what it's the source of truth for
2. **Core Hierarchy** — Entity relationships (Company → Wells → AFEs → GL entries, etc.)
3. **Ingestion Pattern (Estuary)** — 3 systems in one schema, 2 ODA connectors, CDC vs batch
4. **Source Tables Summary** — Definitive mapping table from Phase 1
5. **Tables Without Staging Models** — New/unstaged tables
6. **Key Gotchas** — Naming inconsistencies, CDC-on-batch confusion, orphaned refs, V1/V2

**Template:** Follow structure of `context/sources/combo_curve/combo_curve.md`

---

### Phase 3: Per-Table YAMLs (FOR-262)

**Goal:** Generate YAML column definition files for every confirmed-current ODA source table.

#### 3.1 Query column metadata
- Run `dbt show --inline` against `ESTUARY_DB.INFORMATION_SCHEMA.COLUMNS` for each in-scope table
- Capture: COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH

#### 3.2 Generate YAML files
- Create files in `context/sources/oda/tables/` following the pattern in `context/sources/combo_curve/tables/projects.yaml`
- Format: `TABLE_NAME: Description\n  COLUMN(type) #description`
- Infer descriptions from: column names, existing staging SQL, domain knowledge
- Group columns by category (identifiers, dates, measures, flags, audit, metadata)

#### 3.3 Only create YAMLs for current tables
- Skip legacy/orphaned tables (e.g., V1 tables that have been superseded by V2)
- Skip non-ODA tables in the schema (OpenInvoice, AFE Data)

**Output:** ~35 YAML files in `context/sources/oda/tables/`

---

### Phase 4: Domain Files + Index (FOR-263)

**Goal:** Create the domain relationship files and table index.

#### 4.1 Create `_index.yaml`
- Table catalog with domain groupings, connector type, row counts, V1/V2 status
- Follow format of `context/sources/combo_curve/_index.yaml`
- Include both staged and unstaged tables

#### 4.2 Create 7 domain files in `context/sources/oda/domains/`
Each follows the pattern in `context/sources/combo_curve/domains/economics.yaml`:

| Domain | File | Tables |
|--------|------|--------|
| Accounts Payable | `accounts_payable.yaml` | ODA_APINVOICE, ODA_APINVOICEDETAIL, ODA_APCHECK* |
| Accounts Receivable | `accounts_receivable.yaml` | ODA_ARINVOICE_V2, ODA_ARINVOICEDETAIL, ODA_ARINVOICEPAYMENT, etc. |
| General Ledger | `general_ledger.yaml` | GL, ODA_VOUCHER_V2, ODA_GLRECONCILIATIONTYPE, etc. |
| Revenue & Expense Decks | `decks.yaml` | ODA_BATCH_ODA_REVENUEDECK_V2, ODA_REVENUEDECKSET, etc. |
| AFE/Budgeting | `afe_budgeting.yaml` | ODA_AFEBUDGET, ODA_AFEBUDGETDETAIL_V2, ODA_BATCH_ODA_AFE_V2 |
| Master Data | `master_data.yaml` | ODA_BATCH_ODA_COMPANY_V2, ODA_BATCH_ODA_ENTITY_V2, etc. |
| Supporting | `supporting.yaml` | ODA_JIB, ODA_JIBDETAIL, MDM_CALENDAR, etc. |

**Output:** 7 domain files + 1 index file

---

### Execution Strategy

**Parallelization opportunities:**
- Phase 1 (audit) must complete first — it determines what's "current" vs "orphaned"
- Phase 2 (overview) depends on Phase 1 output
- Phase 3 (per-table YAMLs) and Phase 4 (domain files) can run in parallel after Phase 2

**Swarm agent decomposition:**
1. **Agent 1 (Audit):** Run Snowflake queries, build the definitive mapping table
2. **Agent 2 (Overview):** Write `oda.md` using audit results + brainstorm doc
3. **Agent 3 (Table YAMLs — batch 1):** Generate YAMLs for tables in AP, AR, GL domains
4. **Agent 4 (Table YAMLs — batch 2):** Generate YAMLs for tables in Decks, AFE, Master Data, Supporting domains
5. **Agent 5 (Domain + Index):** Create all 7 domain files + `_index.yaml`

---

### Definition of Done

- [ ] `context/sources/oda/oda.md` — system overview with definitive source mapping
- [ ] `context/sources/oda/_index.yaml` — full table catalog
- [ ] Per-table YAMLs for all confirmed-current source tables
- [ ] 7 domain YAML files
- [ ] All ~11 orphaned source reference questions resolved and documented
- [ ] Committed and pushed to `feature/oda-staging-refactor-sprint-0`
