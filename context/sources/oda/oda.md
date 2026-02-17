# ODA (Quorum OnDemand Accounting)

## System Overview

Quorum OnDemand Accounting (ODA) is Formentera's **accounting and financial management system**. It manages the general ledger, accounts receivable, accounts payable, joint interest billing, revenue/expense decks, AFE budgeting, and master data (companies, entities, vendors, owners, wells, accounts). It is the **system of record for financial transactions, owner revenue distribution, and operational cost tracking**.

- **Vendor:** Quorum Business Solutions (SaaS)
- **Snowflake database:** `ESTUARY_DB`
- **Snowflake schema:** `ESTUARY_SCHEMA`
- **Ingestion:** Estuary (two connectors — CDC and Batch)
- **CDC soft delete pattern:** `_operation_type != 'd'` (CDC tables only, 7 tables)
- **Batch soft delete pattern:** Not needed (batch tables only contain `'c'` operations)
- **Deduplication:** Not required (Estuary handles deduplication at the connector level)
- **Ingestion timestamp:** `_meta/op_ms` or `current_timestamp()` (mapped to `_loaded_at` in staging)
- **Table naming:** Mixed — bare names (`GL`), `ODA_` prefix (CDC), `ODA_BATCH_ODA_` prefix (batch). See Naming Patterns below.

## Estuary Architecture: Three Systems, One Schema

The Estuary materialization puts **three distinct source systems** into the same `ESTUARY_DB.ESTUARY_SCHEMA`:

| System | Estuary Capture | Snowflake Tables | In Scope? |
|--------|----------------|-----------------|-----------|
| **ODA** (Quorum OnDemand) | `FormenteraOps/ODA` + `FormenteraOps/ODA_BATCH` | ~35 | **Yes** |
| **OpenInvoice** (SAP) | `FormenteraOps/openinvoice_data_batch` | ~63 | No |
| **AFE Data** | `FormenteraOps/afe_data` | ~11 | No |

Only ODA tables are in scope for this project. OpenInvoice and AFE Data tables share the schema but are separate source systems with their own future refactors.

## Core Hierarchy

ODA organizes data around **financial transactions** flowing through the general ledger, with master data entities providing the dimensional context.

```
Companies [ODA_BATCH_ODA_COMPANY_V2]           <- top-level organizational unit (39)
  |
  +-- Entities [ODA_BATCH_ODA_ENTITY_V2]       <- properties/wells/cost centers (56K)
  |     |
  |     +-- Wells [ODA_BATCH_ODA_WELL]         <- well-level master data (9.4K)
  |
  +-- Accounts [ODA_BATCH_ODA_ACCOUNT_V2]      <- chart of accounts (2K)
  |     |
  |     +-- Account Types [ODA_ACCOUNTTYPE]    <- type classification (2)
  |     +-- Account Sub Types [ODA_BATCH_ODA_ACCOUNTSUBTYPE] <- sub-classification (5)
  |
  +-- General Ledger [GL]                      <- all financial transactions (180M, CDC)
  |     |
  |     +-- Vouchers [ODA_VOUCHER_V2]          <- journal entry groupings (370K)
  |     +-- GL Reconciliation Types [ODA_GLRECONCILIATIONTYPE] <- recon categories (8)
  |
  +-- Accounts Payable
  |     +-- AP Invoices [ODA_APINVOICE]        <- vendor invoices (334K, CDC)
  |     |     +-- AP Invoice Details [ODA_APINVOICEDETAIL] <- line items (6.8M, CDC)
  |     +-- AP Checks [ODA_APCHECK]            <- payment records (50K, old connector)
  |
  +-- Accounts Receivable
  |     +-- AR Invoices [ODA_ARINVOICE_V2]     <- owner invoices (436K, CDC)
  |     |     +-- AR Invoice Details [ODA_ARINVOICEDETAIL] <- line items (611, CDC)
  |     +-- AR Payments [ODA_ARINVOICEPAYMENT] <- payment records (38K)
  |     |     +-- AR Payment Details [ODA_ARINVOICEPAYMENTDETAIL] <- (172K)
  |     +-- AR Netted Details [ODA_ARINVOICENETTEDDETAIL] <- netting records (531K)
  |     +-- AR Adjustments [ODA_BATCH_ODA_ARINVOICEADJUSTMENT] <- (11K)
  |     |     +-- AR Adjustment Details [ODA_BATCH_ODA_ARINVOICEADJUSTMENTDETAIL] <- (41K)
  |     +-- AR Advances [ODA_BATCH_ODA_ARADVANCE] <- advance payments (27)
  |           +-- AR Advance Closeouts [ODA_BATCH_ODA_ARADVANCECLOSEOUT] <- (0)
  |
  +-- Joint Interest Billing
  |     +-- JIB [ODA_JIB]                      <- joint interest billings (63M, CDC)
  |     +-- JIB Details [ODA_JIBDETAIL]        <- parallel denormalized view (63M, CDC, no FK to JIB)
  |
  +-- Revenue & Expense Decks
  |     +-- Revenue Deck Sets [ODA_REVENUEDECKSET] <- per well/product/company (19K)
  |     |     +-- Revenue Decks [ODA_BATCH_ODA_REVENUEDECK_V2] <- effective-date instances (20K)
  |     |           +-- Revenue Deck Revisions [ODA_REVENUEDECKREVISION] <- version history (346K)
  |     |                 +-- Revenue Deck Participants [ODA_REVENUEDECKPARTICIPANT] <- owner interests (196M)
  |     +-- Expense Deck Sets [ODA_EXPENSEDECKSET] <- per well/product/company (8K)
  |     |     +-- Expense Decks [ODA_BATCH_ODA_EXPENSEDECK_V2] <- effective-date instances (10K)
  |     |           +-- Expense Deck Revisions [ODA_BATCH_ODA_EXPENSEDECKREVISION] <- version history (24K)
  |     |                 +-- Expense Deck Participants [ODA_BATCH_ODA_EXPENSEDECKPARTICIPANT] <- owner interests (877K)
  |
  +-- AFE/Budgeting
  |     +-- AFEs [ODA_BATCH_ODA_AFE_V2]        <- authorization for expenditure (2.2K)
  |     +-- AFE Budgets [ODA_AFEBUDGET]        <- budget headers (1.9K)
  |     |     +-- AFE Budget Details [ODA_AFEBUDGETDETAIL_V2] <- budget line items (424K)
  |     +-- Revenue Suspense Categories [ODA_BATCH_ODA_REVENUESUSPENSECATEGORY] <- (24)
  |
  +-- Master Data (Reference)
        +-- Owners [ODA_BATCH_ODA_OWNER_V2]    <- mineral/royalty owners (42K)
        +-- Vendors [ODA_BATCH_ODA_VENDOR_V2]  <- AP vendors (4K)
        +-- Purchasers [ODA_BATCH_ODA_PURCHASER_V2] <- revenue purchasers (183)
        +-- Products [ODA_BATCH_ODA_PRODUCT]    <- oil/gas/NGL products (9)
        +-- Interest Types [ODA_BATCH_ODA_INTERESTTYPE] <- WI/NRI/RI/ORRI (5)
        +-- Payment Types [ODA_BATCH_ODA_PAYMENTTYPE] <- check/ACH/wire (8)
        +-- Calendar [MDM_CALENDAR]             <- accounting calendar (37K)
        +-- User Fields [ODA_USERFIELD]         <- custom field values (4.8M)
        +-- Revision States [ODA_REVISIONSTATE] <- deck revision states (3)
        +-- Source Modules [ODA_BATCH_ODA_SOURCEMODULE] <- GL source module types (26)
```

### Key Relationships

| Parent | Child | Join Key | Relationship |
|--------|-------|----------|-------------|
| Company | Entity | `CompanyId` | 1:many |
| Entity | Well | `EntityId` | 1:many |
| Company | Account | `CompanyId` | 1:many |
| AP Invoice | AP Invoice Detail | `APInvoiceId` | 1:many |
| AR Invoice | AR Invoice Detail | `InvoiceId` | 1:many |
| AR Invoice | AR Payment Detail | `InvoiceId` | 1:many (payment links through detail) |
| AR Payment | AR Payment Detail | `InvoicePaymentId` | 1:many |
| AR Invoice | AR Netted Detail | `InvoiceId` | 1:many |
| AR Invoice | AR Adjustment Detail | `InvoiceId` | 1:many (adjustment links through detail) |
| AR Adjustment | AR Adjustment Detail | `ARInvoiceAdjustmentId` | 1:many |
| JIB | JIB Detail | _(no FK — parallel denormalized views)_ | — |
| Revenue Deck Set | Revenue Deck | `DeckSetId` | 1:many |
| Revenue Deck | Revenue Deck Revision | `DeckId` | 1:many |
| Revenue Deck Revision | Revenue Deck Participant | `RevenueDeckRevisionId` | 1:many |
| Expense Deck Set | Expense Deck | `DeckSetId` | 1:many |
| Expense Deck | Expense Deck Revision | `DeckId` | 1:many |
| Expense Deck Revision | Expense Deck Participant | `ExpenseDeckRevisionId` | 1:many |
| AFE | AFE Budget | `AFEId` | 1:many |
| AFE Budget | AFE Budget Detail | `AFEBudgetId` | 1:many |
| GL Entry | Voucher | `VoucherId` | many:1 |
| GL Entry | Entity | `EntityId` | many:1 |
| GL Entry | Account | `AccountId` | many:1 |

### ID Patterns

- **ODA IDs**: Integer primary keys (e.g., `Id`, `APInvoiceId`, `EntityId`, `CompanyId`)
- **Cross-system key**: `EntityId` links ODA entities to wells across WellView and ProdView
- **Accounting periods**: `AccountingDate`, `PostDate`, `EffectiveDate` define temporal context
- **Calendar key**: `MDM_CALENDAR` provides the accounting calendar with fiscal periods

## Ingestion Pattern (Estuary)

### Two Connectors, Three Path Patterns

ODA data flows through **two Estuary connectors** that produce three distinct Snowflake table naming patterns:

| Connector | Estuary Path | Snowflake Table Naming | Tables | Delete Filtering |
|-----------|-------------|----------------------|--------|-----------------|
| **CDC** | `FormenteraOps/ODA/oda/*` | `GL` or `ODA_*` | **7** | **Required** (`_operation_type != 'd'`) |
| **Batch (clean)** | `FormenteraOps/ODA_BATCH/oda/*` | `ODA_*` | ~16 | Not needed |
| **Batch (prefixed)** | `FormenteraOps/ODA_BATCH/oda_*` | `ODA_BATCH_ODA_*` | ~12 | Not needed |

The naming inconsistency comes from Estuary's `targetNaming: "prefixNonDefaultSchema"` setting. Tables whose Estuary path uses `oda/*` (default schema) get clean `ODA_*` names. Tables whose path uses `oda_*` (non-default) get the `ODA_BATCH_ODA_*` prefix.

### Comparison with Other Sources

| Aspect | ODA (Estuary CDC) | ODA (Estuary Batch) | WellView (Fivetran) | ComboCurve (Portable) |
|--------|-------------------|--------------------|--------------------|----------------------|
| Connector | Estuary CDC | Estuary Batch | Fivetran CDC | Portable Batch |
| Soft delete | `_operation_type != 'd'` | Not needed | `_fivetran_deleted = true` | `deleteddate is not null` |
| Deduplication | Not needed | Not needed | `qualify row_number()` on `_fivetran_synced` | Not needed |
| Ingestion timestamp | `_meta/op_ms` | `current_timestamp()` | `_FIVETRAN_SYNCED` | `_PORTABLE_EXTRACTED` |
| Column naming | camelCase (as-is from API) | camelCase (as-is from API) | UPPER_SNAKE_CASE | UPPER_SNAKE_CASE |

### Estuary-Specific Gotchas

1. **`_meta/op` exists on ALL tables**: Both CDC and batch tables have this column. Batch tables only contain `'c'` (create) — never `'d'` (delete). Use the connector configuration to determine CDC vs batch, not column presence.
2. **Three connector eras in one schema**: Old ODA connector (tables stale since Jan 11, 2026), current CDC connector (7 tables), and current batch connector (~28 tables). Some tables exist from multiple eras with different row counts.
3. **No `_loaded_at` equivalent**: Estuary does not provide a consistent ingestion timestamp like Fivetran's `_fivetran_synced` or Portable's `_portable_extracted`. CDC tables have `_meta/op_ms` (operation timestamp). Batch tables use `current_timestamp()` as fallback.
4. **Column names are camelCase**: Unlike Fivetran/Portable which convert to UPPER_SNAKE_CASE, Estuary preserves the source API's camelCase naming (e.g., `AccountingDate`, `CompanyId`, `EntityId`). Staging models must handle the case conversion.
5. **Shared schema with non-ODA systems**: `ESTUARY_SCHEMA` contains tables from OpenInvoice (63 tables) and AFE Data (11 tables) alongside ODA. Filter by table name prefix when exploring the schema.

## CDC Tables (Exactly 7)

These are the only tables that require soft-delete filtering (`_operation_type != 'd'`):

| Snowflake Table | Rows | Staging Model | Last Altered | Domain |
|----------------|------|---------------|--------------|--------|
| GL | 180,288,799 | stg_oda__gl | 2026-02-16 | General Ledger |
| ODA_APINVOICE | 334,335 | stg_oda__apinvoice | 2026-02-17 | Accounts Payable |
| ODA_APINVOICEDETAIL | 6,813,538 | stg_oda__apinvoicedetail | 2026-02-17 | Accounts Payable |
| ODA_JIB | 63,210,996 | stg_oda__jib | 2026-02-13 | Supporting |
| ODA_JIBDETAIL | 63,213,576 | stg_oda__jibdetail | 2026-02-13 | Supporting |
| ODA_ARINVOICE_V2 | 436,113 | stg_oda__arinvoice_v2 | 2026-02-12 | Accounts Receivable |
| ODA_ARINVOICEDETAIL | 611 | stg_oda__arinvoicedetail | 2026-02-10 | Accounts Receivable |

## Source Tables Summary

### Accounts Payable (3 models)

| Staging Model | Source Table | Rows | Connector | Status |
|--------------|-------------|------|-----------|--------|
| stg_oda__apinvoice | ODA_APINVOICE | 334,335 | CDC | Current |
| stg_oda__apinvoicedetail | ODA_APINVOICEDETAIL | 6,813,538 | CDC | Current |
| stg_oda__apcheck | ODA_APCHECK | 49,531 | Old connector | Stale (last altered 2026-01-11) |

### Accounts Receivable (9 models)

| Staging Model | Source Table | Rows | Connector | Status |
|--------------|-------------|------|-----------|--------|
| stg_oda__arinvoice_v2 | ODA_ARINVOICE_V2 | 436,113 | CDC | Current |
| stg_oda__arinvoicedetail | ODA_ARINVOICEDETAIL | 611 | CDC | Current |
| stg_oda__arinvoicepayment | ODA_ARINVOICEPAYMENT | 37,645 | Batch | Current |
| stg_oda__arinvoicepaymentdetail | ODA_ARINVOICEPAYMENTDETAIL | 172,313 | Batch | Current |
| stg_oda__arinvoicenetteddetail | ODA_ARINVOICENETTEDDETAIL | 530,531 | Batch | Current |
| stg_oda__arinvoiceadjustment | ODA_BATCH_ODA_ARINVOICEADJUSTMENT | 10,716 | Batch | Current |
| stg_oda__arinvoiceadjustmentdetail | ODA_BATCH_ODA_ARINVOICEADJUSTMENTDETAIL | 41,073 | Batch | Current |
| stg_oda__aradvance | ODA_BATCH_ODA_ARADVANCE | 27 | Batch | Current |
| stg_oda__aradvancecloseout | ODA_BATCH_ODA_ARADVANCECLOSEOUT | 0 | Batch | Current (empty) |

### General Ledger (5 models)

| Staging Model | Source Table | Rows | Connector | Status |
|--------------|-------------|------|-----------|--------|
| stg_oda__gl | GL | 180,288,799 | CDC | Current |
| stg_oda__voucher_v2 | ODA_VOUCHER_V2 | 369,823 | Batch | Current |
| stg_oda__gl_reconciliation_type | ODA_GLRECONCILIATIONTYPE | 8 | Batch | Current |
| stg_oda__account_types | ODA_ACCOUNTTYPE | 2 | Batch | Current |
| stg_oda__account_sub_types | ODA_BATCH_ODA_ACCOUNTSUBTYPE | 5 | Batch | Current |

### Supporting (7 models)

| Staging Model | Source Table | Rows | Connector | Status |
|--------------|-------------|------|-----------|--------|
| stg_oda__jib | ODA_JIB | 63,210,996 | CDC | Current |
| stg_oda__jibdetail | ODA_JIBDETAIL | 63,213,576 | CDC | Current |
| stg_oda__checkrevenue | ODA_CHECKREVENUE | 658,795 | Old connector | Stale (last altered 2026-01-11) |
| stg_oda__calendar | MDM_CALENDAR | 36,890 | Batch | Current |
| stg_oda__userfield | ODA_USERFIELD | 4,777,582 | Batch | Current |
| stg_oda__revision_state | ODA_REVISIONSTATE | 3 | Batch | Current |
| stg_oda__source_module | ODA_BATCH_ODA_SOURCEMODULE | 26 | Batch | Current |

### Revenue & Expense Decks (8 models)

| Staging Model | Source Table | Rows | Connector | Status |
|--------------|-------------|------|-----------|--------|
| stg_oda__revenue_deck_v2 | ODA_BATCH_ODA_REVENUEDECK_V2 | 20,470 | Batch | Current |
| stg_oda__revenue_deck_set | ODA_REVENUEDECKSET | 18,953 | Batch | Current |
| stg_oda__revenue_deck_revision | ODA_REVENUEDECKREVISION | 345,985 | Batch | Current |
| stg_oda__revenue_deck_participant | ODA_REVENUEDECKPARTICIPANT | 196,069,668 | Batch | Current |
| stg_oda__expense_deck_v2 | ODA_BATCH_ODA_EXPENSEDECK_V2 | 9,744 | Batch | Current |
| stg_oda__expense_deck_set | ODA_EXPENSEDECKSET | 7,598 | Batch | Current |
| stg_oda__expense_deck_revision | ODA_BATCH_ODA_EXPENSEDECKREVISIONVIEW | 22,245 | Batch | Re-point to ODA_BATCH_ODA_EXPENSEDECKREVISION (24,358) |
| stg_oda__expense_deck_participant | ODA_BATCH_ODA_EXPENSEDECKPARTICIPANT | 876,866 | Batch | Current |

### AFE/Budgeting (4 models)

| Staging Model | Source Table | Rows | Connector | Status |
|--------------|-------------|------|-----------|--------|
| stg_oda__afebudget | ODA_AFEBUDGET | 1,940 | Batch | Current |
| stg_oda__afebudgetdetail_v2 | ODA_AFEBUDGETDETAIL_V2 | 424,116 | Batch | Current |
| stg_oda__afe_v2 | ODA_BATCH_ODA_AFE_V2 | 2,208 | Batch | Current |
| stg_oda__revenue_suspense_category | ODA_BATCH_ODA_REVENUESUSPENSECATEGORY | 24 | Batch | Current |

### Master Data (10 models)

| Staging Model | Source Table | Rows | Connector | Status |
|--------------|-------------|------|-----------|--------|
| stg_oda__company_v2 | ODA_BATCH_ODA_COMPANY_V2 | 39 | Batch | Current |
| stg_oda__entity_v2 | ODA_BATCH_ODA_ENTITY_V2 | 56,024 | Batch | Current |
| stg_oda__vendor_v2 | ODA_BATCH_ODA_VENDOR_V2 | 4,006 | Batch | Current |
| stg_oda__owner_v2 | ODA_BATCH_ODA_OWNER_V2 | 42,040 | Batch | Current |
| stg_oda__purchaser_v2 | ODA_BATCH_ODA_PURCHASER_V2 | 183 | Batch | Current |
| stg_oda__wells | ODA_BATCH_ODA_WELL | 9,421 | Batch | Current |
| stg_oda__account_v2 | ODA_BATCH_ODA_ACCOUNT_V2 | 1,998 | Batch | Current |
| stg_oda__interest_type | ODA_BATCH_ODA_INTERESTTYPE | 5 | Batch | Current |
| stg_oda__product | ODA_BATCH_ODA_PRODUCT | 9 | Batch | Current |
| stg_oda__payment_type | ODA_BATCH_ODA_PAYMENTTYPE | 8 | Batch | Current |

## Tables Without Staging Models

### From Old/Duplicate Connectors

| Table | Rows | Domain | Notes |
|-------|------|--------|-------|
| ODA_APCHECKDETAIL | 313,008 | AP | Check line items (old connector, stale) |
| ODA_CHECKREVENUEDETAIL | 44,751,978 | Supporting | Revenue check details (old connector, stale) |
| ODA_OWNERREVENUEDETAIL | 61,147,868 | Supporting | Owner revenue details (old connector, stale) |
| ODA_GL | 135,034,032 | GL | Duplicate GL from different connector path |
| ODA_ENTITY_V2 | 45,063 | Master Data | From old connector (vs BATCH 56K rows) |
| ODA_OWNER_V2 | 42,255 | Master Data | From old connector |
| ODA_PURCHASER_V2 | 179 | Master Data | From old connector |
| ODA_VENDOR_V2 | 4,509 | Master Data | Newer than BATCH version — candidate for re-pointing |
| ODA_EXPENSEDECKREVISION | 22,464 | Decks | From old connector |
| ODA_EXPENSEDECKSET | 7,598 | Decks | From old connector |
| ODA_EXPENSEDECK_V2 | 8,052 | Decks | From old connector |

### New/Uncovered Tables

| Table | Rows | Domain | Notes |
|-------|------|--------|-------|
| ODA_ARINVOICENETTEDDETAILLINEITEM | 861,127 | AR | Netted detail line items — evaluate for staging |

### Lookup/Reference Tables Without Staging Models

| Table | Rows | Domain | Notes |
|-------|------|--------|-------|
| ODA_BATCH_ODA_AFEBUDGETDETAILTYPE | 16 | AFE | Budget detail type classifications |
| ODA_BATCH_ODA_APPAYMENTTYPE | 12 | AP | AP payment type classifications |
| ODA_BATCH_ODA_ARADJUSTMENTTYPE | 3 | AR | AR adjustment type classifications |
| ODA_BATCH_ODA_ARINVOICETYPE | 8 | AR | AR invoice type classifications |
| ODA_BATCH_ODA_COSTTYPE | 3 | Supporting | Cost type classifications |
| ODA_BATCH_ODA_CURRENCYTYPE | 1 | Master Data | Currency type (single value) |
| ODA_BATCH_ODA_CURRENCY_V2 | 158 | Master Data | Currency definitions |
| ODA_BATCH_ODA_CUSTOMERTYPE | 11 | Master Data | Customer type classifications |
| ODA_BATCH_ODA_ENTITYTYPE | 14 | Master Data | Entity type classifications |
| ODA_BATCH_ODA_LOCATIONTYPE | 6 | Master Data | Location type classifications |
| ODA_BATCH_ODA_OWNERTYPE | 4 | Master Data | Owner type classifications |
| ODA_BATCH_ODA_PARTNERTYPE | 2 | Master Data | Partner type classifications |
| ODA_BATCH_ODA_PRODUCTTYPE | 6 | Master Data | Product type classifications |
| ODA_BATCH_ODA_USAGETYPE | 5 | Master Data | Usage type classifications |
| ODA_BATCH_ODA_VOUCHERTYPE | 24 | GL | Voucher type classifications |
| ODA_BATCH_ODA_VOUCHER_V2 | 351,056 | GL | Voucher V2 from batch (vs ODA_VOUCHER_V2 370K from clean batch) |
| ODA_BATCH_ODA_WELLCOMPLETIONTYPE | 3 | Master Data | Well completion type classifications |
| ODA_BATCH_ODA_WELLSTATUS | 10 | Master Data | Well status classifications |
| ODA_BATCH_MDM_CALENDAR | 36,890 | Supporting | Duplicate of MDM_CALENDAR |

### Superseded V1 Tables (Empty or Replaced)

| Table | Rows | Replaced By | Notes |
|-------|------|------------|-------|
| ODA_ARINVOICE | 0 | ODA_ARINVOICE_V2 (436K) | V1 empty |
| ODA_BATCH_ODA_ACCOUNT | 0 | ODA_BATCH_ODA_ACCOUNT_V2 (2K) | V1 empty |
| ODA_BATCH_ODA_COMPANY | 18 | ODA_BATCH_ODA_COMPANY_V2 (39) | V1 superseded |
| ODA_BATCH_ODA_ENTITY | 57,782 | ODA_BATCH_ODA_ENTITY_V2 (56K) | V1 superseded |
| ODA_BATCH_ODA_OWNER | 0 | ODA_BATCH_ODA_OWNER_V2 (42K) | V1 empty |
| ODA_BATCH_ODA__OWNER | 42,255 | ODA_BATCH_ODA_OWNER_V2 (42K) | Typo variant |
| ODA_BATCH_ODA_VENDOR | — | ODA_BATCH_ODA_VENDOR_V2 (4K) | V1 superseded |
| ODA_BATCH_ODA_PURCHASER | — | ODA_BATCH_ODA_PURCHASER_V2 (183) | V1 superseded |

## Domain Organization (7 Domains)

| Domain | Directory | Models | CDC Tables | Key Tables |
|--------|-----------|--------|-----------|------------|
| Accounts Payable | `accounts_payable/` | 3 | 2/3 | AP Invoice, AP Invoice Detail |
| Accounts Receivable | `accounts_receivable/` | 9 | 2/9 | AR Invoice V2, AR Invoice Detail |
| General Ledger | `general_ledger/` | 5 | 1/5 | GL (180M rows) |
| Revenue & Expense Decks | `decks/` | 8 | 0 | Revenue Deck Participant (196M rows) |
| AFE/Budgeting | `afe_budgeting/` | 4 | 0 | AFE V2, AFE Budget Detail V2 |
| Master Data | `master_data/` | 10 | 0 | Entity V2, Owner V2, Vendor V2 |
| Supporting | `supporting/` | 7 | 2/7 | JIB (63M), JIB Detail (63M) |

## Downstream Consumers

~30 downstream models consume ODA staging:

| Consumer Layer | Models | Key Dependencies |
|---------------|--------|-----------------|
| Finance intermediates | int_oda_gl, int_oda_ar_*, int_gl_enhanced, int_accounts_classified | GL, AR Invoice, Entity, Account, Company |
| Finance marts | general_ledger, dim_accounts, dim_companies, dim_vendors, fct_ar_aging | All master data + GL + AR |
| Well 360 | int_well__oda, int_well__spine | Entity V2, Wells |
| Applications | wiserock_app__companies | Company V2 |

## Key Gotchas

1. **`_meta/op` column exists on BOTH CDC and batch tables**: Batch tables only have `'c'` operations, never `'d'`. Use the connector configuration (see "Two Connectors, Three Path Patterns") to classify a table as CDC or batch — not column presence.

2. **Three connector eras in one schema**: Old ODA connector (tables stale since Jan 11, 2026), current CDC connector (7 tables, actively updating), current batch connector (~28 tables, actively updating). Some tables exist from multiple eras with different row counts and freshness.

3. **V1/V2 duplicates**: Use V2 where both exist and V2 has data. V1 tables (`ODA_ARINVOICE`, `ODA_BATCH_ODA_ACCOUNT`, `ODA_BATCH_ODA_VENDOR`, `ODA_BATCH_ODA_OWNER`, `ODA_BATCH_ODA_PURCHASER`) are empty or superseded.

4. **Naming inconsistency across three patterns**: Same schema has tables named `GL` (bare), `ODA_APINVOICE` (CDC prefix), and `ODA_BATCH_ODA_COMPANY_V2` (batch prefix). This is an artifact of Estuary's `prefixNonDefaultSchema` setting, not a meaningful classification.

5. **ODA_GL vs GL**: Two GL tables exist. `GL` (180M rows, actively updated by CDC connector) is the correct one. `ODA_GL` (135M rows, last altered 2026-01-29) is from a different connector path and should not be used.

6. **Stale tables from old connector**: `ODA_APCHECK` (49K rows) and `ODA_CHECKREVENUE` (659K rows) are from the old ODA connector, last altered 2026-01-11. They still have data but are no longer being updated. Evaluate whether to add these tables to the current connector or deprecate the staging models.

7. **Expense deck revision VIEW vs non-VIEW**: `stg_oda__expense_deck_revision` currently references `ODA_BATCH_ODA_EXPENSEDECKREVISIONVIEW` (22,245 rows). The non-VIEW table `ODA_BATCH_ODA_EXPENSEDECKREVISION` (24,358 rows) is more current and should be used instead.

8. **Vendor V2 source options**: `stg_oda__vendor_v2` references `ODA_BATCH_ODA_VENDOR_V2` (4,006 rows). A newer `ODA_VENDOR_V2` (4,509 rows) also exists from the old connector. Consider re-pointing to the newer table if its freshness is confirmed.

9. **Revenue deck participant is massive**: `ODA_REVENUEDECKPARTICIPANT` has 196M rows (batch table). Currently materialized as a view. Evaluate materialization during the decks sprint — may need `table` materialization if downstream queries are slow.

10. **MDM_CALENDAR is cross-system**: The calendar table comes from `FormenteraOps/ODA_BATCH/mdm/calendar` — a shared MDM (Master Data Management) table, not strictly ODA-specific. A duplicate `ODA_BATCH_MDM_CALENDAR` also exists.

## Schema Reference

Detailed column-level schemas will be in `context/sources/oda/tables/`.
Domain relationship files will be in `context/sources/oda/domains/`.
Load `_index.yaml` to find the right table or domain file for your task.
