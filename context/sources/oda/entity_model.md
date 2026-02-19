# ODA Entity Model

Reference for designing intermediate and mart models from ODA (Quorum OnDemand) data. Describes financial entities, their relationships, cross-system resolution, and how they feed into the enterprise analytical model.

## How to Use This Document

- Load when planning new intermediate or mart models that touch ODA
- Cross-reference with domain YAMLs (`context/sources/oda/domains/*.yaml`) for table relationships
- Cross-reference with per-table YAMLs (`context/sources/oda/tables/*.yaml`) for column-level detail
- Entity definitions here are **business-centric** — they describe what the finance team cares about, not how ODA stores it

---

## Three Financial Models

ODA organizes around three questions that map to distinct transaction flows:

| Model | Question It Answers | Transaction Flow | Volume |
|-------|---------------------|-----------------|--------|
| **The Ledger** | "Where did every dollar go?" | GL entries ← all subsystems | 180M GL rows |
| **Revenue Distribution** | "Who earned what from production?" | Production → Deck → Owner → Check | 196M deck participant rows |
| **Cost Management** | "What did we pay, and to whom?" | Vendor → Invoice → Check → GL | 6.8M AP detail rows |

These three models share a single root — the **Cost Center (≈ Well)** — but answer fundamentally different business questions. The GL is the ultimate reconciliation anchor: every AP check, every JIB billing, every revenue distribution eventually produces a GL entry.

---

## Model 1: The Ledger — "Where did every dollar go?"

The general ledger is ODA's central fact table. It captures every financial transaction posted to the books — operational expenses (LOE), capital expenditures (CAPEX), revenue credits, JIB billings, and manual journal entries. The GL is the single source of truth; all other financial facts are upstream events that eventually post here.

### Entity Hierarchy

```
Cost Center (≈ Well, linked via EID)
│
├── General Ledger Entry          ← every posted financial transaction (180M rows)
│     ├── Account                 ← chart of accounts classification
│     │     ├── Account Type      ← asset / liability / expense / revenue
│     │     └── Account Sub-Type  ← finer classification within type
│     ├── Voucher (V2)            ← journal batch header
│     │     └── Voucher Type      ← manual / AP / AR / JIB / revenue / recurring
│     ├── Source Module           ← originating subsystem (AP/AR/JIB/Revenue/Manual)
│     └── Entity (V2)             ← the legal entity posting the transaction
│
└── AFE (V2)                      ← authorization for expenditure (capital)
      ├── AFE Budget              ← budget header by period
      └── AFE Budget Detail (V2)  ← monthly allocation (12 rows per AFE budget)
```

### Entity Details

#### General Ledger Entry

The atomic financial event. One row per journal entry line, with full account coding, cross-references to the originating transaction (AP invoice, AR invoice, JIB, check, or null for manual entries), and three date dimensions (journal date, cash date, accrual date).

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Parent** | Cost Center / Well (`WellId`) |
| **Account coding** | `AccountId` → `stg_oda__account_v2` — full chart of accounts hierarchy |
| **Voucher** | `VoucherId` → `stg_oda__voucher_v2` — journal batch |
| **Cross-refs** | `ApInvoiceId`, `ArInvoiceId`, `ApCheckId`, `CheckRevenueId`, `AfeId`, `EntityId` |
| **Date dimensions** | `JournalDate` (books), `CashDate` (cash basis), `AccrualDate` (accrual basis) |
| **Amounts** | `GrossValue`, `NetValue`, `GrossVolume`, `NetVolume` |
| **Source tables** | `stg_oda__gl` (CDC, 180M rows) |
| **Existing marts** | `general_ledger` — incremental, clustered by `company_code + journal_date + account_key` |
| **Existing marts** | `fct_los` — incremental subset filtered to LOS-mapped accounts only, with LOS hierarchy from `dim_los_acct_map` |

**GL design note:** The GL is too large to compute MD5 surrogate keys at query time (180M rows). It uses `Id` directly as the mart PK (`gl_id`). Do NOT add surrogate keys to the GL staging model — the performance cost is prohibitive.

#### Account

The chart of accounts. Every GL entry, AP invoice detail line, and JIB detail line codes to an account.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Code structure** | `MainAccount` + `SubAccount` (e.g., "6100" + "001") |
| **Type hierarchy** | `AccountTypeId` → `AccountType`, `AccountSubTypeId` → `AccountSubType` |
| **LOS mapping** | `los_category`, `los_section`, `los_line_item_name` — maintained in `stg_sharepoint__los_account_map` |
| **Flags** | `NormallyDebit`, `SummarizeInGLReports`, `SummarizeInJIBInvoice`, `GenerateFixedAssetCandidates` |
| **Source tables** | `stg_oda__account_v2`, `stg_oda__account_types`, `stg_oda__account_sub_types` |
| **Existing marts** | `dim_accounts` — full chart of accounts with LOS mapping and reporting flags |

#### Voucher (V2)

Journal batch header. One voucher groups related GL entries together (e.g., all entries from one AP invoice posting, or one recurring journal entry).

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Code** | `Code` (int) — voucher number |
| **Type** | `VoucherTypeId` — manual / AP / AR / JIB / revenue / recurring |
| **Posting** | `Posted` (bool), `PostedByName`, `PostedDate` |
| **Source tables** | `stg_oda__voucher_v2` (Batch, 370K rows) |

#### AFE (V2)

Authorization for Expenditure — the accounting entity that tracks capital spending approval and actuals. ODA AFEs are the *accounting authorization*; WellView Job AFEs are the *operational authorization*. They refer to the same real-world AFE but live in different systems.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Business key** | `Code` (nvarchar) — the AFE number string-matched across systems |
| **Well link** | `WellId` → `stg_oda__wells` |
| **Field/Operating Group** | Denormalized on the AFE record for reporting |
| **AFE Type** | `AfeTypeId` — budget usage classification |
| **Child tables** | `stg_oda__afebudget` (1.9K rows), `stg_oda__afebudgetdetail_v2` (424K rows — 12 monthly rows per budget) |
| **Source tables** | `stg_oda__afe_v2` (Batch, 2.2K rows) |
| **Existing marts** | `dim_afes` — AFE master with well, field, budget type attributes |

**Cross-system note:** WellView AFEs are *operational authorizations* — approved spend on a job. ODA AFEs are *accounting entities* — they track GL postings against AFE numbers. Same real-world document, different representations. Link: `stg_oda__afe_v2.code` = `stg_wellview__job_afe_definitions.afe_number`.

---

## Model 2: Revenue Distribution — "Who earned what from production?"

Revenue flows from the wellhead through a cascade of ODA entities before landing in an owner's bank account. The Revenue Deck defines *who gets paid*; the Owner Revenue Detail records *what they were paid*; the Check Revenue records *the actual payment*.

### Entity Hierarchy

```
Cost Center (≈ Well)
│
└── Revenue Deck Set              ← well + product + company (what's being sold)
      └── Revenue Deck (V2)      ← one deck per DeckSetId, dated
            └── Deck Revision    ← versioned ownership snapshot (effective date)
                  │
                  ├── Deck Participant      ← one row per owner interest
                  │     ├── Interest Type  ← WI / ORRI / Royalty / NPI
                  │     └── Deck Deduction ← deduction rates (% off gross)
                  │                          [UNSTAGED — see sprint roadmap]
                  │
                  └── Owner Revenue Detail (V2)   ← per-owner revenue line item
                        │                           [UNSTAGED — see sprint roadmap]
                        │  (Production Month + Product + DecimalInterest +
                        │   NetVolume + NetValue + PaidAmount)
                        │
                        ├── Owner Revenue Detail Deductions  ← deduction breakdown
                        │                                     [UNSTAGED]
                        │
                        └── Check Revenue Detail (V2)  ← payment allocation
                                                         [UNSTAGED]
                              └── Check Revenue         ← actual revenue check issued
                                    └── CheckRevenue Detail Line Item
```

For AR invoices (the receivable side of owner revenue):

```
Cost Center (≈ Well)
│
└── AR Invoice (V2)               ← owner invoice / revenue statement (436K rows, CDC)
      ├── AR Invoice Payment      ← payment received (37.6K rows)
      ├── AR Invoice Adjustment   ← corrections, credit memos (10.7K rows)
      ├── AR Invoice Netting      ← cross-invoice netting (531K rows)
      └── AR Advance              ← advance against future revenue (27 rows)
            └── AR Advance Closeout ← advance reconciliation (0 rows — unused)
```

### Entity Details

#### Revenue Deck Set

The well-level container for revenue ownership. One deck set per (well + product + company) combination. The set itself is stable; the *revisions* underneath it change when ownership changes.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Grain** | Well × Product × Company |
| **Key flags** | `IsDefaultDeck`, `IsGasEntitlementDeck` |
| **Source tables** | `stg_oda__revenue_deck_set` (Batch, 19K rows) |

#### Deck Revision

A point-in-time snapshot of who owns what interest on a deck. When ownership changes (acquisition, divestiture, new lease), a new revision is created. The revision with the most recent close date that precedes the production month is the effective one.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Parent** | Revenue Deck (V2) via `DeckId` |
| **Effective dating** | `CloseDate` — the period this revision covers |
| **Revision state** | `RevisionStateId` → `stg_oda__revision_state` (Open/Closed) |
| **Source tables** | `stg_oda__revenue_deck_revision` (Batch, 346K rows) |

#### Deck Participant

One row per owner on a deck revision. This is where interest decimals live.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Parent** | Deck Revision via `DeckRevisionId` |
| **Owner** | `OwnerId` → `stg_oda__owner_v2` |
| **Interest** | `InterestTypeId` → `stg_oda__interest_type`, `DecimalInterest` |
| **Volume** | 196M rows — one row per owner per deck revision |
| **Source tables** | `stg_oda__revenue_deck_participant` (Batch, 196M rows) |

**Note on volume:** 196M deck participant rows reflects the full history of every ownership change across ~9.4K wells. Most of these are historical revisions. "Current ownership" requires filtering to the most recent closed revision per deck set.

#### Owner Revenue Detail (V2) — UNSTAGED

The central revenue distribution record. One row per owner × well × product × production month. This is the bridge between the deck participant's decimal interest and the actual check amount.

| Attribute | Description |
|-----------|-------------|
| **Grain** | Owner × Well × Voucher × Product × Production Month |
| **Key measures** | `NetVolume`, `NetValue`, `PaidAmount`, `DecimalInterest`, `BtuFactor` |
| **Interest type** | `InterestTypeId` — WI/ORRI/Royalty split |
| **Status** | `PaymentStatusId`, `StatementStatusId` |
| **Suspense** | `SuspenseCategoryId`, `PendingSuspenseCategoryId` |
| **Source table** | `ODA_BATCH_ODA_OWNERREVENUEDETAIL_V2` (row count unknown — likely millions) |
| **Status** | **NOT STAGED** — highest priority staging sprint for revenue domain |
| **Enables** | `fct_revenue_distribution`, `plat_well__net_economics`, `plat_owner__revenue_statement` |

#### Check Revenue

The actual revenue check issued to an owner. The check header is `CheckRevenue`; the detail allocation per owner+well is `CheckRevenueDetail_V2`.

| Attribute | Description |
|-----------|-------------|
| **Grain** | One check per owner per payment cycle |
| **Key measures** | `CheckAmount`, `IssuedDate`, `VoidedDate` |
| **Flags** | `SystemGenerated`, `Voided`, `Reconciled` |
| **Source tables** | `stg_oda__checkrevenue` (old connector, 659K rows) |
| **Related unstaged** | `ODA_CHECKREVENUEDETAIL_V2` — detail per check with working/non-working splits |

#### AR Invoice (V2)

The receivable invoice sent to an owner for revenue distribution. Think of it as the "bill" that precedes the Check Revenue payment. AR invoices and Revenue Checks are two representations of the same cash flow.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Owner** | Linked via `stg_oda__entity_v2` |
| **Amounts** | `InvoiceAmount`, `PaidAmount`, `RemainingBalance` (computed in `int_oda_ar_invoice_remaining_balances`) |
| **Flags** | `is_posted`, `is_voucher_posted`, `is_overage_invoice`, `is_include_in_accrual_report` |
| **Source tables** | `stg_oda__arinvoice_v2` (CDC, 436K rows) |
| **Existing marts** | `dim_ar_summary` — flat owner+well+invoice lookup with basic amounts |
| **Existing marts** | `fct_ar_aging_detail` — aging fact with remaining balance, aging buckets, posted/unposted split (1.18M rows) |
| **Existing marts** | `fct_ar_aging_summary` — aggregated by invoice_id (436K rows); use `SUM(CASE WHEN is_posted THEN ...)` splits — never GROUP BY on flags |

**Two AR mart distinction:**
- `dim_ar_summary` — lightweight, owner+well flattened, predates aging refactor. Good for basic invoice lookups and existing downstream consumers.
- `fct_ar_aging_detail` / `fct_ar_aging_summary` — full aging lifecycle. Use for aging reports, cash collection analysis, pre-JIB unposted invoice identification.

---

## Model 3: Cost Management — "What did we pay, and to whom?"

The cost side of ODA tracks vendor invoices (AP), inter-partner billings (JIB), and capital authorizations (AFE). All three ultimately post to the GL.

### Entity Hierarchy

```
Cost Center (≈ Well)
│
├── AP Invoice                   ← vendor invoice header (334K rows, CDC)
│     ├── AP Invoice Detail      ← line items: account + AFE coding, amounts (6.8M rows, CDC)
│     │     ├── Account          ← GL account for this line item
│     │     ├── AFE (V2)         ← capital authorization for this expenditure
│     │     ├── Expense Deck Set ← JIB routing for this expense
│     │     └── Well Allocation Deck ← multi-well allocation
│     └── AP Check               ← payment issued to vendor (49.5K rows)
│           └── AP Check Detail  ← invoice-level allocation per check [UNSTAGED]
│
├── JIB                          ← Joint Interest Billing header (63M rows, CDC)
│     └── JIB Detail             ← detail lines: owner + account + amounts (63M rows, CDC)
│
└── Expense Deck Set             ← who pays operating costs (mirrors Revenue Deck)
      └── Expense Deck (V2)      ← effective-dated deck version (9.7K rows)
            └── Deck Revision    ← versioned cost ownership snapshot (22K rows)
                  └── Deck Participant  ← entity + interest type + decimal (877K rows)
```

### Entity Details

#### AP Invoice

Vendor invoice header. Each AP invoice represents money owed to a vendor for goods or services. Invoice detail lines carry the account coding and AFE allocation.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Vendor** | `VendorId` → `stg_oda__vendor_v2` |
| **Voucher** | `VoucherId` → `stg_oda__voucher_v2` — links to GL posting |
| **Amounts** | `InvoiceAmount`, `PaidAmount`, `ToBePaid`, `DiscountAllowed`, `DiscountTakenAmount` |
| **Flags** | `is_posted` (GL posted), `is_approved_for_posting`, `is_ready_to_pay` |
| **Dates** | `InvoiceDate`, `DueDate`, `AccrualDate`, `AcceptanceDate` |
| **Source tables** | `stg_oda__apinvoice` (CDC, 334K rows) |

#### AP Invoice Detail

The line-item level of an AP invoice. This is where account coding and AFE assignment happen. One invoice has many detail lines (often by well or expense category).

| Attribute | Description |
|-----------|-------------|
| **Grain** | Invoice × Detail line |
| **Account coding** | `AccountId` → chart of accounts |
| **AFE coding** | `AfeId` → `stg_oda__afe_v2` |
| **Well coding** | `WellId` → `stg_oda__wells` |
| **Expense deck** | `ExpenseDeckSetId`, `ExpenseDeckRevisionId` — JIB routing |
| **Amounts** | `NetValue`, `GrossValue`, `ExpenseDeckInterest` |
| **Source tables** | `stg_oda__apinvoicedetail` (CDC, 6.8M rows) |

#### AP Check

Payment issued to a vendor against one or more AP invoices.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `Id` (UUID) |
| **Vendor** | `VendorId` |
| **Amount** | `PaymentAmount` |
| **Flags** | `SystemGenerated`, `Voided`, `Reconciled` |
| **Source tables** | `stg_oda__apcheck` (old connector, 49.5K rows) |
| **Existing marts** | `dim_ap_check_register` — AP payment history by vendor + check date |
| **Related unstaged** | `ODA_APCHECKDETAIL` — check-to-invoice payment allocation detail |

#### JIB / JIB Detail

Joint Interest Billing. When Formentera operates a well with working interest partners, JIB is how we bill them for their share of costs. JIB and JIBDetail are denormalized parallel tables (not parent-child in the traditional sense — both carry the full context per billing line).

| Attribute | Description |
|-----------|-------------|
| **JIB grain** | JIB header per billing cycle |
| **JIBDetail grain** | Detail line: Owner × Well × Account × Billing Period |
| **Key measures** | `GrossValue`, `NetValue`, `ExpenseDeckInterest` |
| **Billing status** | `BillingStatusId` — pending / billed / paid |
| **Expense deck link** | `ExpenseDeckRevisionId` — which ownership snapshot was used |
| **Source tables** | `stg_oda__jib` (CDC, 63M rows), `stg_oda__jibdetail` (CDC, 63M rows) |
| **Note** | JIB and JIBDetail rows are parallel denormalized views — no FK between them. Both link independently to wells, owners, and accounts. |

#### Expense Deck (mirrors Revenue Deck)

The same Set → Revision → Participant structure as Revenue Deck, but on the cost side. Determines what percentage of each expense a given entity is responsible for.

| Entity | Source Table | Rows |
|--------|-------------|------|
| Expense Deck Set | `stg_oda__expense_deck_set` | 7.6K |
| Expense Deck (V2) | `stg_oda__expense_deck_v2` | 9.7K |
| Expense Deck Revision | `stg_oda__expense_deck_revision` | 22K |
| Expense Deck Participant | `stg_oda__expense_deck_participant` | 877K |

---

## Master Data Entities

Shared dimensions used across all three models.

| Entity | Source Table | Rows | Current Mart | Notes |
|--------|-------------|------|-------------|-------|
| **Owner** | `stg_oda__owner_v2` | 42K | `dim_owners` | ODA financial owner — distinct from HubSpot `dim_owner`. Includes TaxId, entity link, payment preferences, withholding flags. |
| **Vendor** | `stg_oda__vendor_v2` | 4K | `dim_vendors` | AP vendor. `Terms` is text (e.g., "45D", "1M"), not an integer. |
| **Entity (V2)** | `stg_oda__entity_v2` | 56K | `dim_entities` | Legal entity wrapper used by Owner, Vendor, Purchaser. `Code` is TEXT (not numeric) in Snowflake. |
| **Company** | `stg_oda__company_v2` | 39 | `dim_companies` | ODA operating company. `CurrentAPMonth` is TIMESTAMP_LTZ (not int). |
| **Purchaser** | `stg_oda__purchaser_v2` | 183 | `dim_purchasers` | Oil/gas purchaser (who buys production). `ByWellRevenueReceivable` is the key flag. |
| **Well / Cost Center** | `stg_oda__wells` | 9.4K | `dim_wells` | `Code` right(6) = EID. `PropertyReferenceCode` = EID (direct). `ApiNumber` = API-10 for secondary matching. |
| **Account** | `stg_oda__account_v2` | 2K | `dim_accounts` | Chart of accounts with LOS hierarchy mapping from SharePoint account map. |
| **Interest Type** | `stg_oda__interest_type` | 5 | _inline_ | WI / ORRI / Royalty / NPI / Non-WI. Used in deck participants and owner revenue detail. |
| **Product** | `stg_oda__product` | 9 | _inline_ | Oil/Gas/NGL/Water product codes. |
| **Payment Type** | `stg_oda__payment_type` | 8 | _inline_ | Check / Wire / ACH / etc. |
| **Revision State** | `stg_oda__revision_state` | 3 | _inline_ | Open / Closed / Archived — used on deck revisions. |
| **Source Module** | `stg_oda__source_module` | 26 | _inline_ | AP / AR / JIB / Revenue / Manual — originating system for GL entries. |

---

## Cross-System Resolution

### Well / Cost Center Identity

The Well in ODA is called a "Cost Center." It links to all other source systems via EID.

| ODA Field | Value Pattern | Resolution |
|-----------|--------------|------------|
| `oda.Well.Code` | Right 6 characters = EID (e.g., "109181") | Primary join to `well_360` |
| `oda.Well.PropertyReferenceCode` | Direct EID string | Alternative direct join |
| `oda.Well.ApiNumber` | API-10 (10-digit) | Secondary fallback |

**Existing implementation:** `int_well__oda` joins `stg_oda__wells` to the EID spine. `well_360` uses COALESCE to select the best available attribute per domain (WellView = authoritative for drilling, ODA = authoritative for financial/entity attributes).

### Cross-System Entity Mapping

| ODA Entity | Other System | Link Mechanism |
|-----------|-------------|----------------|
| `oda.Well` (Cost Center) | WellView `wvWellHeader` | `right(oda.Well.Code, 6)` = EID OR `PropertyReferenceCode` |
| `oda.Well` | ProdView Unit | EID → `well_360.prodview_unit_id` |
| `oda.Afe_V2.Code` | WellView `wvJobAFE.AfeNumber` | AFE number string match |
| `oda.GL.AfeId` | WellView Job Cost | AFE + cost center + period (no direct FK) |
| `oda.JIBDetail` | WellView daily costs | Account coding + cost center (conceptual match, no FK) |
| `oda.Owner.EntityId` | HubSpot `dim_owner` | Owner Code / TaxId — no current automated link |
| `oda.RevenueDeckParticipant` | WellView `wvAgreementInt` | Conceptual overlap — both track WI/ORRI interests. No automated link. |

### WI/NRI Sources (Ownership Decimal)

ODA holds two representations of working interest:

| Source | Table | Grain | Coverage | Nature |
|--------|-------|-------|----------|--------|
| Deck Participant | `stg_oda__revenue_deck_participant` | Owner × Well × Effective Period | All wells | Full history — effective-dated |
| Deck Participant | `stg_oda__expense_deck_participant` | Entity × Well × Effective Period | All wells | Full history — expense side |
| Latest WI (computed) | `int_oda_latest_company_WI` | Well | ~9.4K wells | Point-in-time snapshot of company WI |
| Latest NRI (computed) | `int_oda_latest_company_NRI` | Well | ~9.4K wells | Point-in-time snapshot of company NRI |

WellView `wvWellHeader.UserNum1-5` stores a WI/NRI snapshot per well, but it's a manual entry field with ~30% population. The ODA deck participants are authoritative.

---

## Mart Roadmap (Gold Layer)

### Exists

| Mart | Type | Grain | Notes |
|------|------|-------|-------|
| `general_ledger` | Fact | GL entry | Incremental, 180M rows. All GL entries enriched with account/entity/well dims. Foundation for all cost/revenue analytics. |
| `fct_los` | Fact | GL entry (LOS subset) | Incremental. Filtered to LOS-mapped accounts only. LOS hierarchy from `dim_los_acct_map`. Clustered by `company_code + journal_date + los_category + los_section`. |
| `fct_ar_aging_detail` | Fact | AR Invoice | 1.18M rows. Remaining balance + aging buckets (0-30/31-60/61-90/90+) + posted/unposted split. Use for aging reports and cash collection. |
| `fct_ar_aging_summary` | Fact | AR Invoice (aggregated) | 436K rows. Aggregated from detail — GROUP BY invoice_id only, conditional SUM for posted/unposted. Never GROUP BY on flag columns. |
| `dim_ar_summary` | Summary | Invoice | Flat owner+well+invoice lookup. Predates aging refactor. Good for basic invoice lookups where aging detail is not needed. |
| `dim_ap_check_register` | Register | AP Check | AP payment history by vendor + check date. |
| `dim_revenue_check_register` | Register | Revenue Check | Revenue check history by owner. |
| `dim_accounts` | Dimension | Account | Chart of accounts with full LOS hierarchy + reporting flags + product type classification. |
| `dim_afes` | Dimension | AFE | AFE master with well, field, operating group, budget type. |
| `dim_companies` | Dimension | Company | ODA operating company master (39 rows). |
| `dim_entities` | Dimension | Entity | Legal entity master (56K rows). |
| `dim_owners` | Dimension | Owner | ODA financial owner master (42K rows). |
| `dim_purchasers` | Dimension | Purchaser | Oil/gas purchaser master (183 rows). |
| `dim_vendors` | Dimension | Vendor | AP vendor master (4K rows). |
| `dim_wells` | Dimension | Well/Cost Center | Finance view of wells — ODA attributes, EID, API-10, operating group, entity. |

### Planned

#### Near-Term (staging prerequisites in place or close)

| Mart | Type | Grain | Blocker | Value |
|------|------|-------|---------|-------|
| `fct_jib_billing` | Fact | JIBDetail line | JIB + JIBDetail staged (63M rows each) | Partner billing by well + account + period. Answers "what did we bill WI partners for?" Supports JIB settlement and partner account analysis. |
| `fct_cost_vs_budget` | Fact | AFE + Account + Month | AFE + GL staged | GL actuals vs AFE budget line by line. Drilling/completion cost control. Cross-references WellView job-level AFE estimates with ODA booked amounts. |
| `fct_ap_payment_activity` | Fact | AP Invoice + Check | AP tables staged | Invoice-to-payment lifecycle: days-to-pay, discount capture, outstanding AP by vendor and age. |
| `fct_deck_interest_history` | Fact | Owner × Well × Effective Date | Deck participant staged | Effective-dated WI/NRI/ORRI by owner and well. Revenue and expense interest ownership over time. Foundation for ownership-aware cost/revenue attribution. |
| `bridge_well_owner` | Bridge | Well × Owner (M:N) | Deck participants staged | Who has an interest in each well at any point in time. Enables filtering `general_ledger` or `fct_los` to "my share" of costs. |

#### Revenue Domain (requires new staging sprint)

These marts require `OwnerRevenueDetail_V2` and related tables to be staged. This is the highest-value unstaged table in ODA.

| Mart | Type | Grain | Staging Needed | Value |
|------|------|-------|---------------|-------|
| `fct_revenue_distribution` | Fact | Owner × Well × Product × Month | `OwnerRevenueDetail_V2` (likely millions of rows) | "Who received what from this well." Owner revenue by production month, product, interest type. Foundation for royalty statements, owner-level P&L, revenue reconciliation. |

**Staging sprint to unlock revenue distribution:**

| Table (ODA schema) | Estimated Rows | Why |
|---|---|---|
| `OwnerRevenueDetail_V2` | Large (millions) | Central — owner+well+product+month revenue line |
| `CheckRevenueDetail_V2` | Medium | Check-to-detail allocation with working/non-working splits |
| `RevenueDeckDeduction` | Small | Deduction rates per deck participant |
| `RevenueDeductionType_V2` | Tiny (lookup) | Decode deduction type codes (Code, CdexCode, Name) |

**Validation approach before staging:** Query `information_schema.tables` to confirm all four exist in `ESTUARY_DB.ESTUARY_SCHEMA`. Confirm connector type (batch vs CDC) by checking for `_meta/op` column existence. `OwnerRevenueDetail_V2` is the critical one — if it doesn't exist in Snowflake, revenue distribution is blocked regardless of the other three.

#### Platinum Layer (OBT — requires fct_revenue_distribution)

Denormalized, pre-joined tables for BI tooling. Land in `FO_PRODUCTION_DB.platinum`.

| Mart | Grain | Inputs | Value |
|------|-------|--------|-------|
| `plat_well__net_economics` | Well × Month | `fct_los` (LOE) + `fct_revenue_distribution` (revenue) | Revenue − LOE = NOI per well per month. The field-level P&L. |
| `plat_owner__revenue_statement` | Owner × Well × Product × Month | `fct_revenue_distribution` + `dim_owners` + `dim_wells` | Owner-facing revenue statement. Royalty/WI/ORRI breakdown by production with all enrichments pre-joined. |

---

## Known Gaps and Open Questions

| Area | Gap | Notes |
|------|-----|-------|
| **Revenue distribution** | `OwnerRevenueDetail_V2` not staged | Largest single gap. Blocks fct_revenue_distribution and all platinum revenue OBTs. |
| **AP payment detail** | `APCheckDetail` not staged | Check-to-invoice allocation detail. Needed for full AP payment reconciliation. |
| **Check revenue detail** | `CheckRevenueDetail_V2` not staged | Working/non-working volume splits per revenue check. |
| **Owner → HubSpot link** | No automated resolution | `dim_owners` (ODA) and `dim_owner` (HubSpot) are separate marts. TaxId/Code could link them but no automated reconciliation exists. |
| **WellView agreements → ODA decks** | No automated link | `wvAgreementInt` (WellView) and deck participants (ODA) both track WI/ORRI but are not linked. `wvAgreement` is unstaged. |
| **AFE budget vs actuals** | No `fct_cost_vs_budget` mart | AFE + GL data is staged; mart just needs to be built. |
| **JIB analytics** | No `fct_jib_billing` mart | JIB + JIBDetail are staged; mart just needs to be built. |
| **`lease_operating_statement`** | Deprecated — will be removed | `fct_los` is the canonical replacement. |
