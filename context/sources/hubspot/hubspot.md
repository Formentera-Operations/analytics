# HubSpot — Source System Overview

## System Purpose

HubSpot is used as the **Owner Relationship Management (ORM)** platform for managing mineral royalty owner records. It stores contact and company information for ~53K owner entities and ~19K contacts.

In this context, "owners" are mineral/royalty interest holders — NOT HubSpot's internal "owners" table (which tracks internal sales reps).

## Ingestion

- **Connector:** Portable (full snapshots, no CDC/dedup)
- **Raw database:** `FO_RAW_DB.HUBSPOT_RAW`
- **Metadata column:** `_portable_extracted` (extraction timestamp)
- **Soft delete:** `archived` boolean (NOT `deleteddate` like other Portable sources)
- **Dedup:** Not needed (Portable full snapshots)

## Core Hierarchy

```
Company (legal entity / individual)
  └── Contact (person associated with the company)
       └── linked via ASSOCIATEDCOMPANYID on contacts → ID on companies
```

### Key Relationships

| From | To | Join Key | Population |
|------|----|----------|------------|
| Contact | Company | `contacts.properties:ASSOCIATEDCOMPANYID` → `companies.id` | 43.9% of contacts |
| Company | HubSpot Owner (internal rep) | `companies.properties:HUBSPOT_OWNER_ID` → `owners.id` | 5.3% of companies |

### Owner Model (Contact-Centric)

The "owner" entity for ORM purposes is the **Contact** (person). Companies represent the legal entity (LLC, Trust, etc.) that may group multiple contacts. Most companies represent individual owners (1:1 with contact), but LLCs/Trusts can have multiple contacts sharing one company.

**Entity type distribution (by company name pattern):**
- Individual/Other: 44,525 (83.9%)
- LLC: 3,046 (5.7%)
- Trust: 2,681 (5.0%)
- Corporation: 1,443 (2.7%)
- Estate: 883 (1.7%)
- LP: 584 (1.1%)

## Table Structure

Both `companies` and `contacts` share the same Portable schema:

| Column | Type | Description |
|--------|------|-------------|
| `ID` | TEXT | Primary key |
| `ARCHIVED` | BOOLEAN | Soft delete flag |
| `CREATEDAT` | TIMESTAMP_NTZ | Record creation timestamp |
| `UPDATEDAT` | TIMESTAMP_NTZ | Record last update timestamp |
| `PROPERTIES` | VARIANT | JSON blob of all HubSpot properties |
| `URL` | TEXT | HubSpot record URL |
| `_PORTABLE_EXTRACTED` | TIMESTAMP_NTZ | Portable extraction timestamp |

All business fields are stored in the `PROPERTIES` VARIANT column as JSON keys. Property keys are UPPER_SNAKE_CASE.

## Tables Available

| Table | Rows | Staging Model | Notes |
|-------|------|--------------|-------|
| `companies` | 53,162 | `stg_hubspot__companies` | Owner legal entities |
| `contacts` | 19,174 | `stg_hubspot__contacts` | Owner contact persons |
| `owners` | 32 | — | Internal HubSpot reps (skip for ORM v1) |
| `emails` | 98,531 | — | Future: engagement tracking |
| `engagements` | 142,088 | — | Future: activity tracking |
| `tickets` | 26,435 | — | Future: support tracking |
| `meetings` | 579 | — | Future: meeting tracking |
| `forms` | 15 | — | Form definitions |
| `teams` | 9 | — | Internal team structure |
| `users` | 44 | — | Internal users |

## Business-Specific Properties

HubSpot is customized with oil & gas owner management fields:

**Company-level:**
- `OWNER_NUMBER___OGSYS` — Link to OGSys owner number (entity resolution key)
- `ACQUIRED_OWNER_NUMBER` — Previous owner number from acquisition
- `TAX_ID_NUMBER` — SSN/EIN for 1099 reporting (78.8% populated)
- `VENDOR_NUMBER` — Link to vendor system (0% populated — not yet used)
- `ASSET` — Asset/property association (68.1%)
- `WORKING_INTEREST_OWNER` — Boolean flag for WI owners (66.0%)
- Revenue/payment fields: `MINIMUM_CHECK_AMOUNT`, `REVENUE_PAYMENT_TYPE`, `NETTING_CODE`, `STATEMENT_DELIVERY`
- Withholding fields: `FEDERAL_WITHHOLDING`, `STATE_WITHHOLDING`, `TAX_STATUS`, `TAX_EXEMPT`

**Contact-level:**
- `ASSOCIATEDCOMPANYID` — Link to company record (43.9%)
- `REV_STATEMENT_DELIVERY` — Revenue statement delivery preference (10.6%)
- `REV_STATEMENT_EMAIL` — Email for revenue statements (10.5%)
- Standard contact fields: name, email, phone, address

## Gotchas

1. **Soft delete is `archived`, not `deleteddate`** — Different from other Portable sources
2. **Contact-company link is 43.9% populated** — Many contacts (especially older imports) lack company association
3. **Entity type detection is name-based** — No explicit `company_type` field; must parse company names for LLC, Trust, Estate, LP, Corp patterns
4. **Property population varies widely** — From 100% (NAME, ACTIVE) to 0% (VENDOR_NUMBER, well fields). Always validate before adding columns.
5. **Properties are in a VARIANT column** — Access via `properties:KEY_NAME::type` syntax
6. **HubSpot "owners" table is internal reps** — Do NOT confuse with mineral royalty owners
