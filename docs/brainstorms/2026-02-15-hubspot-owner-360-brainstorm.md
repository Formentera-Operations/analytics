# HubSpot Owner 360 Foundation — Brainstorm

**Date:** 2026-02-15
**Status:** Ready for planning
**Author:** Rob + Claude

## What We're Building

A foundational data layer for Owner Relationship Management (ORM), where "owners" are mineral royalty owners. The work covers:

1. **Staging refactor** — Bring `stg_hubspot__companies` into 5-CTE compliance, build new `stg_hubspot__contacts` model
2. **Owner mart** — `dim_owner` providing a unified Owner 360 view at the contact level

### Scope Boundaries

**In scope:**
- Refactor `stg_hubspot__companies` (5-CTE pattern, trim to relevant properties)
- New `stg_hubspot__contacts` (5-CTE pattern, relevant properties only)
- `dim_owner` mart (contact-centric, company as attribute)
- Context documentation for HubSpot source system

**Out of scope (future sprints):**
- `stg_hubspot__owners` (internal reps/account managers) — skip for now
- Engagement tracking (emails, meetings, engagements tables)
- Entity resolution to internal systems (well_360, ODA, ProdView)
- Tickets, forms, property history tables

## Why This Approach

### Contact-Centric Owner Model (Approach A)

The owner entity is the **Contact**. Companies are a grouping/legal-entity attribute on the contact.

**Rationale:**
- In ORM, the person you communicate with IS the owner for practical purposes
- Individual owners: contact = company (1:1) — straightforward
- LLCs/Trusts: multiple contacts share the same `company_id` — the legal entity is represented by the company, but each person associated with it gets their own owner record
- Simple grain (one row per contact) makes downstream consumption easy
- Company/legal-entity info enriches the contact record via denormalization
- Can always promote companies to their own dimension later if needed

**Rejected alternatives:**
- **Dual-Entity (B):** Companies and contacts as separate dimensions with bridge table. Too complex for first iteration — adds 3 models vs 1 for marginal benefit.
- **Company-Centric (C):** Owner = Company, contacts nested. Awkward for individual owners who don't have a meaningful "company" entity.

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Owner grain | Contact (person) | ORM is about communicating with people |
| Company role | Denormalized attribute on dim_owner | Avoids bridge complexity; company_id available for grouping |
| Internal reps (HubSpot owners) | Skip for now | Not needed for Owner 360 v1 |
| Column scope | Trim to relevant | 165+ properties is excessive; audit which have data, keep what matters |
| Entity resolution | TBD — future sprint | Need to investigate shared keys (vendor_number? tax_id?) between HubSpot and internal systems |
| Staging pattern | 5-CTE (Portable variant) | No dedup CTE needed; `_portable_extracted` metadata; `_loaded_at = current_timestamp()` |
| Ingestion | Portable (full snapshots) | Already configured; no CDC/dedup patterns needed |

## Open Questions

1. **Which contact/company properties actually have data?** Need to audit `information_schema.columns` and sample data before deciding final column lists — per project memory, never trust source definitions alone for Portable sources.

2. **Contact-to-company relationship:** Is it stored in the contacts table (as a `company_id` property) or in a separate association table? Need to check HubSpot's data model and what Portable syncs.

3. **Entity resolution keys:** Do HubSpot records have custom properties (vendor_number, tax_id_number, entity_id) that reliably map to internal systems? The companies model has `vendor_number` and `tax_id_number` fields — need to check population rates.

4. **LLC/Trust representation:** When a trust has 3 beneficiaries as contacts, does each contact have the trust as their company? Or are there separate association records?

5. **Soft delete pattern:** HubSpot uses `archived` (boolean) not `deleteddate`. Confirm this applies to contacts as well as companies.

6. **dim_owner surrogate key:** Should it be based on `contact_id` alone, or a composite key? Contact-centric approach suggests `contact_id` is sufficient.

## Data Model Sketch

```
stg_hubspot__companies (view, 5-CTE)
  └── company_id (PK)
  └── company_name, company_type, is_llc, is_trust, ...
  └── address fields, tax fields, well-related fields
  └── _loaded_at, _portable_extracted

stg_hubspot__contacts (view, 5-CTE)  [NEW]
  └── contact_id (PK)
  └── company_id (FK → companies)
  └── first_name, last_name, email, phone, ...
  └── lifecycle_stage, owner_status, ...
  └── _loaded_at, _portable_extracted

dim_owner (table)  [NEW]
  └── owner_sk (surrogate key from contact_id)
  └── contact_id (natural key)
  └── -- Contact fields --
  └── first_name, last_name, full_name, email, phone
  └── -- Company fields (denormalized) --
  └── company_id, company_name, company_type
  └── is_llc, is_trust (derived flags)
  └── -- Address --
  └── address, city, state, zip, country
  └── -- Metadata --
  └── created_at, updated_at, _loaded_at
```

## Implementation Prerequisites

Before building, the planning phase must:

1. **Validate source tables exist** — Query `information_schema.tables` for `hubspot_raw.contacts` (critical: per project memory, `dbt parse` won't catch missing tables)
2. **Audit property population** — Sample the JSON `properties` column to see which fields have data
3. **Create context documentation** — `context/sources/hubspot/hubspot.md` + per-table YAMLs
4. **Confirm contact-company join key** — Verify how HubSpot associates contacts to companies in the Portable-synced data

## Sprint Structure (Suggested)

| Sprint | Deliverables |
|--------|-------------|
| 1 | Context docs + source table validation + `stg_hubspot__companies` refactor (5-CTE) |
| 2 | `stg_hubspot__contacts` (5-CTE) + YAML docs |
| 3 | `dim_owner` mart + tests |
