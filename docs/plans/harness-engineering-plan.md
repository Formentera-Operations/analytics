# Plan: Harness Engineering for dbt

## Context

Inspired by OpenAI's ["Harness Engineering"](https://openai.com/index/harness-engineering/) article, which describes building a million-line codebase with zero manually-written code by investing in scaffolding, feedback loops, and automated enforcement. This plan applies those principles to our dbt analytics project to enable agents (Claude Code, OpenClaw) to work autonomously on model development, refactoring, and maintenance.

### What Already Exists

| Component | Status | Notes |
|---|---|---|
| `CLAUDE.md` (dispatch) | ✅ Ready | 103-line table of contents pointing to deep docs |
| `docs/conventions/` | ✅ Ready | 6 convention docs (staging, intermediate, marts, SQL patterns, incremental, testing) |
| `docs/reference/` | ✅ Ready | 5 reference docs (sources, macros, routing, glossary, packages) |
| `context/sources/prodview/` | ✅ Ready | System overview + 13 domain YAMLs |
| `context/sources/wellview/` | ✅ Ready | System overview + 16 domain YAMLs |
| `scripts/validate_staging.py` | ✅ Ready | Structural linter for 5-CTE pattern, tags, surrogate keys |
| `scripts/check.sh` | ✅ Ready | Unified feedback loop (parse → lint → validate → build) |
| `docs/solutions/` | ✅ Ready | 11 solution docs for known error patterns |
| CI pipeline | ✅ Ready | `dbt parse` → `dbt build --select state:modified+ --defer` |

### What's Missing (This Plan Fills)

| # | Initiative | Article Concept | What It Does |
|---|---|---|---|
| 1 | Entropy & Garbage Collection | "Technical debt is a high-interest loan" | Automated repo hygiene — detect and fix drift continuously |
| 2 | Dependency Direction Validator | "Strict dependency directions, enforced mechanically" | Prevent architectural drift in the DAG |
| 3 | Context File Completeness | "Repository knowledge as system of record" | Ensure every source has agent-readable domain knowledge |
| 4 | Agent Self-Review | "Agent reviews its own changes, iterates until satisfied" | Second-pass validation against context/schema definitions |
| 5 | Batch Refactoring Workflow | "Increasing levels of autonomy" | Agent-driven conversion of non-compliant models at scale |

---

## Initiative 1: Entropy & Garbage Collection

**Article concept:** *"Technical debt is like a high-interest loan: it's almost always better to pay it down continuously in small increments than to let it compound. Human taste is captured once, then enforced continuously on every line of code."*

**Problem:** 76 of 212 staging models don't pass structural validation. Tags are inconsistent across sources. Schema YAML coverage is uneven. Without continuous cleanup, every agent-generated PR replicates existing bad patterns.

### Deliverable: `scripts/audit_repo.py`

A repo-wide audit script that produces a prioritized hygiene backlog. Not a fixer — a detector. Run on-demand or scheduled.

**Checks to implement:**

| Check | Severity | Output |
|---|---|---|
| Staging models failing `validate_staging.py` | error | List of files + violation summary |
| Models with no downstream `ref()` (orphans) | warning | Candidate for removal |
| Models missing schema YAML (no tests defined) | warning | List by layer and source |
| Schema YAML files with no `unique` + `not_null` on PK | error | Model + column |
| Tag inconsistencies (non-canonical patterns) | warning | Current vs expected |
| Intermediate models overriding to `view` (legacy) | warning | Candidates to convert to ephemeral |
| Source definitions missing `freshness` config | info | List of sources |
| Macro usage: inline conversions where a macro exists | warning | File + line + suggested macro |

**Output formats:** text (human review), JSON (agent consumption), GitHub Issues (automated backlog).

**Approach:**
1. Build `audit_repo.py` that runs all checks and produces the backlog
2. Add a `--fix` flag for mechanical fixes (tag standardization, adding missing config blocks)
3. Wire into OpenClaw as a scheduled task: run weekly, open a hygiene PR with mechanical fixes, create GitHub Issues for items requiring judgment

**Effort:** Medium. The validator infrastructure exists — this extends it repo-wide. Most checks are manifest/file inspection, not SQL parsing.

**Dependencies:** None. Can start immediately.

---

## Initiative 2: Dependency Direction Validator

**Article concept:** *"Each business domain is divided into a fixed set of layers, with strictly validated dependency directions and a limited set of permissible edges. These constraints are enforced mechanically."*

**Problem:** Nothing prevents a staging model from `ref()`-ing a mart, or an application model from `ref()`-ing a staging model directly. As agents build more models, the DAG could silently violate layer boundaries.

### Deliverable: `scripts/validate_dependencies.py`

Parses the dbt manifest and enforces directional dependency rules.

**Rules:**

```
staging      → can only ref: sources
intermediate → can only ref: staging, intermediate (same or upstream domain)
marts        → can only ref: staging, intermediate, marts
applications → can only ref: marts, intermediate
```

**Cross-tenant rule:** Operations models cannot ref Partners models, and vice versa.

**Edge cases to handle:**
- `dbt_utils` and `elementary` refs (package models) — allow all layers
- Seeds — allow from any layer
- Self-refs (`{{ this }}`) in incremental models — allow

**Output:** Same pattern as `validate_staging.py` — agent-readable violations with remediation. Example:

```
FAIL: models/operations/marts/finance/general_ledger.sql
  ✗ [DEPENDENCY_DIRECTION] refs staging model 'stg_oda__gl' directly.
    → Marts should ref intermediate models, not staging. Create an
      intermediate model or ref an existing one.
```

**Approach:**
1. Generate manifest with `dbt parse`
2. Walk the `manifest.json` parent map
3. Classify each node by layer (from path)
4. Check every edge against the rule table
5. Wire into `scripts/check.sh` as an optional step

**Effort:** Small-medium. Manifest parsing is straightforward. The rule table is simple. Most work is handling edge cases.

**Dependencies:** Requires `dbt parse` to generate `target/manifest.json`. Could be a problem in environments without Snowflake credentials — consider caching the manifest in CI artifacts.

---

## Initiative 3: Context File Completeness

**Article concept:** *"Repository knowledge is the system of record. Design documentation is catalogued and indexed, including verification status."*

**Problem:** ProdView and WellView have rich context files. ODA, Combo Curve, Enverus, Procount, Aegis, and HubSpot have none. Agents working on those sources have no domain knowledge to draw on — they can only pattern-match from existing models, which are themselves non-compliant.

### Deliverable: Context files for remaining sources + a completeness checker

**Phase 1 — Source overview docs (`.md` files):**

| Source | Priority | Complexity | Notes |
|---|---|---|---|
| `oda` | High | Medium | 45 staging models, most violations. CDC pattern, GL hierarchy, deck structures. Rob has deep domain knowledge. |
| `procount` | Medium | Low | 12 models, Griffin/Barnett specific. Simpler schema. |
| `combo_curve` | Medium | Low | 11 models, economics forecasting. API-sourced. |
| `enverus` | Low | Low | 4 models, third-party reference data. |
| `aegis` | Low | Low | 6 tables, market pricing. |
| `hubspot` | Low | Low | 1 table, CRM contacts. |

Each `.md` follows the same structure as `prodview.md` and `wellview.md`:
- System overview and purpose
- Core entity hierarchy
- Snowflake database/schema location
- Ingestion pattern (CDC type, soft deletes, dedup)
- Key gotchas and domain-specific conventions
- Join patterns between tables

**Phase 2 — Domain YAML schema files:**

For ODA (the highest-priority gap), build column-level YAML files similar to the ProdView YAMLs. These define what each source column means, its data type, and its business purpose. Start with the GL tables (most used), then expand to AP, AR, AFE, JIB, and decks.

**Phase 3 — Completeness checker:**

Add to `audit_repo.py`:

```
For each source directory in models/operations/staging/:
  ✓ Does context/sources/{source}/{source}.md exist?
  ✓ Does it have a core hierarchy section?
  ✓ Does it document the ingestion pattern?
  ✓ For each staging model, does a corresponding YAML schema entry exist?
```

**Approach:**
1. Start with ODA — Rob drafts the system overview from domain knowledge, agent generates the YAML schema files from Snowflake `INFORMATION_SCHEMA` + existing staging model column names
2. Use a consistent template across all sources
3. Add `status: verified | draft | stale` header to each context file
4. Add completeness check to the audit script

**Effort:** Medium-high for ODA (45 tables, complex domain). Low for the smaller sources. The YAML generation can be partially automated by querying Snowflake schema metadata.

**Dependencies:** Snowflake access for schema introspection. Domain knowledge from Rob for business definitions.

---

## Initiative 4: Agent Self-Review

**Article concept:** *"We instruct the agent to review its own changes locally, request additional agent reviews, respond to feedback, and iterate in a loop until all reviewers are satisfied."*

**Problem:** Current validation checks structure (5 CTEs, tags, config) but not *semantic correctness*. An agent could write a staging model with all 5 CTEs in the right order but name columns incorrectly, skip unit conversions, or miss foreign key relationships that the context YAML defines.

### Deliverable: `scripts/review_staging.py`

A semantic review pass that compares agent-generated staging models against their context YAML schema definitions.

**Checks:**

| Check | What It Catches |
|---|---|
| Column coverage | Source columns in the YAML that aren't represented in the staging model |
| Column naming | Staging column names that don't match the YAML's suggested business names |
| Unit conversion | Columns flagged as needing conversion in the YAML that use raw values instead of macros |
| FK relationships | `idrecparent`-style columns that should be typed and named per the YAML's relationship map |
| Grain validation | The model's `qualify` dedup key matches the YAML's declared primary key |

**Output example:**

```
REVIEW: stg_prodview__monthly_allocations.sql
  ⚠ [COLUMN_COVERAGE] 3 source columns from allocations.yaml not found in model:
      volprodgathcond, volprodgathsolvent, volnewprodgathcond
    → These may be intentionally excluded. If so, add to exclusion list in YAML.
  ⚠ [UNIT_CONVERSION] Column 'durdown' converted with pv_days_to_hours but
      allocations.yaml says unit is 'days' with target 'hours' — conversion verified ✓
  ✓ [GRAIN] Dedup key 'idrec' matches YAML primary key — correct
```

**Approach:**
1. Parse the context YAML for a given source/domain
2. Parse the staging SQL (reuse the Jinja stripping from `validate_staging.py`)
3. Map source columns → renamed columns in the `renamed` CTE
4. Compare against the YAML expectations
5. Output agent-readable review with actionable suggestions

**Important constraint:** This only works for sources that have context YAML files. It's gated on Initiative 3 progress. Start with ProdView and WellView (already have YAMLs), extend as new context files are created.

**Effort:** Medium-high. The YAML parsing and SQL column extraction are straightforward. The tricky part is fuzzy matching between source column names (UPPERCASE, concatenated) and staged column names (snake_case, renamed). The existing ProdView models serve as training data for the mapping logic.

**Dependencies:** Initiative 3 (context file completeness). Only useful for sources with YAML schema files.

---

## Initiative 5: Batch Refactoring Workflow

**Article concept:** *"Agent throughput far exceeds human attention. Corrections are cheap. Waiting is expensive."*

**Problem:** 76 staging models need 5-CTE conversion. Doing them one at a time via manual Claude Code prompts is slow and doesn't leverage the validator infrastructure. The ODA source alone has 45 models — at the Sprint 1 pace of 6 models per PR, that's 7-8 PRs just for ODA.

### Deliverable: `scripts/batch_refactor.py` + workflow documentation

An orchestration script that converts non-compliant staging models to the 5-CTE pattern, validates each conversion, and batches them into domain-scoped PRs.

**Workflow:**

```
1. Run validate_staging.py --format json to get the failure list
2. Group failures by source directory
3. For each source:
   a. Load the context file (if exists) for domain knowledge
   b. Load 2-3 compliant reference models from the same source
   c. For each non-compliant model:
      i.   Read the current model
      ii.  Generate the 5-CTE refactored version (via Claude Code / API)
      iii. Run validate_staging.py on the result
      iv.  If pass: stage the file
      v.   If fail: log the failure, skip (human review needed)
   d. Run dbt parse --warn-error on the batch
   e. Open a PR with the batch
```

**Domain priority order:**

| Source | Models to Convert | Has Context File | Estimated Complexity |
|---|---|---|---|
| ODA | 45 | ❌ Not yet | High — Estuary CDC pattern, complex GL logic |
| Procount | 12 | ❌ Not yet | Medium — Fivetran, simpler schema |
| Combo Curve | 11 | ❌ Not yet | Low — minimal transformations |
| Enverus | 4 | ❌ Not yet | Low — straightforward rename models |
| SharePoint | 2 | ❌ Not yet | Low — simple reference data |
| HubSpot | 1 | ❌ Not yet | Trivial |
| Aegis | 1 | ❌ Not yet | Trivial |

**Two modes:**

1. **Mechanical mode** (no context file needed): Add missing CTEs with `select *` passthrough, add config blocks, standardize tags. Gets models to pass structural validation without changing business logic. This is the "garbage collection" pass.

2. **Full refactor mode** (requires context file): Rename columns per YAML schema, add proper type casting, apply unit conversion macros, add surrogate keys with correct columns. This produces production-quality models. Gated on Initiative 3.

**Approach:**
1. Build the orchestration script that wraps Claude Code CLI
2. Start with mechanical mode on the easy sources (Combo Curve, Enverus, SharePoint, HubSpot, Aegis) — 19 models, should be completable in a single session
3. Tackle Procount next — 12 models, Fivetran pattern same as ProdView
4. ODA last — requires context files first (Initiative 3), plus careful handling of the Estuary CDC pattern

**Effort:** Medium for the script, low-high per source depending on context file availability and source complexity.

**Dependencies:** Initiative 3 (context files) for full refactor mode. Mechanical mode can run immediately. Claude Code CLI access for the generation step.

---

## Implementation Sequence

```
                    Month 1                          Month 2                     Month 3
            ┌───────────────────────┐    ┌───────────────────────┐    ┌─────────────────────┐
            │                       │    │                       │    │                     │
Initiative  │  1. Audit script      │    │  3b. ODA context      │    │  4. Self-review     │
  order     │  2. Dependency        │    │      YAML files       │    │     (ProdView +     │
            │     validator         │    │  5b. ODA full refactor │    │      WellView)      │
            │  3a. ODA overview.md  │    │                       │    │  4b. Extend to ODA  │
            │  5a. Mechanical       │    │                       │    │                     │
            │      refactor         │    │                       │    │                     │
            │      (easy sources)   │    │                       │    │                     │
            └───────────────────────┘    └───────────────────────┘    └─────────────────────┘

OpenClaw    ┌─────────────────────────────────────────────────────────────────────────────────┐
 (ongoing)  │  Garbage collection: weekly audit → auto-fix mechanical issues → open PRs       │
            └─────────────────────────────────────────────────────────────────────────────────┘
```

### Month 1: Foundation

**Week 1-2:**
- Build `audit_repo.py` (Initiative 1) — the full repo health check
- Build `validate_dependencies.py` (Initiative 2) — DAG direction enforcement
- Wire both into `scripts/check.sh`

**Week 3-4:**
- Write `context/sources/oda/oda.md` (Initiative 3a) — system overview from Rob's domain knowledge
- Run mechanical refactor (Initiative 5a) on Combo Curve, Enverus, SharePoint, HubSpot, Aegis — 19 models, should clear in 2-3 PRs
- Run mechanical refactor on Procount — 12 models

This gets the repo from 64% compliant to ~88% compliant (only ODA remaining).

### Month 2: ODA Deep Dive

- Generate ODA domain YAML schema files from Snowflake metadata (Initiative 3b)
- Rob reviews and enriches with business definitions (GL account hierarchy, deck structures, CDC nuances)
- Run full refactor on ODA staging models (Initiative 5b) — 45 models, batched by domain (GL, AP, AR, AFE, JIB, decks)
- Write context files for remaining minor sources (Procount, Combo Curve, Enverus)

This gets the repo to 100% structural compliance.

### Month 3: Semantic Layer

- Build `review_staging.py` (Initiative 4) — semantic review against context YAMLs
- Run against ProdView and WellView staging models to validate column coverage and naming
- Extend to ODA once context YAMLs are complete
- Begin using context files to inform Cortex semantic model definitions

### Ongoing: OpenClaw Garbage Collection

Once the audit script exists, set up an OpenClaw agent team that:
- Runs `audit_repo.py` weekly
- Auto-fixes mechanical issues (tag normalization, missing `_loaded_at`)
- Opens PRs for mechanical fixes with validation proof
- Creates GitHub Issues for items requiring human judgment
- Reports a weekly "repo health score" (% passing structural + semantic validation)

---

## Success Metrics

| Metric | Current | Month 1 Target | Month 3 Target |
|---|---|---|---|
| Staging structural compliance | 64% (136/212) | 88% | 100% |
| Sources with context files | 2/10 | 4/10 | 8/10 |
| Sources with YAML schemas | 2/10 | 2/10 | 5/10 |
| Dependency direction violations | Unknown | Measured | 0 |
| Models with PK tests (unique + not_null) | Unknown | Measured | >80% |
| Agent autonomy level | Manual prompting | Task-scoped batches | Self-identifying work |

---

## Principles (from the article, adapted for dbt)

1. **When something fails, the fix is never "try harder" — it's "what capability is missing?"** Every agent failure becomes a harness improvement, not a better prompt.
2. **When documentation falls short, promote the rule into code.** If you write the same review comment twice, it becomes a validator rule.
3. **Enforce boundaries centrally, allow autonomy locally.** The 5-CTE pattern, dependency directions, and tag schema are non-negotiable. Column naming, CTE internals, and SQL style within those CTEs are flexible.
4. **Human taste is captured once, then enforced continuously.** Rob's domain knowledge goes into context files. Convention decisions go into validators. Neither needs to be re-explained to every agent session.
5. **Corrections are cheap, waiting is expensive.** Prefer fast PRs with automated validation over blocking reviews. The validator is the reviewer.
