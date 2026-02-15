#!/usr/bin/env python3
"""
validate_staging.py — Structural linter for dbt staging models.

Enforces the 5-CTE staging pattern and conventions defined in docs/conventions/staging.md.
Designed to produce agent-readable output so coding agents can self-correct.

Usage:
    # Validate specific files
    python scripts/validate_staging.py models/operations/staging/oda/stg_oda__gl.sql

    # Validate all staging models
    python scripts/validate_staging.py

    # Validate changed files (CI mode)
    python scripts/validate_staging.py --changed

    # JSON output for programmatic consumption
    python scripts/validate_staging.py --format json

    # Summary only (no per-file details)
    python scripts/validate_staging.py --summary

Exit codes:
    0 — all checked models pass
    1 — one or more models have violations
    2 — script error (bad arguments, file not found)
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = PROJECT_ROOT / "models" / "operations" / "staging"

# The 5 required CTE names, in order
REQUIRED_CTES = ["source", "renamed", "filtered", "enhanced", "final"]

# Canonical tag pattern: ['{source}', 'staging', 'formentera']
# Third tag should always be 'formentera' for operations models
CANONICAL_TAG_THIRD = "formentera"

# Sources and their expected tag names
SOURCE_TAG_MAP = {
    "oda": "oda",
    "prodview": "prodview",
    "wellview": "wellview",
    "procount": "procount",
    "combo_curve": "combo_curve",
    "enverus": "enverus",
    "aegis": "aegis",
    "hubspot": "hubspot",
    "sharepoint": "sharepoint",
    "afe_execute": "afe_execute",
}

# Paths that are excluded from strict 5-CTE enforcement.
# wiserock_tables are simplified app-specific views, not canonical staging.
EXCLUDE_PATTERNS = [
    "wiserock_tables",
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class Violation:
    """A single rule violation with remediation guidance."""

    rule: str
    severity: str  # "error" | "warning"
    message: str
    line: int | None = None
    remediation: str = ""


@dataclass
class FileResult:
    """Validation result for a single file."""

    path: str
    violations: list[Violation] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""

    @property
    def passed(self) -> bool:
        return not any(v.severity == "error" for v in self.violations)

    @property
    def error_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "warning")


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def strip_jinja(sql: str) -> str:
    """Remove Jinja blocks/expressions for structural analysis.

    Preserves line count by replacing with spaces so line numbers stay valid.
    """
    # Remove block tags  {% ... %}
    sql = re.sub(r"\{%.*?%\}", lambda m: " " * len(m.group()), sql, flags=re.DOTALL)
    # Remove expression tags  {{ ... }}
    sql = re.sub(r"\{\{.*?\}\}", lambda m: " " * len(m.group()), sql, flags=re.DOTALL)
    # Remove comments  {# ... #}
    sql = re.sub(r"\{#.*?#\}", lambda m: " " * len(m.group()), sql, flags=re.DOTALL)
    return sql


def extract_config_block(raw_sql: str) -> str | None:
    """Extract the config() block from raw SQL (before Jinja stripping)."""
    match = re.search(
        r"\{\{\s*config\s*\((.*?)\)\s*\}\}", raw_sql, re.DOTALL
    )
    return match.group(1) if match else None


def extract_tags(config_text: str) -> list[str]:
    """Parse tags from a config block string."""
    match = re.search(r"tags\s*=\s*\[(.*?)\]", config_text, re.DOTALL)
    if not match:
        return []
    raw = match.group(1)
    return [t.strip().strip("'\"") for t in raw.split(",") if t.strip()]


def find_cte_names(sql_stripped: str) -> list[tuple[str, int]]:
    """Find all CTE definitions and their line numbers.

    Returns list of (cte_name, line_number) tuples in order of appearance.
    Handles patterns like:
      - 'with source as ('        (same line)
      - 'with\n\nsource as ('     (with on separate line)
      - ', renamed as ('           (subsequent CTEs)
      - 'renamed as ('             (start of line)
    """
    ctes = []

    # First, strip the leading 'with' keyword to simplify matching.
    # Replace 'with' (as CTE introducer) with whitespace.
    cleaned = re.sub(
        r"\bwith\b(?=[\s\n]+\w+\s+as\s*\()",
        lambda m: " " * len(m.group()),
        sql_stripped,
        count=1,
        flags=re.IGNORECASE,
    )

    # Now match: name as (
    pattern = re.compile(
        r"(?:^|[,\s])(\w+)\s+as\s*\(", re.IGNORECASE | re.MULTILINE
    )
    for match in pattern.finditer(cleaned):
        name = match.group(1).lower()
        # Skip SQL keywords that aren't CTE names
        if name in (
            "select", "insert", "update", "delete", "merge",
            "with", "not", "cast", "case", "when", "then",
            "else", "end", "from", "where", "and", "or",
            "join", "left", "right", "inner", "outer", "cross",
            "on", "group", "order", "having", "limit", "union",
            "intersect", "except", "values", "set", "into",
        ):
            continue
        line = sql_stripped[: match.start()].count("\n") + 1
        ctes.append((name, line))
    return ctes


def find_final_select(sql_stripped: str) -> int | None:
    """Find the line of the final SELECT statement (after all CTEs)."""
    # Look for 'select * from final' or 'select * from enhanced' etc at end
    pattern = re.compile(
        r"^\s*select\s+\*\s+from\s+(\w+)\s*$", re.IGNORECASE | re.MULTILINE
    )
    matches = list(pattern.finditer(sql_stripped))
    if matches:
        last = matches[-1]
        return sql_stripped[: last.start()].count("\n") + 1
    return None


def has_surrogate_key_in_enhanced(sql_stripped: str, raw_sql: str) -> bool:
    """Check if the enhanced CTE contains a surrogate key generation."""
    # Check in raw SQL (before Jinja stripping) for the macro call
    # Find the enhanced CTE region
    enhanced_match = re.search(
        r"enhanced\s+as\s*\((.*?)\)(?:\s*,|\s*\n\s*\w+\s+as|\s*\n\s*select)",
        raw_sql,
        re.DOTALL | re.IGNORECASE,
    )
    if not enhanced_match:
        # Try a broader match — find enhanced as ( ... until the next CTE
        enhanced_start = re.search(
            r"enhanced\s+as\s*\(", raw_sql, re.IGNORECASE
        )
        if not enhanced_start:
            return False
        region = raw_sql[enhanced_start.end():]
    else:
        region = enhanced_match.group(1)

    return bool(
        re.search(r"generate_surrogate_key|surrogate_key|_sk\b", region, re.IGNORECASE)
    )


def has_loaded_at_in_enhanced(raw_sql: str) -> bool:
    """Check if enhanced CTE contains _loaded_at."""
    enhanced_start = re.search(r"enhanced\s+as\s*\(", raw_sql, re.IGNORECASE)
    if not enhanced_start:
        return False
    region = raw_sql[enhanced_start.end():]
    # Cut at next CTE definition
    next_cte = re.search(r"\)\s*,\s*\n\s*\w+\s+as\s*\(", region, re.IGNORECASE)
    if next_cte:
        region = region[:next_cte.start()]
    return "_loaded_at" in region


def has_explicit_column_list_in_final(sql_stripped: str) -> bool:
    """Check that the final CTE has an explicit column list (not SELECT *)."""
    final_match = re.search(
        r"final\s+as\s*\(\s*select\s+(.*?)\s+from\s+",
        sql_stripped,
        re.DOTALL | re.IGNORECASE,
    )
    if not final_match:
        return False
    select_body = final_match.group(1).strip()
    # SELECT * is not explicit
    if select_body == "*":
        return False
    # Must have at least 2 columns to be considered explicit
    return select_body.count(",") >= 1 or "\n" in select_body


def detect_source_from_path(filepath: Path) -> str | None:
    """Infer the source system from the file path."""
    parts = filepath.parts
    try:
        staging_idx = parts.index("staging")
        if staging_idx + 1 < len(parts):
            return parts[staging_idx + 1]
    except ValueError:
        pass
    return None


def has_config_block(raw_sql: str) -> bool:
    """Check if the file has a config() block."""
    return bool(re.search(r"\{\{\s*config\s*\(", raw_sql))


def check_materialized_view(config_text: str) -> bool:
    """Check that staging model is materialized as view."""
    match = re.search(r"materialized\s*=\s*['\"](\w+)['\"]", config_text)
    return match is not None and match.group(1) == "view"


def has_column_grouping_comments(sql_stripped: str) -> bool:
    """Check for column grouping comments in renamed or final CTE."""
    grouping_patterns = [
        r"--\s*identifiers",
        r"--\s*dates",
        r"--\s*descriptive",
        r"--\s*system",
        r"--\s*audit",
        r"--\s*dbt metadata",
        r"--\s*ingestion",
    ]
    count = sum(
        1 for p in grouping_patterns
        if re.search(p, sql_stripped, re.IGNORECASE)
    )
    return count >= 2  # At least 2 grouping comments


# ---------------------------------------------------------------------------
# Validation rules
# ---------------------------------------------------------------------------


def validate_file(filepath: Path) -> FileResult:
    """Run all validation rules against a single staging model file."""
    result = FileResult(path=str(filepath.relative_to(PROJECT_ROOT)))

    # Check exclusions
    for pattern in EXCLUDE_PATTERNS:
        if pattern in str(filepath):
            result.skipped = True
            result.skip_reason = f"Excluded by pattern: {pattern}"
            return result

    if not filepath.exists():
        result.violations.append(
            Violation(
                rule="FILE_EXISTS",
                severity="error",
                message=f"File not found: {filepath}",
            )
        )
        return result

    raw_sql = filepath.read_text(encoding="utf-8")
    sql_stripped = strip_jinja(raw_sql)
    source_name = detect_source_from_path(filepath)

    # ── Rule: CONFIG_BLOCK ──────────────────────────────────────────────
    if not has_config_block(raw_sql):
        result.violations.append(
            Violation(
                rule="CONFIG_BLOCK",
                severity="error",
                message="Missing config() block.",
                remediation=(
                    "Add a config block at the top of the file:\n"
                    "  {{ config(materialized='view', "
                    f"tags=['{source_name or 'source'}', 'staging', 'formentera']) }}}}"
                ),
            )
        )
    else:
        config_text = extract_config_block(raw_sql)
        if config_text:
            # ── Rule: MATERIALIZED_VIEW ─────────────────────────────────
            if not check_materialized_view(config_text):
                result.violations.append(
                    Violation(
                        rule="MATERIALIZED_VIEW",
                        severity="error",
                        message="Staging model must be materialized as 'view'.",
                        remediation="Set materialized='view' in config().",
                    )
                )

            # ── Rule: TAGS ──────────────────────────────────────────────
            tags = extract_tags(config_text)
            if not tags:
                result.violations.append(
                    Violation(
                        rule="TAGS_MISSING",
                        severity="error",
                        message="No tags defined in config().",
                        remediation=(
                            f"Add tags=['{source_name or 'source'}', "
                            "'staging', 'formentera'] to config()."
                        ),
                    )
                )
            else:
                if "staging" not in tags:
                    result.violations.append(
                        Violation(
                            rule="TAGS_STAGING",
                            severity="error",
                            message=f"Tags {tags} missing 'staging'.",
                            remediation="Include 'staging' in the tags list.",
                        )
                    )
                if len(tags) >= 3 and tags[2] != CANONICAL_TAG_THIRD:
                    result.violations.append(
                        Violation(
                            rule="TAGS_THIRD",
                            severity="warning",
                            message=(
                                f"Third tag is '{tags[2]}', expected "
                                f"'{CANONICAL_TAG_THIRD}'. "
                                f"Current tags: {tags}"
                            ),
                            remediation=(
                                f"Use ['{source_name or tags[0]}', 'staging', "
                                f"'{CANONICAL_TAG_THIRD}'] for consistency."
                            ),
                        )
                    )
                if source_name and len(tags) >= 1 and tags[0] != source_name:
                    expected = SOURCE_TAG_MAP.get(source_name, source_name)
                    if tags[0] != expected:
                        result.violations.append(
                            Violation(
                                rule="TAGS_SOURCE",
                                severity="warning",
                                message=(
                                    f"First tag is '{tags[0]}', expected "
                                    f"'{expected}' based on directory."
                                ),
                                remediation=(
                                    f"Use '{expected}' as the first tag to "
                                    "match the source directory name."
                                ),
                            )
                        )

    # ── Rule: CTE_PATTERN ───────────────────────────────────────────────
    ctes = find_cte_names(sql_stripped)
    cte_names = [name for name, _ in ctes]

    if not ctes:
        result.violations.append(
            Violation(
                rule="CTE_PATTERN",
                severity="error",
                message="No CTEs found. Staging models must use the 5-CTE pattern.",
                remediation=(
                    "Structure the model with: source, renamed, filtered, "
                    "enhanced, final. See docs/conventions/staging.md."
                ),
            )
        )
    else:
        for required_cte in REQUIRED_CTES:
            if required_cte not in cte_names:
                result.violations.append(
                    Violation(
                        rule=f"CTE_MISSING_{required_cte.upper()}",
                        severity="error",
                        message=f"Missing required CTE: '{required_cte}'.",
                        remediation=_cte_remediation(required_cte),
                    )
                )

        # Check CTE order (only for CTEs that exist)
        present_required = [c for c in REQUIRED_CTES if c in cte_names]
        actual_order = [c for c in cte_names if c in REQUIRED_CTES]
        if present_required != actual_order:
            result.violations.append(
                Violation(
                    rule="CTE_ORDER",
                    severity="error",
                    message=(
                        f"CTEs are out of order. Found: {actual_order}, "
                        f"expected: {present_required}."
                    ),
                    remediation=(
                        "Reorder CTEs to: source → renamed → filtered → "
                        "enhanced → final."
                    ),
                )
            )

    # ── Rule: SURROGATE_KEY ─────────────────────────────────────────────
    if "enhanced" in cte_names:
        if not has_surrogate_key_in_enhanced(sql_stripped, raw_sql):
            result.violations.append(
                Violation(
                    rule="SURROGATE_KEY",
                    severity="warning",
                    message="No surrogate key found in 'enhanced' CTE.",
                    remediation=(
                        "Add a surrogate key in enhanced:\n"
                        "  {{ dbt_utils.generate_surrogate_key(['primary_key']) }}"
                        " as entity_sk"
                    ),
                )
            )

    # ── Rule: LOADED_AT ─────────────────────────────────────────────────
    if "enhanced" in cte_names:
        if not has_loaded_at_in_enhanced(raw_sql):
            result.violations.append(
                Violation(
                    rule="LOADED_AT",
                    severity="warning",
                    message="No '_loaded_at' timestamp in 'enhanced' CTE.",
                    remediation=(
                        "Add to the enhanced CTE:\n"
                        "  current_timestamp() as _loaded_at"
                    ),
                )
            )

    # ── Rule: FINAL_EXPLICIT_COLUMNS ────────────────────────────────────
    if "final" in cte_names:
        if not has_explicit_column_list_in_final(sql_stripped):
            result.violations.append(
                Violation(
                    rule="FINAL_EXPLICIT_COLUMNS",
                    severity="error",
                    message=(
                        "'final' CTE uses SELECT * or has no explicit column list."
                    ),
                    remediation=(
                        "Replace SELECT * in final with an explicit, "
                        "logically grouped column list. This defines the "
                        "model's output contract. See docs/conventions/staging.md (final CTE)."
                    ),
                )
            )

    # ── Rule: COLUMN_GROUPING_COMMENTS ──────────────────────────────────
    if "renamed" in cte_names or "final" in cte_names:
        if not has_column_grouping_comments(sql_stripped):
            result.violations.append(
                Violation(
                    rule="COLUMN_GROUPING",
                    severity="warning",
                    message="Missing column grouping comments (-- identifiers, -- dates, etc.).",
                    remediation=(
                        "Add grouping comments in renamed and final CTEs:\n"
                        "  -- identifiers\n"
                        "  -- dates\n"
                        "  -- descriptive fields\n"
                        "  -- system / audit\n"
                        "  -- dbt metadata"
                    ),
                )
            )

    # ── Rule: FINAL_SELECT ──────────────────────────────────────────────
    if "final" in cte_names:
        final_select_line = find_final_select(sql_stripped)
        if final_select_line is None:
            result.violations.append(
                Violation(
                    rule="FINAL_SELECT",
                    severity="warning",
                    message="Model should end with 'select * from final'.",
                    remediation="Add 'select * from final' as the last line.",
                )
            )

    return result


def _cte_remediation(cte_name: str) -> str:
    """Return remediation guidance for a missing CTE."""
    guidance = {
        "source": (
            "Add a 'source' CTE that pulls from {{ source() }} with "
            "deduplication if needed. See docs/conventions/staging.md (source CTE)."
        ),
        "renamed": (
            "Add a 'renamed' CTE for column renaming, type casting, and "
            "trimming. No filtering or logic. See docs/conventions/staging.md (renamed CTE)."
        ),
        "filtered": (
            "Add a 'filtered' CTE to remove soft deletes and null PKs. "
            "Fivetran: coalesce(_fivetran_deleted, false) = false. "
            "Estuary: _operation_type != 'd'. See docs/conventions/staging.md (filtered CTE)."
        ),
        "enhanced": (
            "Add an 'enhanced' CTE with surrogate key, computed flags, "
            "and _loaded_at. Keep it light. See docs/conventions/staging.md (enhanced CTE)."
        ),
        "final": (
            "Add a 'final' CTE with an explicit column list grouped by "
            "category. This is the output contract. See docs/conventions/staging.md (final CTE)."
        ),
    }
    return guidance.get(cte_name, f"Add the '{cte_name}' CTE per docs/conventions/staging.md.")


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------


def find_staging_models(paths: list[str] | None = None) -> list[Path]:
    """Find staging model SQL files to validate."""
    if paths:
        resolved = []
        for p in paths:
            path = Path(p)
            if not path.is_absolute():
                path = PROJECT_ROOT / path
            if path.is_dir():
                resolved.extend(sorted(path.rglob("*.sql")))
            elif path.is_file():
                resolved.append(path)
            else:
                print(f"Warning: path not found: {p}", file=sys.stderr)
        return resolved

    # Default: all staging SQL files
    return sorted(STAGING_DIR.rglob("*.sql"))


def find_changed_files() -> list[Path]:
    """Find staged/modified SQL files using git diff against main."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "origin/main", "--", "models/operations/staging/"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        if result.returncode != 0:
            # Fallback: diff against HEAD
            result = subprocess.run(
                ["git", "diff", "--name-only", "HEAD", "--", "models/operations/staging/"],
                capture_output=True,
                text=True,
                cwd=PROJECT_ROOT,
            )
        files = [
            PROJECT_ROOT / f.strip()
            for f in result.stdout.strip().split("\n")
            if f.strip().endswith(".sql")
        ]
        return [f for f in files if f.exists()]
    except FileNotFoundError:
        print("Warning: git not found, falling back to all files", file=sys.stderr)
        return find_staging_models()


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def format_text(results: list[FileResult], verbose: bool = True) -> str:
    """Format results as human/agent-readable text."""
    lines = []
    total_errors = 0
    total_warnings = 0
    total_passed = 0
    total_skipped = 0

    for r in results:
        if r.skipped:
            total_skipped += 1
            continue

        if r.passed and not r.violations:
            total_passed += 1
            continue

        total_errors += r.error_count
        total_warnings += r.warning_count

        if r.passed and not verbose:
            total_passed += 1
            continue

        if r.violations:
            lines.append(f"\n{'FAIL' if not r.passed else 'WARN'}: {r.path}")
            for v in r.violations:
                icon = "✗" if v.severity == "error" else "⚠"
                loc = f" (line {v.line})" if v.line else ""
                lines.append(f"  {icon} [{v.rule}]{loc} {v.message}")
                if v.remediation and verbose:
                    for rem_line in v.remediation.split("\n"):
                        lines.append(f"    → {rem_line}")
        else:
            total_passed += 1

    # Summary
    total_checked = len(results) - total_skipped
    lines.insert(
        0,
        (
            f"Staging Validator: {total_checked} checked, "
            f"{total_passed} passed, {total_errors} errors, "
            f"{total_warnings} warnings, {total_skipped} skipped"
        ),
    )

    if total_errors == 0:
        lines.append(f"\n✓ All {total_checked} checked models pass.")
    else:
        lines.append(
            f"\n✗ {total_errors} error(s) across "
            f"{sum(1 for r in results if not r.passed and not r.skipped)} file(s). "
            "Fix errors before merging."
        )

    return "\n".join(lines)


def format_json(results: list[FileResult]) -> str:
    """Format results as JSON for programmatic consumption."""
    output = {
        "summary": {
            "total": len(results),
            "passed": sum(1 for r in results if r.passed and not r.skipped),
            "failed": sum(1 for r in results if not r.passed and not r.skipped),
            "skipped": sum(1 for r in results if r.skipped),
            "errors": sum(r.error_count for r in results),
            "warnings": sum(r.warning_count for r in results),
        },
        "files": [
            {
                "path": r.path,
                "passed": r.passed,
                "skipped": r.skipped,
                "skip_reason": r.skip_reason or None,
                "violations": [
                    {
                        "rule": v.rule,
                        "severity": v.severity,
                        "message": v.message,
                        "line": v.line,
                        "remediation": v.remediation or None,
                    }
                    for v in r.violations
                ],
            }
            for r in results
            if not r.passed or r.violations
        ],
    }
    return json.dumps(output, indent=2)


def format_summary(results: list[FileResult]) -> str:
    """Compact summary grouped by rule."""
    from collections import Counter

    rule_counts: Counter[str] = Counter()
    for r in results:
        for v in r.violations:
            rule_counts[v.rule] += 1

    total = len(results)
    skipped = sum(1 for r in results if r.skipped)
    checked = total - skipped
    passed = sum(1 for r in results if r.passed and not r.skipped)
    failed = checked - passed

    lines = [
        f"Staging Validator Summary: {checked} models checked",
        f"  ✓ {passed} pass  |  ✗ {failed} fail  |  ⊘ {skipped} skipped",
        "",
        "Violations by rule:",
    ]

    for rule, count in rule_counts.most_common():
        lines.append(f"  {count:>4}  {rule}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate dbt staging models against docs/conventions/staging.md."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files or directories to validate (default: all staging models)",
    )
    parser.add_argument(
        "--changed",
        action="store_true",
        help="Only validate files changed vs origin/main",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json", "summary"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--no-remediation",
        action="store_true",
        help="Suppress remediation guidance in text output",
    )

    args = parser.parse_args()

    # Discover files
    if args.changed:
        files = find_changed_files()
        if not files:
            print("No changed staging models found.")
            return 0
    elif args.paths:
        files = find_staging_models(args.paths)
    else:
        files = find_staging_models()

    if not files:
        print("No staging models found to validate.", file=sys.stderr)
        return 2

    # Validate
    results = [validate_file(f) for f in files]

    # Output
    if args.format == "json":
        print(format_json(results))
    elif args.format == "summary":
        print(format_summary(results))
    else:
        print(format_text(results, verbose=not args.no_remediation))

    # Exit code
    has_errors = any(not r.passed for r in results if not r.skipped)
    return 1 if has_errors else 0


if __name__ == "__main__":
    sys.exit(main())
