#!/usr/bin/env bash
#
# check.sh — Single-command feedback loop for dbt model validation.
#
# Runs the full validation pipeline in order:
#   1. dbt parse (syntax validation)
#   2. SQLFluff lint (formatting)
#   3. Structural validation (5-CTE pattern, tags, conventions)
#   4. dbt build (optional, if --build flag passed)
#   5. dbt show (optional, if --show flag passed)
#
# Usage:
#   ./scripts/check.sh                              # Validate all staging models
#   ./scripts/check.sh models/operations/staging/oda/stg_oda__gl.sql  # Specific file
#   ./scripts/check.sh --changed                    # Only changed files vs main
#   ./scripts/check.sh --build models/path/to.sql   # Full pipeline including build
#   ./scripts/check.sh --show models/path/to.sql    # Include dbt show preview
#
# Exit codes:
#   0 — all checks pass
#   1 — one or more checks failed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
DO_BUILD=false
DO_SHOW=false
CHANGED_ONLY=false
TARGETS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build)   DO_BUILD=true; shift ;;
        --show)    DO_SHOW=true; shift ;;
        --changed) CHANGED_ONLY=true; shift ;;
        -h|--help)
            head -18 "$0" | tail -16
            exit 0
            ;;
        *)         TARGETS+=("$1"); shift ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
PASS=0
FAIL=0
STEPS=()

step_pass() {
    local name="$1"
    PASS=$((PASS + 1))
    STEPS+=("✓ $name")
    echo "  ✓ $name"
}

step_fail() {
    local name="$1"
    FAIL=$((FAIL + 1))
    STEPS+=("✗ $name")
    echo "  ✗ $name"
}

# Resolve targets for dbt commands (extract model names from paths)
dbt_select_from_paths() {
    local selects=()
    for t in "${TARGETS[@]}"; do
        # Extract model name from path: models/.../stg_foo__bar.sql -> stg_foo__bar
        local model_name
        model_name="$(basename "$t" .sql)"
        selects+=("$model_name")
    done
    echo "${selects[*]}"
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  dbt Harness Check"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ---------------------------------------------------------------------------
# Step 1: dbt parse
# ---------------------------------------------------------------------------
echo "Step 1/5: dbt parse --warn-error"
if dbt parse --warn-error --quiet 2>&1 | tail -3; then
    step_pass "dbt parse"
else
    step_fail "dbt parse (syntax errors found)"
    echo ""
    echo "Fix parse errors before continuing. Run: dbt parse --warn-error"
    exit 1
fi
echo ""

# ---------------------------------------------------------------------------
# Step 2: SQLFluff lint
# ---------------------------------------------------------------------------
echo "Step 2/5: SQLFluff lint"
LINT_TARGETS=()
if [[ ${#TARGETS[@]} -gt 0 ]]; then
    LINT_TARGETS=("${TARGETS[@]}")
elif [[ "$CHANGED_ONLY" == true ]]; then
    mapfile -t LINT_TARGETS < <(git diff --name-only origin/main -- 'models/operations/staging/*.sql' 2>/dev/null || true)
fi

if [[ ${#LINT_TARGETS[@]} -gt 0 ]]; then
    if sqlfluff lint "${LINT_TARGETS[@]}" --format human 2>&1 | tail -5; then
        step_pass "SQLFluff lint"
    else
        step_fail "SQLFluff lint (formatting violations)"
    fi
else
    echo "  (skipped — no specific targets, use --changed or pass paths)"
    step_pass "SQLFluff lint (skipped)"
fi
echo ""

# ---------------------------------------------------------------------------
# Step 3: Structural validation
# ---------------------------------------------------------------------------
echo "Step 3/5: Structural validation (5-CTE pattern, tags, conventions)"
VALIDATE_ARGS=()
if [[ "$CHANGED_ONLY" == true ]]; then
    VALIDATE_ARGS+=("--changed")
elif [[ ${#TARGETS[@]} -gt 0 ]]; then
    VALIDATE_ARGS+=("${TARGETS[@]}")
fi

if python3 scripts/validate_staging.py "${VALIDATE_ARGS[@]}"; then
    step_pass "Structural validation"
else
    step_fail "Structural validation (convention violations)"
fi
echo ""

# ---------------------------------------------------------------------------
# Step 4: dbt build (optional)
# ---------------------------------------------------------------------------
if [[ "$DO_BUILD" == true ]]; then
    echo "Step 4/5: dbt build"
    if [[ ${#TARGETS[@]} -gt 0 ]]; then
        SELECT=$(dbt_select_from_paths)
        if dbt build --select $SELECT 2>&1 | tail -10; then
            step_pass "dbt build"
        else
            step_fail "dbt build"
        fi
    else
        echo "  (skipped — specify target files to build)"
        step_pass "dbt build (skipped)"
    fi
else
    echo "Step 4/5: dbt build (skipped — pass --build to enable)"
fi
echo ""

# ---------------------------------------------------------------------------
# Step 5: dbt show (optional)
# ---------------------------------------------------------------------------
if [[ "$DO_SHOW" == true ]]; then
    echo "Step 5/5: dbt show (data preview)"
    if [[ ${#TARGETS[@]} -gt 0 ]]; then
        SELECT=$(dbt_select_from_paths)
        for model in $SELECT; do
            echo "  --- $model ---"
            dbt show --select "$model" --limit 5 2>&1 | tail -10
        done
        step_pass "dbt show"
    else
        echo "  (skipped — specify target files to preview)"
        step_pass "dbt show (skipped)"
    fi
else
    echo "Step 5/5: dbt show (skipped — pass --show to enable)"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results: $PASS passed, $FAIL failed"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for s in "${STEPS[@]}"; do
    echo "  $s"
done
echo ""

if [[ $FAIL -gt 0 ]]; then
    echo "✗ Fix the above failures before opening a PR."
    exit 1
else
    echo "✓ All checks passed."
    exit 0
fi
