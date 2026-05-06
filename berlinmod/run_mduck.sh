#!/usr/bin/env bash
# BerlinMOD portable SQL — MobilityDuck/DuckDB comparison runner
#
# Loads data, runs Q1/Q3/Q4/Q5/Q6 + QRT, compares all against expected CSV.
# Q3 and QRT use asHexWKB() for binary return — byte-for-byte comparable
# across MobilityDB, MobilityDuck, and MobilitySpark.
#
# Usage (from the repository root):
#   ./berlinmod/run_mduck.sh [duckdb-binary]
#
# Auto-detects local vs community MobilityDuck build:
#   Local build : binary is next to extension/mobilityduck/ directory
#                 → LOAD mobilityduck;
#   Community   : extension not found locally
#                 → INSTALL mobilitydb FROM community; LOAD mobilitydb;

set -euo pipefail

DUCKDB="${1:-duckdb}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
EXPECTED="${SCRIPT_DIR}/expected"
TMP=$(mktemp -d)

trap 'rm -rf "$TMP"' EXIT

# Detect local vs community extension
DUCKDB_ABS="$(command -v "$DUCKDB" 2>/dev/null || echo "$DUCKDB")"
DUCKDB_DIR="$(cd "$(dirname "$DUCKDB_ABS")" && pwd 2>/dev/null || true)"
if [ -d "${DUCKDB_DIR}/extension/mobilityduck" ]; then
    MOBILITY_LOAD="LOAD mobilityduck;"
else
    MOBILITY_LOAD="INSTALL mobilitydb FROM community; LOAD mobilitydb;"
fi

# Strip trailing semicolon from query to safely embed in COPY (...)
strip_semi() { grep -v '^--' "$1" | tr '\n' ' ' | sed 's/;[[:space:]]*$//'; }

run_duckdb() {
  "$DUCKDB" :memory: "$@"
}

# run_query LABEL SQLFILE
run_query() {
  local label="$1"
  local qfile="$2"
  local outfile="${TMP}/${label}.csv"
  echo "--- Running ${label} ---"
  run_duckdb \
    -s "${MOBILITY_LOAD}" \
    -s "$(cat "${SCRIPT_DIR}/load_mduck.sql")" \
    -s "COPY ($(strip_semi "$qfile")) TO '${outfile}' (HEADER, DELIMITER ',')"
  echo "  $(( $(wc -l < "$outfile") - 1 )) data rows"
}

compare() {
  local label="$1"
  local got="${TMP}/${label}.csv"
  local exp="${EXPECTED}/${label}.csv"
  if diff -u "$exp" "$got" > "${TMP}/diff_${label}.txt" 2>&1; then
    echo "[PASS] ${label}"
  else
    echo "[FAIL] ${label}"
    cat "${TMP}/diff_${label}.txt"
    FAILURES=$((FAILURES + 1))
  fi
}

cd "$REPO_ROOT"

echo ""
echo "=== Running Q1/Q2/Q3/Q4/Q5/Q6/Q7/Q8 + QRT ==="
run_query q01 "${SCRIPT_DIR}/q01.sql"
run_query q02 "${SCRIPT_DIR}/q02.sql"
run_query q03 "${SCRIPT_DIR}/q03.sql"
run_query q04 "${SCRIPT_DIR}/q04.sql"
run_query q05 "${SCRIPT_DIR}/q05.sql"
run_query q06 "${SCRIPT_DIR}/q06.sql"
run_query q07 "${SCRIPT_DIR}/q07.sql"
run_query q08 "${SCRIPT_DIR}/q08.sql"
run_query qrt "${SCRIPT_DIR}/qrt.sql"

echo ""
echo "=== Comparing against expected output ==="
FAILURES=0
compare q01
compare q02
compare q03
compare q04
compare q05
compare q06
compare q07
compare q08
compare qrt

echo ""
if [ "$FAILURES" -eq 0 ]; then
  echo "ALL PASS — MobilityDuck results match expected output."
else
  echo "${FAILURES} FAILURE(S) — see diffs above."
  exit 1
fi
