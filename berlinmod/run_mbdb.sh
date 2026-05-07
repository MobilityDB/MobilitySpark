#!/usr/bin/env bash
# BerlinMOD portable SQL — MobilityDB/PostgreSQL comparison runner
#
# Loads data, runs Q1/Q2/Q3/Q4/Q5/Q6/Q7/Q8 + QRT, compares all against expected CSV.
# Q3, Q7 and QRT use asHexWKB() for binary return — byte-for-byte identical
# across MobilityDB, MobilityDuck, and MobilitySpark.
#
# Usage (from any directory):
#   ./berlinmod/run_mbdb.sh [dbname]        # default: berlinmod_portability
#
# Requirements: psql on PATH, MobilityDB installable in the target database.

set -euo pipefail

DBNAME="${1:-berlinmod_portability}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATADIR="${SCRIPT_DIR}/data"
EXPECTED="${SCRIPT_DIR}/expected"
TMP=$(mktemp -d)

trap 'rm -rf "$TMP"' EXIT

_psql() { PGOPTIONS='-c search_path=portable,public' psql -d "$DBNAME" "$@"; }

# Substitute DATADIR placeholder in the loader template
LOADER="${TMP}/load_mbdb.sql"
sed "s|DATADIR|${DATADIR}|g" "${SCRIPT_DIR}/load_mbdb.sql" > "$LOADER"

echo "=== Loading data into PostgreSQL database: $DBNAME ==="
_psql -f "$LOADER"

# run_query LABEL SQLFILE [normalize_floats]
#   normalize_floats=true: append .0 to bare integers (Q5 only — PostgreSQL
#   prints float8 value 3.0 as "3"; other platforms print "3.0").
run_query() {
  local label="$1"
  local qfile="$2"
  local normalize="${3:-false}"
  local rawfile="${TMP}/${label}_raw.csv"
  local outfile="${TMP}/${label}.csv"
  local sql
  sql=$(grep -v '^\s*--' "$qfile" | tr '\n' ' ' | sed 's/;[[:space:]]*$//')
  echo "--- Running ${label} ---"
  _psql -c "\copy ($sql) TO '${rawfile}' CSV HEADER"
  if [ "$normalize" = "true" ]; then
    awk 'BEGIN{FS=OFS=","} NR>1{for(i=1;i<=NF;i++) if($i~/^-?[0-9]+$/) $i=$i".0"} 1' \
        "$rawfile" > "$outfile"
  else
    cp "$rawfile" "$outfile"
  fi
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

echo ""
echo "=== Running Q1/Q2/Q3/Q4/Q5/Q6/Q7/Q8 + QRT ==="
run_query q01 "${SCRIPT_DIR}/q01.sql"
run_query q02 "${SCRIPT_DIR}/q02.sql"
run_query q03 "${SCRIPT_DIR}/q03.sql"
run_query q04 "${SCRIPT_DIR}/q04.sql"
run_query q05 "${SCRIPT_DIR}/q05.sql" true   # float min_dist: PG prints 3, others 3.0
run_query q06 "${SCRIPT_DIR}/q06.sql"
run_query q07 "${SCRIPT_DIR}/q07.sql"
run_query q08 "${SCRIPT_DIR}/q08.sql"
run_query qrt "${SCRIPT_DIR}/qrt.sql"
run_query q09 "${SCRIPT_DIR}/q09.sql"
run_query q10 "${SCRIPT_DIR}/q10.sql"
run_query q11 "${SCRIPT_DIR}/q11.sql"
run_query q12 "${SCRIPT_DIR}/q12.sql"
run_query q13 "${SCRIPT_DIR}/q13.sql"
run_query q14 "${SCRIPT_DIR}/q14.sql"
run_query q15 "${SCRIPT_DIR}/q15.sql"
run_query q16 "${SCRIPT_DIR}/q16.sql"
run_query q17 "${SCRIPT_DIR}/q17.sql"

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
compare q09
compare q10
compare q11
compare q12
compare q13
compare q14
compare q15
compare q16
compare q17

echo ""
if [ "$FAILURES" -eq 0 ]; then
  echo "ALL PASS — MobilityDB results match expected output."
else
  echo "${FAILURES} FAILURE(S) — see diffs above."
  exit 1
fi
