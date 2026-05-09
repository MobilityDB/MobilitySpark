#!/usr/bin/env bash
# BerlinMOD timing runner — MobilityDB / PostgreSQL
#
# Loads data once, runs each query RUNS times, records wall-clock time per run,
# and writes a JSON file suitable for report.py.
#
# Usage:
#   bench_mbdb.sh [options]
#
# Options:
#   --dbname NAME    PostgreSQL database name  (default: berlinmod_bench)
#   --data   DIR     Directory containing the shared CSV files (vehicles.csv, trips.csv, …)
#   --runs   N       Timed runs per query      (default: 3)
#   --output FILE    Path to write results JSON (default: results/mbdb.json)
#   --no-load        Skip data loading (reuse existing tables in DBNAME)
#
# Requirements:
#   psql on PATH; MobilityDB installed and available for CREATE EXTENSION.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BERLINMOD_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── defaults ──────────────────────────────────────────────────────────────────
DBNAME="berlinmod_bench"
DATADIR="${BERLINMOD_DIR}/data"
RUNS=3
OUTPUT="${SCRIPT_DIR}/results/mbdb.json"
LOAD=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dbname)  DBNAME="$2";  shift 2 ;;
    --data)    DATADIR="$2"; shift 2 ;;
    --runs)    RUNS="$2";    shift 2 ;;
    --output)  OUTPUT="$2";  shift 2 ;;
    --no-load) LOAD=false;   shift   ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

_psql() { psql -d "$DBNAME" -q "$@"; }

# ── load data ─────────────────────────────────────────────────────────────────
if $LOAD; then
  echo "=== Creating database: $DBNAME ==="
  createdb "$DBNAME" 2>/dev/null || true
  LOADER=$(mktemp --suffix=.sql)
  trap 'rm -f "$LOADER"' EXIT
  sed "s|DATADIR|${DATADIR}|g" "${BERLINMOD_DIR}/load_mbdb.sql" > "$LOADER"
  echo "=== Loading data ==="
  _psql -f "$LOADER"
  echo "    done."
fi

# ── version ───────────────────────────────────────────────────────────────────
MBDB_VER=$(_psql -t -c "SELECT mobilitydb_version();" | tr -d ' \n')
PG_VER=$(_psql -t -c "SELECT version();" | awk '{print $1, $2}' | tr -d '\n')
PLATFORM_VER="${MBDB_VER} on ${PG_VER}"

TRIP_COUNT=$(_psql -t -c "SELECT count(*) FROM Trips;" | tr -d ' \n')
VEH_COUNT=$(_psql  -t -c "SELECT count(*) FROM Vehicles;" | tr -d ' \n')

echo "=== Platform: ${PLATFORM_VER} ==="
echo "=== Dataset : ${VEH_COUNT} vehicles / ${TRIP_COUNT} trips ==="
echo "=== Runs    : ${RUNS} per query ==="
echo ""

QUERIES=(q01 q02 q03 q04 q05 q06 q07 q08 qrt q09 q10 q11 q12 q13 q14 q15 q16 q17)
TIMEFILE=$(mktemp)
trap 'rm -f "$TIMEFILE"' EXIT

for Q in "${QUERIES[@]}"; do
  QFILE="${BERLINMOD_DIR}/${Q}.sql"
  [[ -f "$QFILE" ]] || { echo "  [skip] ${Q} — SQL file not found"; continue; }
  printf "  timing %-6s: " "$Q"
  for RUN in $(seq 1 "$RUNS"); do
    T0=$(date +%s%3N)
    _psql -o /dev/null -f "$QFILE" 2>/dev/null || true
    T1=$(date +%s%3N)
    ELAPSED=$((T1 - T0))
    printf "%d " "$ELAPSED"
    echo "${Q} ${ELAPSED}" >> "$TIMEFILE"
  done
  echo "ms"
done

mkdir -p "$(dirname "$OUTPUT")"

python3 - "$TIMEFILE" "$OUTPUT" "$PLATFORM_VER" "$TRIP_COUNT" "$VEH_COUNT" "$RUNS" <<'PYEOF'
import sys, json, collections, datetime

timefile, outfile, version, trips, vehicles, runs = \
    sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])

times = collections.defaultdict(list)
with open(timefile) as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) == 2:
            times[parts[0]].append(int(parts[1]))

result = {
    "platform": "mobilitydb",
    "version":  version,
    "data_vehicles": vehicles,
    "data_trips":    trips,
    "runs":          runs,
    "timestamp":     datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    "queries":       dict(times),
}
with open(outfile, "w") as f:
    json.dump(result, f, indent=2)
print(f"\nResults written to {outfile}")
PYEOF
