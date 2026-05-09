#!/usr/bin/env bash
# BerlinMOD timing runner — MobilityDuck / DuckDB
#
# Loads data once into a file-based DuckDB database, runs each query RUNS times,
# and writes a JSON file suitable for report.py.
#
# Usage:
#   bench_mduck.sh [options]
#
# Options:
#   --duckdb PATH    Path to duckdb binary  (default: duckdb from PATH)
#   --data   DIR     Directory containing the shared CSV files
#   --runs   N       Timed runs per query   (default: 3)
#   --output FILE    Path to write results JSON (default: results/mduck.json)
#   --dbfile PATH    DuckDB file to use  (default: /tmp/berlinmod_bench.duckdb)
#   --no-load        Skip data loading (reuse existing dbfile)
#
# Requirements:
#   duckdb on PATH (or pass --duckdb); MobilityDuck extension loadable.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BERLINMOD_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── defaults ──────────────────────────────────────────────────────────────────
DUCKDB="${DUCKDB:-duckdb}"
DATADIR="${BERLINMOD_DIR}/data"
RUNS=3
OUTPUT="${SCRIPT_DIR}/results/mduck.json"
DBFILE="/tmp/berlinmod_bench.duckdb"
LOAD=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duckdb)  DUCKDB="$2";  shift 2 ;;
    --data)    DATADIR="$2"; shift 2 ;;
    --runs)    RUNS="$2";    shift 2 ;;
    --output)  OUTPUT="$2";  shift 2 ;;
    --dbfile)  DBFILE="$2";  shift 2 ;;
    --no-load) LOAD=false;   shift   ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Detect local vs community MobilityDuck build
DUCKDB_ABS="$(command -v "$DUCKDB" 2>/dev/null || echo "$DUCKDB")"
DUCKDB_DIR="$(cd "$(dirname "$DUCKDB_ABS")" 2>/dev/null && pwd || true)"
if [ -d "${DUCKDB_DIR}/extension/mobilityduck" ]; then
    MOBILITY_LOAD="LOAD mobilityduck;"
else
    MOBILITY_LOAD="INSTALL mobilitydb FROM community; LOAD mobilitydb;"
fi

_duck() { "$DUCKDB" "$DBFILE" -c "$1" 2>/dev/null; }
_duck_q() { "$DUCKDB" "$DBFILE" -noheader -list -c "$1" 2>/dev/null; }

# ── load data ─────────────────────────────────────────────────────────────────
if $LOAD; then
  echo "=== Loading data into: $DBFILE ==="
  rm -f "$DBFILE"
  # Strip the inline DATADIR default from load_mduck.sql before injecting the
  # script-provided full path, so our SET VARIABLE takes effect.
  LOAD_BODY="$(sed '/^SET VARIABLE DATADIR/d' "${BERLINMOD_DIR}/load_mduck.sql")"
  LOAD_SQL="${MOBILITY_LOAD} SET VARIABLE DATADIR='${DATADIR}/'; ${LOAD_BODY}"
  "$DUCKDB" "$DBFILE" -c "$LOAD_SQL"
  echo "    done."
fi

# ── version ───────────────────────────────────────────────────────────────────
MDUCK_VER=$(_duck_q "SELECT mobilityduck_version();" 2>/dev/null | head -1 || echo "unknown")
DUCK_VER=$(_duck_q  "SELECT version();" 2>/dev/null | head -1 || echo "unknown")
PLATFORM_VER="${MDUCK_VER} on DuckDB ${DUCK_VER}"

TRIP_COUNT=$(_duck_q "SELECT count(*) FROM Trips;"    || echo 0)
VEH_COUNT=$( _duck_q "SELECT count(*) FROM Vehicles;" || echo 0)

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
  QSQL=$(grep -v '^\s*--' "$QFILE" | tr '\n' ' ')
  printf "  timing %-6s: " "$Q"
  for RUN in $(seq 1 "$RUNS"); do
    T0=$(date +%s%3N)
    "$DUCKDB" "$DBFILE" -c "${MOBILITY_LOAD} SET search_path='portable,main'; ${QSQL}" \
      > /dev/null 2>&1 || true
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
            ms = int(parts[1])
            if ms > 0:   # skip negative values (WSL2 clock jitter)
                times[parts[0]].append(ms)

result = {
    "platform": "mobilityduck",
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
