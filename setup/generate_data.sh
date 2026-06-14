#!/usr/bin/env bash
# Generate BerlinMOD CSV data for the cross-platform benchmark.
#
# Uses MobilityDB-BerlinMOD's data generator (PostgreSQL-based) to produce
# the shared CSV files that bench.sh loads into all three platforms.
#
# Usage:
#   generate_data.sh [options]
#
# Options:
#   --mobilitydbbm DIR   Path to MobilityDB-BerlinMOD clone
#                        (default: auto-detect sibling of MobilitySpark)
#   --scalefactor FLOAT  BerlinMOD scale factor (default: 0.005)
#                          0.005 →  ~100 vehicles, ~10 000 trips  (~15 min total)
#                          0.05  → ~1000 vehicles, ~100 000 trips  (~2–3 hours)
#   --dbname NAME        Temporary PostgreSQL database for generation
#                        (default: berlinmod_gen; dropped when done)
#   --output DIR         Where to write the CSV files
#                        (default: berlinmod/data/ inside this repo)
#   --keep-db            Keep the PostgreSQL database after export
#   --skip-osm           Skip the osm2pgrouting / brussels_preparedata steps
#                        (use when the road network is already loaded in --dbname)
#
# Requirements:
#   psql + createdb + dropdb on PATH; MobilityDB + pgRouting installed.
#   osm2pgrouting on PATH (sudo apt-get install osm2pgrouting).
#   The script expects MobilityDB-BerlinMOD at the path given by
#   --mobilitydbbm or auto-detected.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── defaults ──────────────────────────────────────────────────────────────────
# Try to find MobilityDB-BerlinMOD next to this repo
AUTO_BM="$(cd "${REPO_ROOT}/.." && pwd)/MobilityDB-BerlinMOD"
BM_DIR="${AUTO_BM}"
SCALEFACTOR="0.005"
DBNAME="berlinmod_gen"
OUTPUT="${REPO_ROOT}/berlinmod/data"
KEEP_DB=false
SKIP_OSM=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mobilitydbbm) BM_DIR="$2";      shift 2 ;;
    --scalefactor)  SCALEFACTOR="$2"; shift 2 ;;
    --dbname)       DBNAME="$2";      shift 2 ;;
    --output)       OUTPUT="$2";      shift 2 ;;
    --keep-db)      KEEP_DB=true;     shift   ;;
    --skip-osm)     SKIP_OSM=true;    shift   ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

BM_SQL="${BM_DIR}/BerlinMOD"

if [[ ! -d "$BM_SQL" ]]; then
  echo "ERROR: MobilityDB-BerlinMOD not found at: ${BM_DIR}"
  echo ""
  echo "Clone it first:"
  echo "  git clone https://github.com/MobilityDB/MobilityDB-BerlinMOD.git \\"
  echo "    ${BM_DIR}"
  echo ""
  echo "Then re-run this script."
  exit 1
fi

# Check osm2pgrouting is present (needed for road network load)
if ! $SKIP_OSM && ! command -v osm2pgrouting >/dev/null 2>&1; then
  echo "ERROR: osm2pgrouting not found on PATH."
  echo "Install it:  sudo apt-get install osm2pgrouting"
  echo "Or skip the road network step if the database already has it:"
  echo "  $0 --skip-osm [other options]"
  exit 1
fi

echo "╔══════════════════════════════════════════════════╗"
echo "║  BerlinMOD data generator                       ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Scale factor : ${SCALEFACTOR}"
echo "Database     : ${DBNAME}"
echo "Output       : ${OUTPUT}"
echo ""

_psql() { psql -d "$DBNAME" -q "$@"; }

# ── create database ───────────────────────────────────────────────────────────
echo "=== Creating database: ${DBNAME} ==="
createdb "$DBNAME" 2>/dev/null || true
_psql -c "CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;"
_psql -c "CREATE EXTENSION IF NOT EXISTS pgRouting CASCADE;"

# ── load Brussels road network (osm2pgrouting + brussels_preparedata.sql) ─────
if ! $SKIP_OSM; then
  echo "=== Loading Brussels road network from OSM (~2 min) ==="
  OSM_FILE="${BM_SQL}/brussels.osm"
  MAP_CONFIG="${BM_SQL}/mapconfig.xml"
  if [[ ! -f "$OSM_FILE" ]]; then
    echo "ERROR: brussels.osm not found at ${OSM_FILE}"
    exit 1
  fi
  DB_USER="${USER:-$(id -un)}"
  osm2pgrouting --dbname "$DBNAME" -U "$DB_USER" \
                --file "$OSM_FILE" \
                --conf "$MAP_CONFIG" \
                --clean \
                2>&1 | grep -E "Execution (started|ended)|Elapsed"
  # osm2pgsql provides planet_osm_polygon needed by brussels_preparedata.sql
  osm2pgsql -c -H localhost -U "$DB_USER" -d "$DBNAME" "$OSM_FILE" \
                2>&1 | grep -v "^20[0-9][0-9]-"
  echo "=== Preparing road network graph ==="
  _psql -f "${BM_SQL}/brussels_preparedata.sql"
fi

# ── run the BerlinMOD data generator ─────────────────────────────────────────
echo "=== Generating data (scalefactor=${SCALEFACTOR}) — this may take several minutes ==="
# Load function definitions (the activation call is commented out in the file)
_psql -f "${BM_SQL}/berlinmod_datagenerator.sql"
# Invoke the generator with the requested scale factor
_psql -c "SELECT berlinmod_generate(scaleFactor := ${SCALEFACTOR});"

# ── export to shared CSV format ───────────────────────────────────────────────
mkdir -p "$OUTPUT"
echo ""
echo "=== Exporting to CSV: ${OUTPUT} ==="
_psql -f "${BM_SQL}/berlinmod_export.sql"
# The canonical generator (MobilityDB-BerlinMOD berlinmod_portability_export)
# writes every CSV directly — vehicles, trips (hex-EWKB), licences, instants,
# points, periods, regions — as RAW lat/lon in SRID 4326 (the canonical default).
# load.sql then builds the H3 prefilter columns (th3index / geoToH3IndexSet), which
# require EPSG:4326; nearestApproachDistance on the 4326 geodetic trips returns true
# metres. No per-tool post-processing, reprojection, or committed corpus copy.
_psql -c "SELECT berlinmod_portability_export('${OUTPUT}/', 7, 4326);"

# ── stats ─────────────────────────────────────────────────────────────────────
echo ""
echo "=== Dataset statistics ==="
_psql -c "SELECT count(*) AS vehicles FROM Vehicles;"
_psql -c "SELECT count(*) AS trips    FROM Trips;"
echo ""
wc -l "${OUTPUT}"/*.csv

# ── cleanup ───────────────────────────────────────────────────────────────────
if $KEEP_DB; then
  echo ""
  echo "Database ${DBNAME} kept (--keep-db)."
else
  echo ""
  echo "=== Dropping temporary database: ${DBNAME} ==="
  dropdb "$DBNAME"
fi

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║  Done.  CSV files are in: ${OUTPUT}"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Run the benchmark:"
echo "  ./berlinmod/bench/bench.sh --data ${OUTPUT}"
