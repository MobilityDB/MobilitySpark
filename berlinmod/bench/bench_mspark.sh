#!/usr/bin/env bash
# BerlinMOD timing runner — MobilitySpark / Apache Spark
#
# Builds the fat JAR if necessary, then runs BerlinMODBench in a single
# Spark session (avoiding JVM startup overhead per query).  BerlinMODBench
# writes a JSON results file directly.
#
# Usage:
#   bench_mspark.sh [options]
#
# Options:
#   --spark-submit PATH  Path to spark-submit binary (default: spark-submit from PATH)
#   --data   DIR         Directory containing the shared CSV files
#   --runs   N           Timed runs per query  (default: 3)
#   --quick              Run each query once (--runs 1); useful for crash-safety checks
#   --queries RANGE      Page-range query selector: "3", "2-5", "q02-q05", "qrt", "q04,qrt"
#                        Default: all queries in canonical order
#   --output FILE        Path to write results JSON (default: results/mspark.json)
#   --jar    PATH        Pre-built fat JAR (skip mvn build)
#
# Requirements:
#   spark-submit on PATH (or --spark-submit); Java 11/17/21; Maven for building.
#   Run setup/install_spark.sh if spark-submit is missing.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BERLINMOD_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ── defaults ──────────────────────────────────────────────────────────────────
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
DATADIR="${BERLINMOD_DIR}/data"
RUNS=3
OUTPUT="${SCRIPT_DIR}/results/mspark.json"
QUERIES=""
JAR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --spark-submit) SPARK_SUBMIT="$2"; shift 2 ;;
    --data)         DATADIR="$2";      shift 2 ;;
    --runs)         RUNS="$2";         shift 2 ;;
    --quick)        RUNS=1;            shift   ;;
    --queries)      QUERIES="$2";      shift 2 ;;
    --output)       OUTPUT="$2";       shift 2 ;;
    --jar)          JAR="$2";          shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── verify spark-submit ───────────────────────────────────────────────────────
if ! command -v "$SPARK_SUBMIT" >/dev/null 2>&1; then
  echo "ERROR: spark-submit not found."
  echo "  Run: ${REPO_ROOT}/setup/install_spark.sh"
  echo "  or pass: --spark-submit /opt/spark/bin/spark-submit"
  exit 1
fi

# ── build fat JAR if needed ───────────────────────────────────────────────────
if [[ -z "$JAR" ]]; then
  JAR=$(ls "${REPO_ROOT}/target/"*-spark.jar 2>/dev/null | head -1 || true)
fi

if [[ -z "$JAR" ]]; then
  echo "=== No fat JAR found — building with mvn package ==="
  if ! command -v mvn >/dev/null 2>&1; then
    echo "ERROR: mvn not found.  Install Maven:"
    echo "  sudo apt-get install -y maven"
    exit 1
  fi
  (cd "$REPO_ROOT" && mvn package -DskipTests -q)
  JAR=$(ls "${REPO_ROOT}/target/"*-spark.jar | head -1)
fi
echo "=== Using JAR: $JAR ==="

LIBMEOS_DIR="${LIBMEOS_DIR:-/usr/local/lib}"

mkdir -p "$(dirname "$OUTPUT")"

# Suppress core dumps: a JVM crash produces a 3-5 GB core file that can OOM WSL2.
ulimit -c 0

QUERIES_MSG="${QUERIES:-all}"
echo "=== Running BerlinMODBench (${RUNS} runs/query, queries=${QUERIES_MSG}) on MobilitySpark ==="
"$SPARK_SUBMIT" \
  --class org.mobilitydb.spark.demo.BerlinMODBench \
  --master "local[2]" \
  --conf "spark.driver.extraJavaOptions=-Djava.library.path=${LIBMEOS_DIR} -Dlog4j.logger.org.apache=WARN" \
  "$JAR" \
  "$DATADIR" \
  "$OUTPUT" \
  "$RUNS" \
  ${QUERIES:+"$QUERIES"}

echo "=== Results written to ${OUTPUT} ==="
