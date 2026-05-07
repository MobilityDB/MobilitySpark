#!/usr/bin/env bash
# BerlinMOD portable SQL — MobilitySpark/Spark SQL comparison runner
#
# Builds the fat jar (if mvn is available), then submits BerlinMODDemo
# against the shared berlinmod/data/ CSV files and compares all results
# against the expected/ CSV files.
#
# Q3, Q7 and QRT use asHexWKB() for binary return — byte-for-byte identical
# across MobilityDB, MobilityDuck, and MobilitySpark.
#
# Usage (from the repository root):
#   ./berlinmod/run_mspark.sh [spark-submit-binary]
#
# Requirements:
#   - spark-submit on PATH (or pass explicit path as $1)
#   - Java 11/17/21, Maven (mvn) for building
#   - target/*-spark.jar must exist or mvn must be available to build it

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SPARK_SUBMIT="${1:-spark-submit}"
DATA_DIR="${SCRIPT_DIR}/data"
EXPECTED_DIR="${SCRIPT_DIR}/expected"
JAR_GLOB="${REPO_ROOT}/target/*-spark.jar"

cd "$REPO_ROOT"

# Build fat jar if not present (requires mvn)
JAR=$(ls $JAR_GLOB 2>/dev/null | head -1 || true)
if [ -z "$JAR" ]; then
  echo "=== No fat jar found — building with mvn package ==="
  if ! command -v mvn >/dev/null 2>&1; then
    echo "ERROR: mvn not found. Build the fat jar manually:"
    echo "  mvn package -DskipTests"
    echo "  ${SPARK_SUBMIT} --class org.mobilitydb.spark.demo.BerlinMODDemo \\"
    echo "      target/*-spark.jar ${DATA_DIR} ${EXPECTED_DIR}"
    exit 1
  fi
  mvn package -DskipTests -q
  JAR=$(ls $JAR_GLOB | head -1)
fi
echo "=== Using jar: $JAR ==="

# Verify spark-submit is available
if ! command -v "$SPARK_SUBMIT" >/dev/null 2>&1; then
  echo "ERROR: spark-submit not found. Install Apache Spark and ensure"
  echo "spark-submit is on PATH, or pass the path as the first argument."
  echo ""
  echo "Manual run:"
  echo "  ${SPARK_SUBMIT} --class org.mobilitydb.spark.demo.BerlinMODDemo \\"
  echo "      ${JAR} ${DATA_DIR} ${EXPECTED_DIR}"
  exit 1
fi

echo "=== Running BerlinMOD Q1-Q8 + QRT + Q9-Q17 on MobilitySpark ==="
"$SPARK_SUBMIT" \
  --class org.mobilitydb.spark.demo.BerlinMODDemo \
  --master "local[*]" \
  "$JAR" \
  "$DATA_DIR" \
  "$EXPECTED_DIR"
