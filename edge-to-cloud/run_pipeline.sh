#!/usr/bin/env bash
# run_pipeline.sh — edge-to-cloud demo: MobilityDuck → TemporalParquet → MobilitySpark
#
# Usage:
#   ./run_pipeline.sh <path-to-MobilityDuck-repo> [<path-to-spark-jar>]
#
# The script runs two steps:
#   1. MobilityDuck generates the TemporalParquet shard (edge ingest)
#   2. MobilitySpark reads the shard and runs analytics queries (cloud)
#
# If you already have edge_to_cloud_demo.parquet in the current directory,
# skip step 1 by omitting the MobilityDuck repo path:
#   ./run_pipeline.sh "" path/to/mobilityspark.jar
#
# Requirements:
#   - MobilityDuck built at <MDUCK_ROOT>/build/release/duckdb
#   - spark-submit on PATH, or set SPARK_SUBMIT env var
#   - MobilitySpark fat jar built (mvn package -DskipTests from the repo root)

set -euo pipefail

MDUCK_ROOT="${1:-}"
JAR="${2:-$(ls "$(dirname "$0")/../target/"*-spark.jar 2>/dev/null | head -1)}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"

if [[ -z "$JAR" ]]; then
    echo "ERROR: no fat jar found. Build with: mvn package -DskipTests" >&2
    exit 1
fi

PARQUET="edge_to_cloud_demo.parquet"

if [[ -n "$MDUCK_ROOT" ]]; then
    echo "=== Step 1: MobilityDuck — generate TemporalParquet shard ==="
    (
        cd "$MDUCK_ROOT"
        TZ=UTC ./build/release/duckdb :memory: \
            -f examples/quickstart/quickstart.sql
    )
    cp "$MDUCK_ROOT/$PARQUET" .
    echo "Parquet shard written: $PARQUET"
else
    if [[ ! -f "$PARQUET" ]]; then
        echo "ERROR: $PARQUET not found. Run step 1 or provide a MobilityDuck repo path." >&2
        exit 1
    fi
    echo "=== Step 1: skipped — using existing $PARQUET ==="
fi

echo ""
echo "=== Step 2: MobilitySpark — cloud analytics on Parquet shard ==="
"$SPARK_SUBMIT" \
    --class org.mobilitydb.spark.examples.N02AISData \
    "$JAR" \
    "$PARQUET"
