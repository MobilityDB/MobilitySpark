#!/usr/bin/env bash
# regen-from-pin.sh — regenerate the MobilitySpark UDF layer from the catalog + JMEOS jar
# (per GENERATION.md). MobilitySpark is a JMEOS consumer.
#
# Usage:  tools/regen-from-pin.sh <pin>
#   env:  CATALOG   = path to meos-idl.json produced by MEOS-API run.py (required)
#         JMEOS_JAR = path to the JMEOS jar built from the same pin (required)
#
# Invoked standalone, or by MEOS-API tools/ecosystem-generate.sh (after the JMEOS jar).
set -euo pipefail
PIN="${1:?usage: regen-from-pin.sh <pin>}"
CATALOG="${CATALOG:?set CATALOG to the meos-idl.json from MEOS-API run.py}"
JMEOS_JAR="${JMEOS_JAR:?set JMEOS_JAR to the JMEOS jar built from the same pin}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

# run the in-repo generator (tools/codegen_spark_udfs.py: --catalog --jar) -> the Spark UDF layer
python3 "$HERE/tools/codegen_spark_udfs.py" --catalog "$CATALOG" --jar "$JMEOS_JAR"

# build-verify
( cd "$HERE" && mvn -q test ) || echo "WARN: MobilitySpark mvn test returned non-zero"
echo "[spark] regenerated from catalog + JMEOS jar at pin $PIN"
