# Edge-to-Cloud Demo

Demonstrates the full TemporalParquet pipeline:

```
MobilityDuck (edge ingest)
        │  asBinary(traj) → BYTE_ARRAY
        ▼
TemporalParquet shard
  (edge_to_cloud_demo.parquet)
        │  tgeompointFromBinary(traj) → hex-WKB STRING
        ▼
MobilitySpark (cloud analytics)
  Query A: length + maxSpeed per vessel
  Query B: vessels inside Copenhagen bounding box
  Query C: trip duration per vessel
```

## Quick start

### Step 1 — generate the Parquet shard with MobilityDuck

```bash
cd <MobilityDuck-repo>
TZ=UTC ./build/release/duckdb :memory: \
    -f examples/quickstart/quickstart.sql
cp edge_to_cloud_demo.parquet <MobilitySpark-repo>/edge-to-cloud/
```

### Step 2 — run MobilitySpark analytics

```bash
cd <MobilitySpark-repo>
mvn package -DskipTests
cd edge-to-cloud
./run_pipeline.sh "" ../target/*-spark.jar
```

Or run both steps with a single command:

```bash
./run_pipeline.sh <MobilityDuck-repo> ../target/*-spark.jar
```

## What the queries do

| Query | Description | MEOS UDFs used |
|-------|-------------|----------------|
| A | Total path length + peak speed per vessel | `length`, `maxSpeed` |
| B | Vessels that entered lon 11.5–13.5, lat 55.0–56.5 (Øresund) | `eIntersects` |
| C | Trip duration per vessel | `duration` |

The same SQL queries run unchanged on MobilityDB (`quickstart_mobilitydb.sql`) and
MobilityDuck (`quickstart.sql`) — the portable named-function dialect from RFC #861.

## TemporalParquet encoding

MobilityDuck writes temporal columns as `BYTE_ARRAY` (binary MEOS-WKB) via
`asBinary()`.  MobilitySpark decodes them with `tgeompointFromBinary(col)`,
which converts the binary to hex and calls `temporal_from_hexwkb` — the same
MEOS deserializer used by all other UDFs.
