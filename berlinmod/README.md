# BerlinMOD Portable SQL — Cross-Platform Verification

This directory contains BerlinMOD benchmark queries in the **RFC #861 portable
dialect** — using named functions only, no MobilityDB-specific operator symbols.
The same SQL files run unchanged on all three platforms.

| Platform | Engine | Extension |
|---|---|---|
| [MobilityDB](https://github.com/MobilityDB/MobilityDB) | PostgreSQL | `CREATE EXTENSION mobilitydb` |
| [MobilityDuck](https://github.com/MobilityDB/MobilityDuck) | DuckDB | `LOAD mobilitydb` (community) |
| [MobilitySpark](https://github.com/MobilityDB/MobilitySpark) | Apache Spark | `MobilitySparkSession.create(spark)` |

---

## Schema

All three platforms use the same schema:

```
Vehicles      (vehId INT, licence TEXT, type TEXT, model TEXT)
Trips         (tripId INT, vehId INT, trip TEXT)   -- tgeompoint hex-WKB
QueryLicences (licenceId INT, licence TEXT)
QueryInstants (instantId INT, instant TIMESTAMPTZ)
QueryPoints   (pointId INT, geom TEXT)             -- geometry WKT, SRID 0
QueryRegions  (regionId INT, geom TEXT)            -- polygon WKT, SRID 0
QueryPeriods  (periodId INT, period TEXT)          -- tstzspan literal
```

**Storage conventions:**
- `tgeompoint` values → hex-WKB STRING (`temporal_as_hexwkb` / `temporal_from_hexwkb`)
- `geometry` / polygon values → WKT STRING, parsed via `geo_from_text` / `ST_GeomFromText`
- `tstzspan` values → literal STRING `"[t1,t2]"`, cast to `tstzspan` by each platform

Storing temporal/geometry values as portable text keeps the CSV files
human-readable across all platforms without requiring platform-specific binary
encoding.

---

## Queries

| File | Query | Temporal operations |
|------|-------|---------------------|
| `q01.sql` | Vehicle models for query licences | none (baseline relational join) |
| `q02.sql` | Licence plates of vehicles that ever entered a query region | `eIntersects(tgeompoint, geometry)` |
| `q03.sql` | Position of query-licence vehicles at each query instant | `atTime(tgeompoint, timestamptz)` |
| `q04.sql` | Vehicles that ever passed a query point | `eIntersects(tgeompoint, geometry)` |
| `q05.sql` | Min nearest-approach distance between query-licence pairs | `nearestApproachDistance(tgeompoint, tgeompoint)` |
| `q06.sql` | Truck pairs within 10 m | `eDwithin(tgeompoint, tgeompoint, float)` |
| `q07.sql` | Trip portions of query-licence vehicles during each query period | `atTime(tgeompoint, tstzspan)` |
| `q08.sql` | Trajectory geometry of each trip | `trajectory(tgeompoint)` |
| `qrt.sql` | Binary roundtrip — all trips serialised as hex-WKB | `asHexWKB(tgeompoint)` |

`atTime` is polymorphic: pass a `TIMESTAMPTZ` (Q3) or a `tstzspan` literal (Q7)
and the platform routes to the appropriate MEOS function.

---

## Shared dataset

`data/` contains CSV files that all three platforms load:

| File | Description |
|------|-------------|
| `data/vehicles.csv` | 5 vehicles (3 passenger, 2 truck) |
| `data/trips.csv` | 5 trips, each as a tgeompoint WKT sequence (SRID 0) |
| `data/query_licences.csv` | 2 query licences |
| `data/query_instants.csv` | 1 query instant |
| `data/query_points.csv` | 2 query points (WKT) |
| `data/query_regions.csv` | 1 query polygon region (WKT) |
| `data/query_periods.csv` | 1 query period (tstzspan literal) |

**Dataset design (SRID 0, planar):**

```
trip1 (B-AA 100): (0,0) → (100,0)  y = 0
trip2 (B-BB 200): (0,5) → (100,5)  y = 5
trip3 (B-CC 300): (0,3) → (100,3)  y = 3  (truck)
trip4 (B-DD 400): (0,4) → (100,4)  y = 4  (truck, 1 unit from trip3)
trip5 (B-EE 500): far away          (not near others)

QueryPoints:  POINT(50 0), POINT(50 5)
QueryRegions: POLYGON((40 -1,60 -1,60 6,40 6,40 -1))  covers x=40..60, y=-1..6
QueryPeriods: [2020-01-01 00:02:00+00, 2020-01-01 00:08:00+00]
All trips active during: 2020-01-01 00:00 – 00:10 UTC
```

**Expected results (verified against MobilityDB, MobilityDuck, and MobilitySpark):**

| Query | Result |
|-------|--------|
| Q1 | B-AA 100 → Sedan ; B-CC 300 → Lorry |
| Q2 | B-AA 100, B-BB 200, B-CC 300, B-DD 400 (all 4 non-remote vehicles) |
| Q3 | 2 rows — MEOS hex-WKB of position at 00:05 UTC |
| Q4 | B-AA 100, B-BB 200 |
| Q5 | B-AA 100 ↔ B-CC 300 : 3.0 |
| Q6 | B-CC 300 ↔ B-DD 400 (trucks, distance 1.0 < 10.0) |
| Q7 | 2 rows — hex-WKB of trip portions during the query period |
| Q8 | 5 rows — WKT trajectory geometry for each trip |
| QRT | 5 rows — MEOS hex-WKB of all 5 trips (binary roundtrip) |

Expected CSV files for all queries are in `expected/`.

**Cross-platform portability design:**
- Q3 / Q7 / QRT: use `asHexWKB()` → `temporal_as_hexwkb(ptr, 0)` — byte-for-byte identical
- Q8: uses `trajectory()` → `geo_as_hexewkb(ptr, NULL)` (PostgreSQL COPY, DuckDB COPY, and MobilitySpark UDF all produce the same little-endian WKB hex)
- All other queries: boolean / integer / float / text outputs — identical across platforms

---

## Running on MobilityDB (PostgreSQL)

```bash
# Create a database and run the comparison:
createdb berlinmod_portability
./berlinmod/run_mbdb.sh berlinmod_portability
```

---

## Running on MobilityDuck (DuckDB)

```bash
# Run from the repository root:
./berlinmod/run_mduck.sh [path/to/duckdb]
```

---

## Running on MobilitySpark (Apache Spark)

```bash
./berlinmod/run_mspark.sh [spark-submit-binary]
```

Or manually:

```bash
spark-submit \
  --class org.mobilitydb.spark.demo.BerlinMODDemo \
  --master "local[*]" \
  target/mobilityspark-*-spark.jar \
  berlinmod/data \
  berlinmod/expected
```

---

## Replacing the synthetic dataset with real BerlinMOD data

The shared CSV format is produced directly by
[MobilityDB-BerlinMOD](https://github.com/MobilityDB/MobilityDB-BerlinMOD)
via `berlinmod_portability_export()`:

```sql
-- In a PostgreSQL database with generated BerlinMOD data:
\i BerlinMOD/berlinmod_export.sql
SELECT berlinmod_portability_export('/path/to/output/');
```

This writes `vehicles.csv`, `trips.csv`, `query_licences.csv`,
`query_instants.csv`, `query_points.csv`, `query_regions.csv`, and
`query_periods.csv` in exactly the schema expected by the comparison scripts.

Replace `data/*.csv` with the generated files and re-run:

```bash
./berlinmod/run_mbdb.sh berlinmod_portability   # MobilityDB
./berlinmod/run_mduck.sh                         # MobilityDuck
./berlinmod/run_mspark.sh                        # MobilitySpark
```
