MobilitySpark
=============

[MEOS (Mobility Engine, Open Source)](https://www.libmeos.org/) is a C library that enables the
manipulation of temporal and spatiotemporal data based on
[MobilityDB](https://mobilitydb.com/)'s data types and functions.

MobilitySpark is a binding for [Apache Spark](https://spark.apache.org/) built on top of MEOS
via [JMEOS](https://github.com/MobilityDB/JMEOS) (the Java binding for MEOS).

<img src="doc/images/mobilitydb-logo.svg" width="200" alt="MobilityDB Logo" />

The MobilityDB project is developed by the Computer & Decision Engineering Department of the
[Université libre de Bruxelles](https://www.ulb.be/) (ULB) under the direction of
[Prof. Esteban Zimányi](http://cs.ulb.ac.be/members/esteban/).
ULB is an OGC Associate Member and member of the OGC Moving Feature Standard Working Group
([MF-SWG](https://www.ogc.org/projects/groups/movfeatswg)).

<img src="doc/images/OGC_Associate_Member_3DR.png" width="100" alt="OGC Associate Member Logo" />

---

## 1. Requirements

- **Java 21** (OpenJDK or Temurin)
- **Apache Maven 3.8+**
- **Apache Spark 3.5** (provided at runtime; not needed to compile)
- **JMEOS 1.4** — bundled in `libs/JMEOS-1.4.jar`
  (the Java binding for MEOS 1.4; includes `libmeos.so` for Linux).
  Every MEOS symbol used by MobilitySpark is now provided by JMEOS;
  there is no supplementary JNR-FFI surface in this repository.

---

## 2. Building MobilitySpark

### Clone the repository

```sh
git clone https://github.com/MobilityDB/MobilitySpark.git
cd MobilitySpark
```

### Compile

```sh
mvn compile
```

### Package (fat jar for `spark-submit`)

```sh
mvn package -DskipTests
```

The fat jar is written to `target/mobilityspark-0.1.0-SNAPSHOT-spark.jar`.

---

## 3. Using MobilitySpark

### 3.1. Initialise MEOS and register UDFs

```java
SparkSession spark = SparkSession.builder().master("local[2]").getOrCreate();
try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
    // All UDFs are now available in Spark SQL
    spark.sql("SELECT atTime(trip, TIMESTAMP '2020-01-01 00:30:00') FROM trips").show();
}
```

### 3.2. Available UDFs

MobilitySpark covers **100% of MobilityDB's active addressable SQL surface**
(858/858 functions) — the same audit methodology runs against MobilityDuck
to keep both bindings in lockstep. See
[`docs/parity-100.md`](docs/parity-100.md) for the achievement note,
[`docs/parity-status.md`](docs/parity-status.md) for the per-section
coverage report (regenerable via `python3 scripts/parity-audit.py`),
and the comprehensive UDF inventory in [PR #5](https://github.com/MobilityDB/MobilitySpark/pull/5).

A small sample (every UDF group in MobilityDB has a Spark equivalent):

| UDF | Signature | Description |
|-----|-----------|-------------|
| `atTime` | `(STRING, TIMESTAMP) → STRING` | Restrict tgeompoint to a timestamp |
| `eIntersects` | `(STRING, STRING) → BOOLEAN` | Ever intersects a geometry |
| `nearestApproachDistance` | `(STRING, STRING) → DOUBLE` | Min distance at any common instant |
| `eDwithin` | `(STRING, STRING, DOUBLE) → BOOLEAN` | Ever within given distance |
| `spaceTimeTiles` | `(STRING, DOUBLE×3, STRING, STRING, TIMESTAMP, BOOLEAN) → ARRAY<STRING>` | Multidimensional tiling for parallel partitioning |
| `tfloatSeqSetGaps` | `(ARRAY<STRING>, STRING, DOUBLE, STRING) → STRING` | Build a tfloat sequence-set with gap detection |

All tgeompoint values are stored as **hex-WKB strings** (output of `temporal_as_hexwkb`).
Geometry values are **hex-EWKB strings** (output of `geo_as_hexewkb`).
Set / span / spanset / tbox / stbox values follow the same hex-encoding convention.

### 3.3. Portable SQL (BerlinMOD benchmark)

The SQL queries in `berlinmod/` use **named functions only** — no operator symbols — so the
same file runs unchanged on MobilityDB (PostgreSQL), MobilityDuck (DuckDB), and MobilitySpark
(Spark SQL). This is the portability contract defined in
[Discussion #861](https://github.com/MobilityDB/MobilityDB/discussions/861).

Run the demo:

```sh
spark-submit --class org.mobilitydb.spark.demo.BerlinMODDemo \
    target/mobilityspark-0.1.0-SNAPSHOT-spark.jar
```

### 3.4. Sample queries

Restrict a trip to a query instant:
```sql
SELECT atTime(trip, TIMESTAMP '2020-01-01 00:30:00+00') AS pos FROM Trips;
```

Find vehicles that ever passed a query point:
```sql
SELECT DISTINCT v.licence
FROM Vehicles v JOIN Trips t ON t.vehId = v.vehId
JOIN QueryPoints p ON eIntersects(t.trip, p.geom);
```

Minimum nearest-approach distance between vehicle pairs:
```sql
SELECT MIN(nearestApproachDistance(t1.trip, t2.trip)) AS min_dist
FROM Trips t1 JOIN Trips t2 ON t1.vehId < t2.vehId;
```

---

## 4. Running the tests

Unit tests run without a Spark session (each UDF is a plain Java lambda):

```sh
mvn test
```

---

## 5. Examples

Numbered examples mirror the MEOS C examples (`meos/examples/01_hello_world.c`, etc.):

| Class | Description |
|-------|-------------|
| `N01HelloWorld` | Round-trip a tgeompoint through hex-WKB |
| `N03BerlinMOD` | BerlinMOD Q1/Q3/Q4/Q5/Q6 portable SQL |

Run with:
```sh
spark-submit --class org.mobilitydb.spark.examples.N01HelloWorld \
    target/mobilityspark-0.1.0-SNAPSHOT-spark.jar
```

---

## 6. Project structure

```
src/main/java/org/mobilitydb/spark/
  MobilitySparkSession.java   — entry point: init MEOS + register all UDFs
  temporal/TemporalUDFs.java  — atTime and other time-axis UDFs
  geo/GeoUDFs.java            — eIntersects, nearestApproachDistance, eDwithin
  examples/N01HelloWorld.java — hello-world example
  examples/N03BerlinMOD.java  — BerlinMOD portable SQL demo
  demo/BerlinMODDemo.java     — Q1/Q3/Q4/Q5/Q6 implementation
  udfs/TemporalUDFs.java      — convenience facade (registerAll)

berlinmod/                    — portable SQL files (RFC #861)
libs/JMEOS-1.3.jar            — JMEOS 1.3 (includes libmeos.so)
tools/scripts/                — license header checker
```
