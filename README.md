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
- **Apache Spark 3.5** (provided at runtime; not needed to compile or run unit tests)
- **JMEOS 1.4** — bundled in `libs/JMEOS-1.4.jar` (patched from
  [MobilityDB/JMEOS PR #9](https://github.com/MobilityDB/JMEOS/pull/9) with
  cross-platform native library loading)

> **Platform support:**
> - **Linux x86-64** — fully supported; `libmeos.so` is bundled in the jar.
> - **macOS (Apple Silicon / Intel)** — supported; requires MEOS installed via
>   Homebrew (see §2.3) or built from source. CI-tested on `macos-latest`.
> - **Windows x86-64** — CI bootstrap in progress (non-blocking); manual MEOS
>   build via MSYS2 required (see §2.4).

---

## 2. Building MobilitySpark

### 2.1 Clone the repository

```sh
git clone https://github.com/MobilityDB/MobilitySpark.git
cd MobilitySpark
```

### 2.2 Linux — no extra steps needed

`libmeos.so` for Linux x86-64 is bundled in `libs/JMEOS-1.4.jar`. Install the
runtime dependencies and compile:

```sh
# Ubuntu / Debian
sudo apt-get install -y libjson-c5 libgeos-c1t64 libproj25 libgsl27
mvn compile
```

### 2.3 macOS — install MEOS via Homebrew

```sh
# Install MEOS build dependencies
brew install json-c geos proj gsl cmake ninja

# Clone MobilityDB and build the standalone MEOS library
git clone https://github.com/MobilityDB/MobilityDB.git
cmake -S MobilityDB -B meos-build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="$(brew --prefix)" \
  -DMEOS=ON -DCBUFFER=ON -DNPOINT=ON -DPOSE=OFF -DRGEO=OFF
cmake --build meos-build -j
sudo cmake --install meos-build      # installs libmeos.dylib to /usr/local/lib/

# Tell JMEOS where to find the library, then build
export DYLD_LIBRARY_PATH=/usr/local/lib
mvn compile
mvn test
```

### 2.4 Windows — install MEOS via MSYS2

```sh
# In an MSYS2 UCRT64 shell
pacman -S mingw-w64-ucrt-x86_64-{gcc,cmake,ninja,json-c,geos,proj,gsl}

# Clone MobilityDB and build MEOS
git clone https://github.com/MobilityDB/MobilityDB.git
cmake -S MobilityDB -B meos-build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DMEOS=ON -DCBUFFER=ON -DNPOINT=ON -DPOSE=OFF -DRGEO=OFF
cmake --build meos-build -j
cmake --install meos-build --prefix "$PWD/meos-install"

# Add libmeos.dll to PATH, then build with Maven (PowerShell)
$env:PATH = "$PWD\meos-install\bin;" + $env:PATH
mvn compile
mvn test
```

### 2.5 Compile (all platforms)

```sh
mvn compile
```

### 2.6 Package (fat jar for `spark-submit`)

```sh
mvn package -DskipTests
```

The fat jar is written to `target/mobilityspark-0.1.0-SNAPSHOT-spark.jar`.

---

## 3. Using MobilitySpark

### 3.1. Initialise MEOS and register UDFs

```java
SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
    // All UDFs are now available in Spark SQL
    spark.sql("SELECT atTime(trip, '2020-01-01 00:30:00+00') FROM trips").show();
}
```

`MobilitySparkSession.create()` initialises MEOS, registers all UDFs, and loads the
spatial reference system table for geodetic operations.

### 3.2. Available UDFs

All temporal values are stored as **hex-WKB strings** (portable binary format, identical
across MobilityDB, MobilityDuck, and MobilitySpark). Geometry values use **hex-EWKB strings**.

#### Temporal axis UDFs

| UDF | Signature | Description |
|-----|-----------|-------------|
| `atTime` | `(STRING, STRING) → STRING` | Restrict to a timestamp or tstzspan literal |
| `startTimestamp` | `(STRING) → TIMESTAMP` | First timestamp of the temporal value |
| `endTimestamp` | `(STRING) → TIMESTAMP` | Last timestamp of the temporal value |
| `numInstants` | `(STRING) → INT` | Number of instants |
| `speed` | `(STRING) → STRING` | Instantaneous speed (returns tfloat hex-WKB) |
| `atGeometry` | `(STRING, STRING) → STRING` | Restrict tgeompoint to a geometry (WKT) |
| `asHexWKB` | `(STRING) → STRING` | Parse and re-serialise to canonical hex-WKB |

#### Geo UDFs (eXistential — ever-true semantics)

| UDF | Signature | Description |
|-----|-----------|-------------|
| `eIntersects` | `(STRING, STRING) → BOOLEAN` | Ever intersects a geometry |
| `nearestApproachDistance` | `(STRING, STRING) → DOUBLE` | Min distance at any common instant |
| `eDwithin` | `(STRING, STRING, DOUBLE) → BOOLEAN` | Ever within given distance |

#### TemporalParquet UDFs

These UDFs support reading from and writing to Parquet files whose temporal columns were
written by MobilityDuck's `asBinary()` (Parquet `BYTE_ARRAY`).

**Read (Parquet BINARY → hex-WKB string):**

| UDF | Input type | Description |
|-----|------------|-------------|
| `tgeompointFromBinary` | `BINARY` | tgeompoint (2D/3D Cartesian) |
| `tgeogpointFromBinary` | `BINARY` | tgeogpoint (geodetic) |
| `tintFromBinary` | `BINARY` | tint |
| `tfloatFromBinary` | `BINARY` | tfloat |
| `tboolFromBinary` | `BINARY` | tbool |
| `ttextFromBinary` | `BINARY` | ttext |
| `tstzspanFromBinary` | `BINARY` | tstzspan |
| `intspanFromBinary` | `BINARY` | intspan |
| `floatspanFromBinary` | `BINARY` | floatspan |
| `bigintspanFromBinary` | `BINARY` | bigintspan |
| `datespanFromBinary` | `BINARY` | datespan |
| `tstzspansetFromBinary` | `BINARY` | tstzspanset |
| `intspansetFromBinary` | `BINARY` | intspanset |
| `floatspansetFromBinary` | `BINARY` | floatspanset |
| `bigintspansetFromBinary` | `BINARY` | bigintspanset |
| `datespansetFromBinary` | `BINARY` | datespanset |

**Write (hex-WKB string → Parquet BINARY):**

| UDF | Signature | Description |
|-----|-----------|-------------|
| `asBinary` | `(STRING) → BINARY` | Any hex-WKB → raw bytes for Parquet BYTE_ARRAY |

### 3.3. Portable SQL (BerlinMOD benchmark)

The SQL queries in `berlinmod/` use **named functions only** — no operator symbols — so the
same file runs unchanged on MobilityDB (PostgreSQL), MobilityDuck (DuckDB), and MobilitySpark
(Spark SQL). This is the portability contract defined in
[Discussion #861](https://github.com/MobilityDB/MobilityDB/discussions/861).

Run all 17 BerlinMOD queries plus the round-trip test:

```sh
./berlinmod/run_mspark.sh
```

This script builds the fat jar if needed, then submits `BerlinMODDemo` against the
pre-generated data in `berlinmod/data/` and verifies results against `berlinmod/expected/`.
`spark-submit` must be on `PATH` (or pass its path as the first argument).

### 3.4. TemporalParquet: edge-to-cloud pipeline

MobilitySpark can read Parquet files produced by MobilityDuck at the edge:

```sql
-- 1. Edge (MobilityDuck): write tgeompoint trajectories to Parquet
COPY (SELECT vesselId, asBinary(trip) AS trip FROM Trips) TO 'trips.parquet';

-- 2. Cloud (MobilitySpark): read and query
CREATE OR REPLACE TEMPORARY VIEW Trips AS
  SELECT vesselId, tgeompointFromBinary(trip) AS trip
  FROM parquet.`trips.parquet`;

SELECT vesselId, startTimestamp(trip), endTimestamp(trip), numInstants(trip)
FROM Trips;
```

### 3.5. Sample queries

Restrict a trip to a query instant:
```sql
SELECT atTime(trip, '2020-01-01 00:30:00+00') AS pos FROM Trips;
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

The test suite covers 51 unit tests across three test classes:

| Class | Tests | Coverage |
|-------|-------|----------|
| `GeoUDFsTest` | 23 | eIntersects, nearestApproachDistance, eDwithin |
| `TemporalUDFsTest` | 17 | atTime, TemporalParquet tgeompoint/tgeogpoint/scalar round-trips |
| `SpanUDFsTest` | 11 | TemporalParquet span/spanset round-trips |

---

## 5. Examples

Numbered examples mirror the MEOS C examples (`meos/examples/01_hello_world.c`, etc.):

| Class | Description |
|-------|-------------|
| `N01HelloWorld` | Round-trip a tgeompoint through hex-WKB |
| `N03BerlinMOD` | BerlinMOD Q1–Q17 portable SQL |

Run with:
```sh
spark-submit --class org.mobilitydb.spark.examples.N01HelloWorld \
    target/mobilityspark-0.1.0-SNAPSHOT-spark.jar
```

---

## 6. Project structure

```
src/main/java/org/mobilitydb/spark/
  MobilitySparkSession.java        — entry point: init MEOS + register all UDFs
  temporal/
    TemporalUDFs.java              — time-axis UDFs + TemporalParquet scalar UDFs
    SpanUDFs.java                  — TemporalParquet span/spanset UDFs
  geo/
    GeoUDFs.java                   — eIntersects, nearestApproachDistance, eDwithin
  examples/
    N01HelloWorld.java             — hello-world example
    N03BerlinMOD.java              — BerlinMOD portable SQL demo
  demo/
    BerlinMODDemo.java             — Q1–Q17 + QRT implementation
  udfs/
    TemporalUDFs.java              — convenience facade (registerAll)

src/test/java/org/mobilitydb/spark/
  udfs/TemporalUDFsTest.java       — 17 unit tests
  udfs/SpanUDFsTest.java           — 11 unit tests
  geo/GeoUDFsTest.java             — 23 unit tests
  demo/BerlinMODIntegrationTest.java
  examples/AISDataIntegrationTest.java

berlinmod/
  q01.sql … q17.sql, qrt.sql      — portable SQL (RFC #861)
  data/                            — pre-generated BerlinMOD dataset (scale 0.005)
  expected/                        — expected query results for comparison
  run_mspark.sh                    — end-to-end runner (build + spark-submit + compare)
  run_mbdb.sh                      — MobilityDB runner
  run_mduck.sh                     — MobilityDuck runner

libs/JMEOS-1.3.jar                 — JMEOS 1.3 (Linux x86-64, includes libmeos.so)
tools/scripts/                     — license header checker (CI)
```
