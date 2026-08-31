# MobilitySpark

An open-source large-scale geospatial trajectory data analytics platform based on [Spark](https://spark.apache.org/).

::: note 📝

MobilitySpark brings [MobilityDB](https://github.com/MobilityDB/MobilityDB) datatypes and functions into the Spark environment. Its Spark SQL surface is *generated* from the MEOS catalog rather than hand-written, so it tracks MobilityDB master rather than a snapshot of it.

:::

<img src="images/mobilitydb-logo.svg" width="200" alt="MobilityDB Logo" />

The MobilityDB project is developed by the Computer & Decision Engineering Department of the [Université libre de Bruxelles](https://www.ulb.be/) (ULB) under the direction of [Prof. Esteban Zimányi](http://cs.ulb.ac.be/members/esteban/). ULB is an OGC Associate Member and member of the OGC Moving Feature Standard Working Group ([MF-SWG](https://www.ogc.org/projects/groups/movfeatswg)).

<img src="images/OGC_Associate_Member_3DR.png" width="100" alt="OGC Associate Member Logo" />

More information about MobilityDB, including publications, presentations, etc., can be found in the MobilityDB [website](https://mobilitydb.com).


## Table of Contents

- [How it is built](#how-it-is-built)
- [Requirements](#requirements)
- [Building](#building)
- [Using the binding](#using-the-binding)
- [Running the tests](#running-the-tests)
- [Project structure](#project-structure)


## How it is built

MobilitySpark holds no hand-written UDF registrations. The whole Spark SQL surface is emitted
at build time by a generator reading two inputs, both derived from one MobilityDB commit:

```
MobilityDB (master)
  -> MEOS-API      meos-idl.json, the catalog of the MEOS C surface
  -> JMEOS         org.jmeos:meos, the JVM FFI projection of that catalog
  -> MobilitySpark the Spark UDF layer, generated from both
```

The generator is `tools/codegen_jvm.py --engine spark`, which **JMEOS owns**. This repository
stages it: `tools/codegen_jvm.py`, `tools/codegen_spark_udfs.py` and `tools/meos-idl.json` are
gitignored, and the refresh chain and CI copy them in. Nothing under `target/generated-sources`
is committed either. The one hand-written class in `src/main` is `MeosMemory`, which frees the
native pointers MEOS returns.

`GENERATION.md` is the full contract.


## Requirements

- Java 21 (the build sets `maven.compiler.source`/`target` to 21)
- Maven 3.8+
- Python 3, for the generator
- `libmeos.so` built from MobilityDB master **with every family on**, and the
  `org.jmeos:meos:1.0` jar built against the same commit

That last point is not optional bookkeeping. The generated surface names every function the
catalog carries, and JNR-FFI resolves symbols lazily, so a `libmeos` built without `-DALL=ON`
fails at the call rather than at load. `tools/refresh-from-master.sh` below produces a matching
pair; build MEOS by hand and the flag is `-DMEOS=ON -DALL=ON`.


## Building

One command runs the whole chain — deriving `libmeos` and the catalog from MobilityDB master,
building the JMEOS jar against them, regenerating the UDF layer and running the suite:

```bash
tools/refresh-from-master.sh
```

To refresh against a local MobilityDB branch instead of master:

```bash
tools/refresh-from-master.sh --mdb ~/src/MobilityDB
```

It runs the shared `MobilityDB/MEOS-API` `refresh-jvm-chain.sh` over this repository, cloning
MEOS-API the first time into `.meos-chain/`. This repository's own last leg is
`tools/refresh.conf`.

With the catalog and the jar already in place, the ordinary Maven build regenerates and tests:

```bash
mvn -B clean test
```

`generate-sources` runs the generator; `build-helper` adds `target/generated-sources/spark` as a
source root. No skip-the-tests variant is offered, deliberately: the suite is what distinguishes a
regenerated surface from a merely well-formed one, and the tree-hygiene job fails a tree that
introduces a skip flag.


## Using the binding

Register the generated surface on a `SparkSession`, then call the functions from Spark SQL:

```java
import org.mobilitydb.spark.generated.GeneratedSpatioTemporalUDFs;

SparkSession spark = SparkSession.builder().appName("mobilityspark").master("local[1]").getOrCreate();
GeneratedSpatioTemporalUDFs.registerAll(spark);
```

Temporal values travel as strings in MEOS hex-WKB, and geometries as hex-EWKB, so any Spark type
system carries them:

```sql
-- accessors under their canonical MobilityDB SQL names
SELECT numInstants(trip) FROM trips;
SELECT atTime(trip, '[2001-01-01, 2001-01-03]') FROM trips;

-- spatial relationships and distance
SELECT eIntersects(trip, 'LINESTRING(1.5 0, 1.5 5)') FROM trips;
SELECT nearestApproachDistance(a.trip, b.trip) FROM trips a, trips b;

-- an N-by-N pair surface over two arrays of trips, consumed with explode:
-- pr.i and pr.j index back into the (0-based) input arrays
SELECT pr.i, pr.j
FROM (SELECT explode(eDwithinPairs(trips_a, trips_b, 1000.0)) AS pr FROM trip_arrays);
```

The C-level entry points are registered too (`tint_in`, `tint_out`, `temporal_num_instants`,
`tnumber_twavg`, …), as is the portable bare-name operator dialect the catalog's `byOperator` map
defines. Because the names come from the catalog, a rename upstream arrives here by
regeneration rather than by editing this repository.

Free what you keep: pointers returned across the FFI boundary are raw native addresses the JVM
garbage collector does not track. `MeosMemory.free(Pointer...)` releases them, and a query that
leaks one per row will exhaust the native heap on a cross join.


## Running the tests

```bash
mvn -B clean test
```

`GeneratedSurfaceTest` drives the generated surface from known hex-WKB literals and asserts that
it *binds and executes*, not merely that it compiles: scalar accessors and I/O round-trips, double
/ boolean / byte marshalling, the cbuffer and npoint families, the JSON-path surface, value-array
accessors, the N-by-N array UDFs, the canonical `@sqlfn` names with runtime argument-kind
dispatch, a folded out-parameter, and the H3 cell prefilter. The bare-name operators are read from
the catalog's own `byOperator` map rather than hard-coded, so a dialect rename updates the test by
itself.

MEOS keeps process-global state and cannot be re-initialised in a JVM that has finalised it, so
Surefire runs one JVM per test class (`forkCount=1`, `reuseForks=false`). Keep that configuration.


## Project structure

```
GENERATION.md                       the generation contract
pom.xml                             generator wiring, dependencies, Surefire policy
.github/workflows/maven.yml         tree hygiene, then provision + generate + test
src/main/java/org/mobilitydb/spark/
  MeosMemory.java                   frees the native pointers MEOS returns
src/test/java/org/mobilitydb/spark/
  GeneratedSurfaceTest.java         the generated surface binds and executes
tools/refresh-from-master.sh        the whole chain in one command
tools/refresh.conf                  this repository's last leg of that chain
```

Everything else the build needs — the catalog, the generator, the generated sources and the jar —
is produced rather than committed.
