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

MobilitySpark holds no hand-written function registrations. The whole Spark SQL surface is emitted
at build time by a generator reading two inputs, both derived from one MobilityDB commit:

```
MobilityDB (master)
  -> MEOS-API      meos-idl.json, the catalog of the MEOS C surface
  -> JMEOS         org.jmeos:meos, the JVM FFI projection of that catalog
  -> MobilitySpark the Spark SQL surface, generated from both
```

The generator is `tools/codegen_jvm.py --engine spark-sql`, which **JMEOS owns**. This repository
stages it: `tools/codegen_jvm.py` and `tools/meos-idl.json` are
gitignored, and the refresh chain and CI copy them in. Nothing under `target/generated-sources`
is committed either. The hand-written classes in `src/main` register no function themselves: the
three classes of the package `catalyst` extend a Spark session: `MobilitySparkExtensions`, named in `spark.sql.extensions`,
registers the generated surface on every session and adds the two optimizer rules
`OrderConjunctsByCost`, which evaluates a conjunct calling a user-defined function after the
conjuncts that do not, and `MaterializeNestedLoopSides`, which computes once each side of a
nested-loop join that calls one.

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
building the JMEOS jar against them, regenerating the SQL surface and running the suite:

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

`generate-sources` runs the generator, `--engine spark-sql`; `build-helper` adds
`target/generated-sources/spark-sql/src/main/java` as a source root. No skip-the-tests variant is offered, deliberately: the suite is what distinguishes a
regenerated surface from a merely well-formed one, and the tree-hygiene job fails a tree that
introduces a skip flag.


## Using the binding

### The typed SQL surface

`MobilitySparkSql.registerAll` registers the surface the `spark-sql` engine generates from the
overloads the Flink SQL surface of MobilityFlink uses. Each MEOS SQL type is a Spark type of its
own (`tint`, `tfloat`, `tgeompoint`, `floatspan`, ...), carried as the form its catalog codec
writes, and each MobilityDB SQL name is registered once, its overload and result type chosen from
the argument types while Spark plans the call, as PostgreSQL resolves them:

Naming `MobilitySparkExtensions` in `spark.sql.extensions` registers the surface on every session
Spark builds: the session the builder answers, each `newSession()`, each Spark Connect session and
each Thrift Server session, with no further call:

```java
SparkSession spark = SparkSession.builder()
    .config("spark.sql.extensions", "org.mobilitydb.spark.catalyst.MobilitySparkExtensions")
    .getOrCreate();
```

```
spark-submit --conf spark.sql.extensions=org.mobilitydb.spark.catalyst.MobilitySparkExtensions ...
```

The same extension adds the optimizer rules of `org.mobilitydb.spark.catalyst`. A session built
without it registers the surface on itself through `MobilitySparkSql.registerAll`, which answers
alike on a session the extension already serves:

```java
import org.mobilitydb.spark.sql.MobilitySparkSql;

MobilitySparkSql.registerAll(spark);
```

```sql
SELECT maxValue(tintFromHexWKB(h)) FROM t;        -- an int over a tint
SELECT maxValue(tfloatFromHexWKB(h)) FROM t;      -- a double over a tfloat
SELECT startTimestamp(shiftTime(v, INTERVAL '1' DAY)) FROM t;
SELECT explode(unnest(set(array(3, 1, 2))));      -- a set-returning function's rows
```

A value enters through its type's constructors (`tfloatFromHexWKB`, `tfloat(value, time)`, ...),
as a PostgreSQL value does. A name Spark itself answers keeps answering its own arguments:
`lower('ABC')` and `round(2.5)` are Spark's. `GeneratedSqlSurfaceTest` asserts the queries
MobilityFlink's `GeneratedSqlSurfaceTest` asserts.

### Apache Sedona's raster

MEOS's `raster` and Apache Sedona's raster (`sedona-spark-shaded-4.0_2.13` 1.9.1, registered by
`SedonaContext.create(spark)`) live in one session under names they never share: every Sedona
name carries the prefix `RS_` or `ST_`, every MobilityDB name none. A Sedona raster is built from
the georeferencing of a MEOS raster `r` and the values of its band:

```sql
SELECT RS_AddBandFromArray(
    RS_MakeEmptyRaster(1, CASE rasterBandPixelType(r, 1) WHEN 'uint8' THEN 'B'
        WHEN 'int16' THEN 'S' WHEN 'uint16' THEN 'US' WHEN 'int32' THEN 'I'
        WHEN 'float32' THEN 'F' WHEN 'float64' THEN 'D' END,
      rasterWidth(r), rasterHeight(r), upperLeftX(r), upperLeftY(r),
      scaleX(r), scaleY(r), skewX(r), skewY(r), SRID(r)),
    flatten(dumpValues(r, 1, false)), 1, rasterBandNoDataValue(r, 1)) AS s
FROM t;
```

and a MEOS raster from a Sedona raster `s` by `raster(pixels, width, height, upperleftx,
upperlefty, scalex, scaley, skewx, skewy, srid, pixeltype[, nodataval])` over its `RS_` accessors,
the pixels being the bytes of the values of `RS_BandAsArray(s, 1)` in their order, each written as
`rasterPixels(addBand(makeEmptyRaster(1, 1, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 0), 1, pixeltype, v), 1)`.
The pixel types both carry (`8BUI`, `16BSI`, `16BUI`, `32BSI`, `32BF`, `64BF`) and the nodata
value, or its absence, cross both ways unchanged; `SedonaRasterBridgeTest` states the two queries
and asserts both round trips.

## Running the tests

```bash
mvn -B clean test
```

`GeneratedSqlSurfaceTest` drives the typed SQL surface and asserts that it *binds and executes*,
not merely that it compiles; `AggregatesCatalogTest` runs every catalog aggregate against
MobilityDB, and `SedonaRasterBridgeTest` the round trips of a raster through Apache Sedona.

MEOS keeps process-global state and cannot be re-initialised in a JVM that has finalised it, so
Surefire runs one JVM per test class (`forkCount=1`, `reuseForks=false`). Keep that configuration.


## Project structure

```
GENERATION.md                       the generation contract
pom.xml                             generator wiring, dependencies, Surefire policy
.github/workflows/maven.yml         tree hygiene, then provision + generate + test
src/main/java/org/mobilitydb/spark/catalyst/
  MobilitySparkExtensions.java      registers the surface on every session, adds the rules
src/test/java/org/mobilitydb/spark/
  GeneratedSqlSurfaceTest.java      the typed SQL surface binds and executes
tools/refresh-from-master.sh        the whole chain in one command
tools/refresh.conf                  this repository's last leg of that chain
```

Everything else the build needs — the catalog, the generator, the generated sources and the jar —
is produced rather than committed.
