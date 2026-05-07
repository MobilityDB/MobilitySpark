/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.spark.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.mobilitydb.spark.MobilitySparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * BerlinMOD Q1/Q3/Q4/Q5/Q6 — portable SQL dialect demo.
 *
 * Runs five BerlinMOD benchmark queries against the shared CSV dataset in
 * berlinmod/data/.  The SQL is identical to the MobilityDB (PostgreSQL) and
 * MobilityDuck (DuckDB) versions — only named functions, no platform-specific
 * operator symbols (RFC #861).
 *
 * Schema
 * ------
 *   Vehicles     (vehId INT, licence STRING, type STRING, model STRING)
 *   Trips        (tripId INT, vehId INT, trip STRING)   -- trip = tgeompoint hex-WKB
 *   QueryLicences(licenceId INT, licence STRING)
 *   QueryInstants(instantId INT, instant TIMESTAMP)
 *   QueryPoints  (pointId INT, geom STRING)             -- geom = WKT text, SRID 0
 *
 * Storage conventions
 * -------------------
 *   tgeompoint → hex-WKB STRING  (temporal_as_hexwkb / temporal_from_hexwkb)
 *   geometry   → WKT STRING      (e.g. "POINT(50 0)", parsed via geo_from_text)
 *
 * Usage
 * -----
 *   spark-submit --class org.mobilitydb.spark.demo.BerlinMODDemo \
 *       target/mobilityspark-*-spark.jar  berlinmod/data  [berlinmod/expected]
 *
 *   argv[0]: path to berlinmod/data directory (required)
 *   argv[1]: path to berlinmod/expected directory — if supplied, Q1/Q4/Q5/Q6
 *            results are compared against the CSV files there and the run
 *            exits with status 1 if any query differs.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 * JMEOS PR source: github.com/MobilityDB/JMEOS/pull/9
 */
public final class BerlinMODDemo {

    public static void main(String[] args) {
        String dataDir    = args.length > 0 ? args[0] : null;
        String expectDir  = args.length > 1 ? args[1] : null;

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("MobilitySpark — BerlinMOD portable SQL")
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
            if (dataDir != null) {
                loadFromCsv(spark, dataDir);
            } else {
                loadSynthetic(spark);
            }

            Dataset<Row> q1  = runQ1(spark);
            Dataset<Row> q2  = runQ2(spark);
            Dataset<Row> q3  = runQ3(spark);
            Dataset<Row> q4  = runQ4(spark);
            Dataset<Row> q5  = runQ5(spark);
            Dataset<Row> q6  = runQ6(spark);
            Dataset<Row> q7  = runQ7(spark);
            Dataset<Row> q8  = runQ8(spark);
            Dataset<Row> qrt = runQRT(spark);
            Dataset<Row> q9  = runQ9(spark);
            Dataset<Row> q10 = runQ10(spark);
            Dataset<Row> q11 = runQ11(spark);
            Dataset<Row> q12 = runQ12(spark);
            Dataset<Row> q13 = runQ13(spark);
            Dataset<Row> q14 = runQ14(spark);
            Dataset<Row> q15 = runQ15(spark);
            Dataset<Row> q16 = runQ16(spark);
            Dataset<Row> q17 = runQ17(spark);

            if (expectDir != null) {
                verify(spark, q1, q2, q3, q4, q5, q6, q7, q8, qrt,
                       q9, q10, q11, q12, q13, q14, q15, q16, q17, expectDir);
            }
        } finally {
            spark.stop();
        }
    }

    // ------------------------------------------------------------------
    // Data loading — from CSV files (shared with MobilityDB and MobilityDuck)
    // ------------------------------------------------------------------
    private static void loadFromCsv(SparkSession spark, String dataDir) {
        String dir = dataDir.endsWith("/") ? dataDir : dataDir + "/";

        spark.read().option("header", "true")
             .csv(dir + "vehicles.csv")
             .createOrReplaceTempView("Vehicles");

        // Trips: load WKT from CSV, convert to hex-WKB via tgeompoint UDF
        spark.read().option("header", "true")
             .csv(dir + "trips.csv")
             .createOrReplaceTempView("TripsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW Trips AS " +
                  "SELECT tripId, vehId, tgeompoint(trip) AS trip FROM TripsRaw")
             .count();  // force evaluation

        spark.read().option("header", "true")
             .csv(dir + "query_licences.csv")
             .createOrReplaceTempView("QueryLicences");

        // QueryInstants: ensure proper TIMESTAMP type
        spark.read().option("header", "true")
             .csv(dir + "query_instants.csv")
             .createOrReplaceTempView("QueryInstantsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW QueryInstants AS " +
                  "SELECT instantId, CAST(instant AS TIMESTAMP) AS instant FROM QueryInstantsRaw")
             .count();

        // QueryPoints: geom column stays as WKT text — eIntersects accepts WKT directly
        spark.read().option("header", "true")
             .csv(dir + "query_points.csv")
             .createOrReplaceTempView("QueryPoints");

        // QueryRegions: geom column stays as WKT text — eIntersects/eContains accept WKT
        spark.read().option("header", "true")
             .csv(dir + "query_regions.csv")
             .createOrReplaceTempView("QueryRegions");

        // QueryPeriods: period column stays as STRING — atTime(trip, period) parses it
        spark.read().option("header", "true")
             .csv(dir + "query_periods.csv")
             .createOrReplaceTempView("QueryPeriods");
    }

    // ------------------------------------------------------------------
    // Data loading — synthetic in-memory data (no external files needed)
    // ------------------------------------------------------------------
    private static void loadSynthetic(SparkSession spark) {
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW Vehicles AS SELECT * FROM VALUES
              (1, 'B-AA 100', 'passenger', 'Sedan'),
              (2, 'B-BB 200', 'passenger', 'SUV'),
              (3, 'B-CC 300', 'truck',     'Lorry'),
              (4, 'B-DD 400', 'truck',     'Truck'),
              (5, 'B-EE 500', 'passenger', 'Van')
            AS t(vehId, licence, type, model)
            """);

        // Two-step: raw WKT strings → tgeompoint UDF → hex-WKB Trips view.
        // Spark cannot evaluate UDFs inside VALUES clauses (inline-table restriction),
        // so the conversion must happen in a separate SELECT.
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW TripsRaw AS SELECT * FROM VALUES
              (1, 1, '[POINT(0 0)@2020-01-01 00:00:00+00, POINT(100 0)@2020-01-01 00:10:00+00]'),
              (2, 2, '[POINT(0 5)@2020-01-01 00:00:00+00, POINT(100 5)@2020-01-01 00:10:00+00]'),
              (3, 3, '[POINT(0 3)@2020-01-01 00:00:00+00, POINT(100 3)@2020-01-01 00:10:00+00]'),
              (4, 4, '[POINT(0 4)@2020-01-01 00:00:00+00, POINT(100 4)@2020-01-01 00:10:00+00]'),
              (5, 5, '[POINT(1000 1000)@2020-01-01 00:00:00+00, POINT(2000 1000)@2020-01-01 00:10:00+00]')
            AS t(tripId, vehId, trip)
            """);
        spark.sql("CREATE OR REPLACE TEMP VIEW Trips AS " +
                  "SELECT tripId, vehId, tgeompoint(trip) AS trip FROM TripsRaw")
             .count();

        spark.sql("""
            CREATE OR REPLACE TEMP VIEW QueryLicences AS SELECT * FROM VALUES
              (1, 'B-AA 100'),
              (2, 'B-CC 300')
            AS t(licenceId, licence)
            """);

        spark.sql("""
            CREATE OR REPLACE TEMP VIEW QueryInstants AS SELECT * FROM VALUES
              (1, TIMESTAMP '2020-01-01 00:05:00')
            AS t(instantId, instant)
            """);

        // geom stored as WKT text — eIntersects accepts WKT directly (no hex-EWKB needed)
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW QueryPoints AS SELECT * FROM VALUES
              (1, 'POINT(50 0)'),
              (2, 'POINT(50 5)')
            AS t(pointId, geom)
            """);

        // QueryRegions: polygon covering X=40-60, Y=-1 to 6 (captures vehicles 1-4)
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW QueryRegions AS SELECT * FROM VALUES
              (1, 'POLYGON((40 -1,60 -1,60 6,40 6,40 -1))')
            AS t(regionId, geom)
            """);

        // QueryPeriods: middle portion of all trips
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW QueryPeriods AS SELECT * FROM VALUES
              (1, '[2020-01-01 00:02:00+00,2020-01-01 00:08:00+00]')
            AS t(periodId, period)
            """);
    }

    // ------------------------------------------------------------------
    // Q1 — Models of vehicles with licences from QueryLicences.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ1(SparkSession spark) {
        System.out.println("=== Q1: Vehicle models for query licences ===");
        Dataset<Row> result = spark.sql("""
            SELECT l.licence, v.model
            FROM   QueryLicences l
            JOIN   Vehicles v ON v.licence = l.licence
            ORDER  BY l.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q2 — Licence plates of vehicles that ever entered a query region.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ2(SparkSession spark) {
        System.out.println("=== Q2: Vehicles that ever entered a query region ===");
        Dataset<Row> result = spark.sql("""
            SELECT DISTINCT v.licence
            FROM   Vehicles v
            JOIN   Trips t      ON  t.vehId = v.vehId
            JOIN   QueryRegions r ON eIntersects(t.trip, r.geom)
            ORDER  BY v.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q3 — Position of query-licence vehicles at each query instant.
    // Binary return: pos is MEOS hex-WKB via asHexWKB() — byte-for-byte
    // identical across MobilityDB, MobilityDuck, and MobilitySpark.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ3(SparkSession spark) {
        System.out.println("=== Q3: Vehicle positions at query instants (binary return) ===");
        Dataset<Row> result = spark.sql("""
            SELECT v.vehId     AS vehid,
                   v.licence,
                   i.instantId AS instantid,
                   asHexWKB(atTime(t.trip, i.instant)) AS pos
            FROM   QueryLicences l
            JOIN   Vehicles v  ON  v.licence = l.licence
            JOIN   Trips    t  ON  t.vehId   = v.vehId
            JOIN   QueryInstants i ON true
            WHERE  atTime(t.trip, i.instant) IS NOT NULL
            ORDER  BY v.vehId, i.instantId
            """);
        result.show(false);
        return result;
    }

    // ------------------------------------------------------------------
    // Q7 — Trip portions of query-licence vehicles during each query period.
    // Binary return: pos is MEOS hex-WKB via asHexWKB() — byte-for-byte
    // identical across MobilityDB, MobilityDuck, and MobilitySpark.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ7(SparkSession spark) {
        System.out.println("=== Q7: Trip portions during query periods (binary return) ===");
        Dataset<Row> result = spark.sql("""
            SELECT v.vehId     AS vehid,
                   v.licence,
                   p.periodId  AS periodid,
                   asHexWKB(atTime(t.trip, p.period)) AS pos
            FROM   QueryLicences l
            JOIN   Vehicles v  ON  v.licence = l.licence
            JOIN   Trips    t  ON  t.vehId   = v.vehId
            JOIN   QueryPeriods p ON true
            WHERE  atTime(t.trip, p.period) IS NOT NULL
            ORDER  BY v.vehId, p.periodId
            """);
        result.show(false);
        return result;
    }

    // ------------------------------------------------------------------
    // Q8 — Trajectory of each vehicle as hex-WKB geometry (byte-for-byte
    // identical across MobilityDB, MobilityDuck, and MobilitySpark).
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ8(SparkSession spark) {
        System.out.println("=== Q8: Vehicle trajectories (hex WKB) ===");
        Dataset<Row> result = spark.sql("""
            SELECT tripId AS tripid,
                   trajectory(trip) AS traj
            FROM   Trips
            ORDER  BY tripId
            """);
        result.show(false);
        return result;
    }

    // ------------------------------------------------------------------
    // QRT — Binary roundtrip: WKT text in → MEOS hex-WKB out.
    // All five trips serialised with asHexWKB(); output must be
    // byte-for-byte identical across all three ecosystem platforms.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQRT(SparkSession spark) {
        System.out.println("=== QRT: Binary roundtrip — asHexWKB(trip) for all trips ===");
        Dataset<Row> result = spark.sql("""
            SELECT tripId AS tripid,
                   asHexWKB(trip) AS trip_hexwkb
            FROM   Trips
            ORDER  BY tripId
            """);
        result.show(false);
        return result;
    }

    // ------------------------------------------------------------------
    // Q4 — Licence plates of vehicles that ever passed a query point.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ4(SparkSession spark) {
        System.out.println("=== Q4: Vehicles that ever passed a query point ===");
        Dataset<Row> result = spark.sql("""
            SELECT DISTINCT v.licence
            FROM   Vehicles v
            JOIN   Trips t      ON t.vehId  = v.vehId
            JOIN   QueryPoints p ON eIntersects(t.trip, p.geom)
            ORDER  BY v.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q5 — Minimum nearest-approach distance between each pair of query vehicles.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ5(SparkSession spark) {
        System.out.println("=== Q5: Min nearest-approach distance between vehicle pairs ===");
        Dataset<Row> result = spark.sql("""
            SELECT l1.licence AS licence1,
                   l2.licence AS licence2,
                   MIN(nearestApproachDistance(t1.trip, t2.trip)) AS min_dist
            FROM   QueryLicences l1
            JOIN   Vehicles v1  ON  v1.licence = l1.licence
            JOIN   Trips    t1  ON  t1.vehId   = v1.vehId
            JOIN   QueryLicences l2 ON l1.licenceId < l2.licenceId
            JOIN   Vehicles v2  ON  v2.licence = l2.licence
            JOIN   Trips    t2  ON  t2.vehId   = v2.vehId
            WHERE  nearestApproachDistance(t1.trip, t2.trip) IS NOT NULL
            GROUP  BY l1.licence, l2.licence
            ORDER  BY l1.licence, l2.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q6 — Pairs of trucks that ever came within 10 m of each other.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ6(SparkSession spark) {
        System.out.println("=== Q6: Truck pairs within 10 m ===");
        Dataset<Row> result = spark.sql("""
            SELECT v1.licence AS licence1,
                   v2.licence AS licence2
            FROM   Vehicles v1
            JOIN   Trips t1 ON t1.vehId = v1.vehId
            JOIN   Vehicles v2 ON v1.vehId < v2.vehId
            JOIN   Trips t2 ON t2.vehId = v2.vehId
            WHERE  v1.type  = 'truck'
              AND  v2.type  = 'truck'
              AND  eDwithin(t1.trip, t2.trip, 10.0)
            ORDER  BY v1.licence, v2.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q9 — Longest distance travelled by a vehicle during each query period.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ9(SparkSession spark) {
        System.out.println("=== Q9: Longest distance per vehicle per query period ===");
        Dataset<Row> result = spark.sql("""
            WITH Distances AS (
              SELECT p.periodId, p.period, t.vehId,
                     SUM(length(atTime(t.trip, p.period))) AS dist
              FROM   Trips t, QueryPeriods p
              GROUP  BY p.periodId, p.period, t.vehId
            )
            SELECT periodId, period, ROUND(CAST(MAX(dist) AS DECIMAL(20,3)), 3) AS maxDist
            FROM   Distances
            GROUP  BY periodId, period
            ORDER  BY periodId
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q10 — When did query-licence vehicles meet other vehicles (within 3 m)?
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ10(SparkSession spark) {
        System.out.println("=== Q10: When did query-licence vehicles meet others within 3 m ===");
        Dataset<Row> result = spark.sql("""
            WITH Temp AS (
              SELECT l.licence AS licence1, t2.vehId AS car2Id,
                     whenTrue(tDwithin(t1.trip, t2.trip, 3.0)) AS periods,
                     t1.tripId AS tripId1, t2.tripId AS tripId2
              FROM   QueryLicences l
              JOIN   Vehicles v1 ON v1.licence = l.licence
              JOIN   Trips    t1 ON t1.vehId   = v1.vehId
              JOIN   Trips    t2 ON t1.vehId  <> t2.vehId
            )
            SELECT licence1, car2Id, periods
            FROM   Temp
            WHERE  periods IS NOT NULL
            ORDER  BY licence1, car2Id, tripId1, tripId2
            """);
        result.show(false);
        return result;
    }

    // ------------------------------------------------------------------
    // Q11 — Which vehicles passed a query point at a query instant?
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ11(SparkSession spark) {
        System.out.println("=== Q11: Vehicles at query points at query instants ===");
        Dataset<Row> result = spark.sql("""
            WITH Temp AS (
              SELECT p.pointId, p.geom, i.instantId, i.instant, t.vehId
              FROM   Trips t, QueryPoints p, QueryInstants i
              WHERE  valueAtTimestamp(t.trip, i.instant) = p.geom
            )
            SELECT t.pointId, t.geom, t.instantId, t.instant, v.licence
            FROM   Temp t
            JOIN   Vehicles v ON t.vehId = v.vehId
            ORDER  BY t.pointId, t.instantId, v.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q12 — Pairs of vehicles at the same query point at the same query instant.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ12(SparkSession spark) {
        System.out.println("=== Q12: Vehicle pairs at same point at same instant ===");
        Dataset<Row> result = spark.sql("""
            WITH Temp AS (
              SELECT DISTINCT p.pointId, p.geom, i.instantId, i.instant, t.vehId
              FROM   Trips t, QueryPoints p, QueryInstants i
              WHERE  valueAtTimestamp(t.trip, i.instant) = p.geom
            )
            SELECT DISTINCT t1.pointId, t1.geom,
                   t1.instantId, t1.instant,
                   v1.licence AS licence1, v2.licence AS licence2
            FROM   Temp t1
            JOIN   Vehicles v1 ON t1.vehId = v1.vehId
            JOIN   Temp     t2 ON t1.vehId < t2.vehId
                              AND t1.pointId   = t2.pointId
                              AND t1.instantId = t2.instantId
            JOIN   Vehicles v2 ON t2.vehId = v2.vehId
            ORDER  BY t1.pointId, t1.instantId, v1.licence, v2.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q13 — Vehicles that travelled within a query region during a query period.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ13(SparkSession spark) {
        System.out.println("=== Q13: Vehicles in query regions during query periods ===");
        Dataset<Row> result = spark.sql("""
            WITH Temp AS (
              SELECT DISTINCT r.regionId, p.periodId, p.period, t.vehId
              FROM   Trips t, QueryRegions r, QueryPeriods p
              WHERE  r.regionId <= 10 AND p.periodId <= 10
                AND  eIntersects(atTime(t.trip, p.period), r.geom)
            )
            SELECT DISTINCT t.regionId, t.periodId, t.period, v.licence
            FROM   Temp t, Vehicles v
            WHERE  t.vehId = v.vehId
            ORDER  BY t.regionId, t.periodId, v.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q14 — Vehicles inside a query region at a query instant.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ14(SparkSession spark) {
        System.out.println("=== Q14: Vehicles inside query regions at query instants ===");
        Dataset<Row> result = spark.sql("""
            WITH Temp AS (
              SELECT DISTINCT r.regionId, i.instantId, i.instant, t.vehId
              FROM   Trips t, QueryRegions r, QueryInstants i
              WHERE  geomContains(r.geom, valueAtTimestamp(t.trip, i.instant))
            )
            SELECT DISTINCT t.regionId, t.instantId, t.instant, v.licence
            FROM   Temp t
            JOIN   Vehicles v ON t.vehId = v.vehId
            ORDER  BY t.regionId, t.instantId, v.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q15 — Vehicles that passed a query point during a query period.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ15(SparkSession spark) {
        System.out.println("=== Q15: Vehicles passing query points during query periods ===");
        Dataset<Row> result = spark.sql("""
            WITH Temp AS (
              SELECT DISTINCT pt.pointId, pt.geom, pr.periodId, pr.period, t.vehId
              FROM   Trips t, QueryPoints pt, QueryPeriods pr
              WHERE  pt.pointId  <= 10 AND pr.periodId <= 10
                AND  eIntersects(atTime(t.trip, pr.period), pt.geom)
            )
            SELECT DISTINCT t.pointId, t.geom, t.periodId, t.period, v.licence
            FROM   Temp t, Vehicles v
            WHERE  t.vehId = v.vehId
            ORDER  BY t.pointId, t.periodId, v.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q16 — Pairs of query-licence vehicles both in a region during a period
    //        but always spatially disjoint.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ16(SparkSession spark) {
        System.out.println("=== Q16: Vehicle pairs always disjoint in shared region/period ===");
        Dataset<Row> result = spark.sql("""
            SELECT p.periodId, p.period, r.regionId,
                   l1.licence AS licence1, l2.licence AS licence2
            FROM   QueryLicences l1
            JOIN   Vehicles      v1 ON v1.licence = l1.licence
            JOIN   Trips         t1 ON t1.vehId   = v1.vehId
            JOIN   QueryLicences l2 ON l1.licenceId < l2.licenceId
            JOIN   Vehicles      v2 ON v2.licence = l2.licence
            JOIN   Trips         t2 ON t2.vehId   = v2.vehId
            JOIN   QueryPeriods  p  ON true
            JOIN   QueryRegions  r  ON true
            WHERE  l1.licenceId <= 10 AND l2.licenceId <= 10
              AND  p.periodId   <= 10
              AND  r.regionId   <= 10
              AND  eIntersects(atTime(t1.trip, p.period), r.geom)
              AND  eIntersects(atTime(t2.trip, p.period), r.geom)
              AND  aDisjoint(atTime(t1.trip, p.period), atTime(t2.trip, p.period))
            ORDER  BY p.periodId, r.regionId, l1.licence, l2.licence
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Q17 — Query points visited by the most distinct vehicles.
    // ------------------------------------------------------------------
    private static Dataset<Row> runQ17(SparkSession spark) {
        System.out.println("=== Q17: Most-visited query points ===");
        Dataset<Row> result = spark.sql("""
            WITH PointCount AS (
              SELECT p.pointId, COUNT(DISTINCT t.vehId) AS hits
              FROM   Trips t, QueryPoints p
              WHERE  eIntersects(t.trip, p.geom)
              GROUP  BY p.pointId
            )
            SELECT pointId, hits
            FROM   PointCount
            WHERE  hits = (SELECT MAX(hits) FROM PointCount)
            ORDER  BY pointId
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Cross-platform verification: compare against expected CSV files.
    // Q1–Q17 and QRT are compared.  Q3/Q7/QRT use asHexWKB() for temporal
    // types; Q8 uses geo_as_hexewkb() for geometry.
    // ------------------------------------------------------------------
    private static void verify(SparkSession spark,
                                Dataset<Row> q1,  Dataset<Row> q2,
                                Dataset<Row> q3,  Dataset<Row> q4,
                                Dataset<Row> q5,  Dataset<Row> q6,
                                Dataset<Row> q7,  Dataset<Row> q8,
                                Dataset<Row> qrt, Dataset<Row> q9,
                                Dataset<Row> q10, Dataset<Row> q11,
                                Dataset<Row> q12, Dataset<Row> q13,
                                Dataset<Row> q14, Dataset<Row> q15,
                                Dataset<Row> q16, Dataset<Row> q17,
                                String expectDir) {
        String dir = expectDir.endsWith("/") ? expectDir : expectDir + "/";
        boolean allPass = true;
        allPass &= compareQuery(spark, "Q1",  q1,  dir + "q01.csv");
        allPass &= compareQuery(spark, "Q2",  q2,  dir + "q02.csv");
        allPass &= compareQuery(spark, "Q3",  q3,  dir + "q03.csv");
        allPass &= compareQuery(spark, "Q4",  q4,  dir + "q04.csv");
        allPass &= compareQuery(spark, "Q5",  q5,  dir + "q05.csv");
        allPass &= compareQuery(spark, "Q6",  q6,  dir + "q06.csv");
        allPass &= compareQuery(spark, "Q7",  q7,  dir + "q07.csv");
        allPass &= compareQuery(spark, "Q8",  q8,  dir + "q08.csv");
        allPass &= compareQuery(spark, "QRT", qrt, dir + "qrt.csv");
        allPass &= compareQuery(spark, "Q9",  q9,  dir + "q09.csv");
        allPass &= compareQuery(spark, "Q10", q10, dir + "q10.csv");
        allPass &= compareQuery(spark, "Q11", q11, dir + "q11.csv");
        allPass &= compareQuery(spark, "Q12", q12, dir + "q12.csv");
        allPass &= compareQuery(spark, "Q13", q13, dir + "q13.csv");
        allPass &= compareQuery(spark, "Q14", q14, dir + "q14.csv");
        allPass &= compareQuery(spark, "Q15", q15, dir + "q15.csv");
        allPass &= compareQuery(spark, "Q16", q16, dir + "q16.csv");
        allPass &= compareQuery(spark, "Q17", q17, dir + "q17.csv");
        System.out.println(allPass ? "\nALL PASS" : "\nFAILURES DETECTED");
        if (!allPass) System.exit(1);
    }

    private static boolean compareQuery(SparkSession spark, String name,
                                         Dataset<Row> got, String expectedCsv) {
        Dataset<Row> expected = spark.read().option("header", "true").csv(expectedCsv);
        // Compare as sorted lists of column-delimited strings
        List<String> gotRows  = toSortedStrings(got);
        List<String> expRows  = toSortedStrings(expected);
        if (gotRows.equals(expRows)) {
            System.out.println("[PASS] " + name);
            return true;
        }
        System.out.println("[FAIL] " + name);
        System.out.println("  Expected: " + expRows);
        System.out.println("  Got:      " + gotRows);
        return false;
    }

    private static List<String> toSortedStrings(Dataset<Row> ds) {
        String[] rows = (String[]) ds.toJSON().collect();
        Arrays.sort(rows);
        return Arrays.asList(rows);
    }
}
