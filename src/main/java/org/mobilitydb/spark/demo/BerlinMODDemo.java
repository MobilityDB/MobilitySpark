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

            if (expectDir != null) {
                verify(spark, q1, q2, q3, q4, q5, q6, q7, q8, qrt, expectDir);
            }
        } finally {
            spark.stop();
        }
    }

    // ------------------------------------------------------------------
    // Data loading — from CSV files (shared with MobilityDB and MobilityDuck)
    // ------------------------------------------------------------------

    /** Package-visible entry point used by BerlinMODBench. */
    static void loadFromCsvPublic(SparkSession spark, String dataDir) {
        loadFromCsv(spark, dataDir);
    }

    private static void loadFromCsv(SparkSession spark, String dataDir) {
        String dir = dataDir.endsWith("/") ? dataDir : dataDir + "/";

        spark.read().option("header", "true").option("inferSchema", "true")
             .csv(dir + "vehicles.csv")
             .createOrReplaceTempView("Vehicles");

        // Trips: hex-WKB strings — load directly; all UDFs call temporal_from_hexwkb.
        spark.read().option("header", "true").option("inferSchema", "true")
             .csv(dir + "trips.csv")
             .createOrReplaceTempView("Trips");

        spark.read().option("header", "true").option("inferSchema", "true")
             .csv(dir + "query_licences.csv")
             .createOrReplaceTempView("QueryLicences");

        // QueryInstants: ensure proper TIMESTAMP type
        spark.read().option("header", "true")
             .csv(dir + "query_instants.csv")
             .createOrReplaceTempView("QueryInstantsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW QueryInstants AS " +
                  "SELECT CAST(instantid AS INT) AS instantId, " +
                  "CAST(instant AS TIMESTAMP) AS instant FROM QueryInstantsRaw")
             .count();

        // QueryPoints: geom column is WKT text; geomWKT is an alias used by Q11/Q12/Q15
        // for portable display (avoids geo_as_text precision divergence).
        spark.read().option("header", "true")
             .csv(dir + "query_points.csv")
             .createOrReplaceTempView("QueryPointsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW QueryPoints AS " +
                  "SELECT CAST(pointid AS INT) AS pointId, geom, geom AS geomWKT " +
                  "FROM QueryPointsRaw")
             .count();

        // QueryRegions: geom column stays as WKT text — eIntersects/geomContains accept WKT.
        spark.read().option("header", "true")
             .csv(dir + "query_regions.csv")
             .createOrReplaceTempView("QueryRegionsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW QueryRegions AS " +
                  "SELECT CAST(regionid AS INT) AS regionId, geom FROM QueryRegionsRaw")
             .count();

        // QueryPeriods: period column stays as STRING — atTime(trip, period) parses it.
        spark.read().option("header", "true")
             .csv(dir + "query_periods.csv")
             .createOrReplaceTempView("QueryPeriodsRaw");
        spark.sql("CREATE OR REPLACE TEMP VIEW QueryPeriods AS " +
                  "SELECT CAST(periodid AS INT) AS periodId, period FROM QueryPeriodsRaw")
             .count();
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

        // tgeompoint WKT → hex-WKB via registered UDF
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW Trips AS SELECT * FROM VALUES
              (1, 1, tgeompoint('[POINT(0 0)@2020-01-01 00:00:00+00, POINT(100 0)@2020-01-01 00:10:00+00]')),
              (2, 2, tgeompoint('[POINT(0 5)@2020-01-01 00:00:00+00, POINT(100 5)@2020-01-01 00:10:00+00]')),
              (3, 3, tgeompoint('[POINT(0 3)@2020-01-01 00:00:00+00, POINT(100 3)@2020-01-01 00:10:00+00]')),
              (4, 4, tgeompoint('[POINT(0 4)@2020-01-01 00:00:00+00, POINT(100 4)@2020-01-01 00:10:00+00]')),
              (5, 5, tgeompoint('[POINT(1000 1000)@2020-01-01 00:00:00+00, POINT(2000 1000)@2020-01-01 00:10:00+00]'))
            AS t(tripId, vehId, trip)
            """);

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
            ORDER  BY licence1, licence2
            """);
        result.show();
        return result;
    }

    // ------------------------------------------------------------------
    // Cross-platform verification: compare against expected CSV files.
    // All queries (Q1–Q8, QRT) are compared.  Q3/Q7/QRT use asHexWKB()
    // for temporal types; Q8 uses geo_as_hexewkb() for geometry — all
    // produce byte-for-byte identical output across all three platforms.
    // ------------------------------------------------------------------
    private static void verify(SparkSession spark,
                                Dataset<Row> q1,  Dataset<Row> q2,
                                Dataset<Row> q3,  Dataset<Row> q4,
                                Dataset<Row> q5,  Dataset<Row> q6,
                                Dataset<Row> q7,  Dataset<Row> q8,
                                Dataset<Row> qrt,
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
