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
import org.junit.jupiter.api.*;
import org.mobilitydb.spark.MobilitySparkSession;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for BerlinMOD queries — runs end-to-end through a local
 * SparkSession (no spark-submit required).  MEOS is initialised via
 * MobilitySparkSession which also registers all UDFs.
 *
 * These tests verify that the UDFs wire correctly into Spark SQL and that
 * real BerlinMOD query plans execute without errors on synthetic data.
 * Detailed UDF correctness is covered by GeoUDFsTest and TemporalUDFsTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BerlinMODIntegrationTest {

    private static SparkSession spark;
    private static MobilitySparkSession ms;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
                .master("local")
                .appName("BerlinMODIntegrationTest")
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        ms = MobilitySparkSession.create(spark);
        loadSynthetic(spark);
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    // Q1: join on licence — pure SQL, no UDFs; validates table loading
    @Test @Order(1)
    void q1_vehicle_models_for_query_licences() {
        Dataset<Row> result = spark.sql("""
            SELECT l.licence, v.model
            FROM   QueryLicences l
            JOIN   Vehicles v ON v.licence = l.licence
            ORDER  BY l.licence
            """);
        long count = result.count();
        assertEquals(2, count, "Q1 should return 2 rows (one per QueryLicence)");
    }

    // Q5: nearestApproachDistance — exercises the NAD UDF end-to-end through Spark
    @Test @Order(2)
    void q5_min_nearest_approach_distance_between_query_vehicles() {
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
            """);
        long count = result.count();
        assertTrue(count > 0, "Q5 should find at least one vehicle pair with a finite distance");
        Row first = result.first();
        Double dist = first.getAs("min_dist");
        assertNotNull(dist);
        assertTrue(dist >= 0.0, "min_dist should be non-negative, got " + dist);
    }

    // QRT: asHexWKB round-trip — verifies MEOS binary serialization through Spark
    @Test @Order(3)
    void qrt_hexwkb_roundtrip_all_trips() {
        Dataset<Row> result = spark.sql("""
            SELECT tripId AS tripid,
                   asHexWKB(trip) AS trip_hexwkb
            FROM   Trips
            ORDER  BY tripId
            """);
        long count = result.count();
        assertEquals(5, count, "QRT should return one row per trip (5 total)");
        result.foreach(row -> {
            String hexwkb = row.getAs("trip_hexwkb");
            assertNotNull(hexwkb, "asHexWKB should not return null");
            assertFalse(hexwkb.isBlank(), "asHexWKB should return non-empty hex string");
        });
    }

    // ------------------------------------------------------------------
    // Synthetic data — mirrors the small dataset in BerlinMODDemo
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
    }
}
