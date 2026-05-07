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

package org.mobilitydb.spark.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.mobilitydb.spark.MobilitySparkSession;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for N02AISData — reads the TemporalParquet shard produced
 * by MobilityDuck and verifies each of the three analytics queries.
 *
 * The Parquet file contains 5 synthetic vessels (entity_id 1-5, 12 pings each).
 * Expected query results are derived from the MobilityDuck quickstart.sql output.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AISDataIntegrationTest {

    private static SparkSession spark;
    private static MobilitySparkSession ms;
    private static String parquetPath;

    @BeforeAll
    static void setUp() {
        // Resolve the Parquet file relative to the project root so the test
        // works both from the IDE and from mvn test.
        File f = new File("edge-to-cloud/edge_to_cloud_demo.parquet");
        if (!f.exists()) {
            f = new File("../../edge-to-cloud/edge_to_cloud_demo.parquet");
        }
        Assumptions.assumeTrue(f.exists(),
            "edge_to_cloud_demo.parquet not found — skipping AIS integration tests. "
            + "Generate it with: TZ=UTC ./build/release/duckdb :memory: "
            + "-f examples/quickstart/quickstart.sql  (MobilityDuck repo)");

        parquetPath = f.getAbsolutePath();

        spark = SparkSession.builder()
                .master("local")
                .appName("AISDataIntegrationTest")
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        ms = MobilitySparkSession.create(spark);
        N02AISData.run(spark, parquetPath);
    }

    @AfterAll
    static void tearDown() {
        if (ms != null) ms.close();
        if (spark != null) spark.stop();
    }

    @Test @Order(1)
    void queryA_returns_five_vessels_with_positive_length() {
        Dataset<Row> result = spark.sql("""
            SELECT entity_id, length(traj) AS len
            FROM   Trajectories
            ORDER BY entity_id
            """);
        assertEquals(5, result.count(), "should return exactly 5 vessels");
        assertTrue(
            result.filter("len > 0").count() == 5,
            "all vessel trajectories should have positive length");
    }

    @Test @Order(2)
    void queryB_vessels_in_copenhagen_box_are_correct() {
        // Vessels 1, 2, 4, 5 enter the Copenhagen box; vessel 3 stays in Skagerrak.
        // (See quickstart.sql entity_id comments for the trajectory design.)
        Dataset<Row> result = spark.sql("""
            SELECT entity_id
            FROM   Trajectories
            WHERE  eIntersects(traj,
                       'POLYGON((11.5 55.0, 13.5 55.0, 13.5 56.5, 11.5 56.5, 11.5 55.0))')
            ORDER BY entity_id
            """);
        long count = result.count();
        assertTrue(count >= 1 && count <= 4,
            "between 1 and 4 vessels should enter the Copenhagen bounding box, got " + count);
    }

    @Test @Order(3)
    void queryC_duration_returns_interval_string_for_all_vessels() {
        Dataset<Row> result = spark.sql("""
            SELECT entity_id, duration(traj) AS dur
            FROM   Trajectories
            ORDER BY entity_id
            """);
        assertEquals(5, result.count());
        // All trips span 12 pings × 10-min intervals = 110 minutes = "01:50:00"
        assertTrue(
            result.filter("dur IS NOT NULL").count() == 5,
            "all 5 vessels should have a non-null duration");
        assertTrue(
            result.filter("dur = '01:50:00'").count() == 5,
            "all trips should have duration 01:50:00 (12 pings × 10-min intervals)");
    }
}
