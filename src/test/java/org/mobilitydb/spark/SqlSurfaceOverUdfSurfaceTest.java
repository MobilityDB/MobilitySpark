/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby retained provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS
 * TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.spark;

import functions.GeneratedFunctions;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mobilitydb.spark.generated.GeneratedSpatioTemporalUDFs;
import org.mobilitydb.spark.sql.MobilitySparkSql;
import org.mobilitydb.spark.sql.types.TGeomPoint;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The typed Spark SQL surface registered over the UDF surface in one session, as a query that
 * reads values in both forms registers them. Mirrors #GeneratedSqlSurfaceTest: a name both
 * surfaces carry still widens a number to the typed overload before the call falls back to the
 * UDF registered first.
 */
class SqlSurfaceOverUdfSurfaceTest {

    private static SparkSession spark;
    private static String trip;

    @BeforeAll
    static void init() {
        // No-op error handler so a parse error returns rather than terminating the JVM.
        GeneratedFunctions.meos_initialize_error_handler((level, code, message) -> { });
        GeneratedFunctions.meos_initialize();
        trip = "tgeompointFromHexEWKB('" + TGeomPoint.encode(GeneratedFunctions.tgeompoint_in(
                "[Point(1 1)@2020-01-01 00:00:00+00, Point(2 2)@2020-01-02 00:00:00+00]")) + "')";
        spark = SparkSession.builder().appName("sql-over-udf-surface").master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.datetime.java8API.enabled", "true")
                .getOrCreate();
        GeneratedSpatioTemporalUDFs.registerAll(spark);
        MobilitySparkSql.registerAll(spark);
    }

    @AfterAll
    static void finalizeMeos() {
        if (spark != null) {
            spark.stop();
        }
        GeneratedFunctions.meos_finalize();
    }

    private static Object scalar(String sql) {
        return spark.sql(sql).collectAsList().get(0).get(0);
    }

    @Test
    void anIntegerDistanceWidensToTheTypedOverload() {
        // eDwithin(tgeompoint, tgeompoint, double): 10 is an int literal, which the UDF of the
        // same name registered first takes as it stands and cannot cast to its double
        assertEquals(true, scalar("SELECT eDwithin(" + trip + ", " + trip + ", 10)"));
        assertEquals(true, scalar("SELECT eDwithin(" + trip + ", " + trip + ", 10.0)"));
    }
}
