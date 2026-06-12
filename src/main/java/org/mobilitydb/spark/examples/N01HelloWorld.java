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

import org.apache.spark.sql.SparkSession;
import org.mobilitydb.spark.MobilitySparkSession;

import static functions.GeneratedFunctions.*;

/**
 * N01 Hello World — the minimal MobilitySpark program.
 *
 * Creates a single tgeompoint value inside Spark SQL, rounds it back to
 * text, and prints it. Mirrors meos/examples/01_hello_world.c.
 *
 * Run with:
 *   spark-submit --class org.mobilitydb.spark.examples.N01HelloWorld \
 *       target/mobilityspark-0.1.0-SNAPSHOT-spark.jar
 */
public final class N01HelloWorld {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .appName("MobilitySpark N01 Hello World")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {

            // Build a tgeompoint sequence and round-trip through hex-WKB.
            String wkt = "[POINT(1 1)@2020-01-01 00:00:00+00, "
                       + "POINT(2 2)@2020-01-01 01:00:00+00]";
            String hex = temporal_as_hexwkb(tgeompoint_in(wkt), (byte) 0);

            // Register a simple view so we can use Spark SQL.
            spark.sql("CREATE OR REPLACE TEMPORARY VIEW trips AS "
                    + "SELECT '" + hex + "' AS trip");

            System.out.println("=== Hello World: tgeompoint round-trip ===");
            spark.sql("SELECT trip FROM trips").show(false);

        } finally {
            spark.stop();
        }
    }
}
