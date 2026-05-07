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

/**
 * N02 AIS Data Lake — cloud analytics on a TemporalParquet shard.
 *
 * Reads the {@code edge_to_cloud_demo.parquet} written by MobilityDuck's
 * {@code examples/quickstart/quickstart.sql} and runs three analytics queries
 * using the same named-function SQL dialect as MobilityDB and MobilityDuck.
 *
 * <p>The TemporalParquet encoding convention:
 * <ul>
 *   <li>MobilityDuck writes the {@code traj} column as a Parquet
 *       {@code BYTE_ARRAY} via {@code asBinary(traj)}.
 *   <li>Spark reads it as {@code BinaryType} ({@code byte[]}).
 *   <li>{@code tgeompointFromBinary(traj)} decodes the MEOS-WKB bytes
 *       into the hex-WKB STRING used by all other UDFs.
 * </ul>
 *
 * <p>Queries mirror {@code quickstart.sql} (MobilityDuck) and
 * {@code quickstart_mobilitydb.sql} (PostgreSQL/MobilityDB):
 * <ul>
 *   <li>Query A: total path length + peak speed per vessel
 *   <li>Query B: vessels that entered the Copenhagen bounding box
 *   <li>Query C: trip duration per vessel
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   spark-submit --class org.mobilitydb.spark.examples.N02AISData \
 *       target/mobilityspark-0.1.0-SNAPSHOT-spark.jar \
 *       [path/to/edge_to_cloud_demo.parquet]
 * </pre>
 * If no path is given, the file is looked up relative to the working directory.
 *
 * <p>To generate the Parquet shard, run MobilityDuck's quickstart first:
 * <pre>
 *   TZ=UTC ./build/release/duckdb :memory: \
 *       -f examples/quickstart/quickstart.sql
 * </pre>
 */
public final class N02AISData {

    private static final String COPENHAGEN_BOX =
        "POLYGON((11.5 55.0, 13.5 55.0, 13.5 56.5, 11.5 56.5, 11.5 55.0))";

    public static void main(String[] args) {
        String parquetPath = args.length > 0 ? args[0] : "edge_to_cloud_demo.parquet";

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("MobilitySpark N02 AIS Data Lake")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
            run(spark, parquetPath);
        } finally {
            spark.stop();
        }
    }

    static void run(SparkSession spark, String parquetPath) {
        // Decode the MEOS-WKB binary column once into a temp view so each
        // query pays the decoding cost exactly once.
        spark.read().parquet(parquetPath).createOrReplaceTempView("ParquetRaw");
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW Trajectories AS
            SELECT entity_id,
                   ping_count,
                   tgeompointFromBinary(traj) AS traj
            FROM   ParquetRaw
            """).count(); // force evaluation

        // ─────────────────────────────────────────────────────────────────
        // Query A: total path length and peak instantaneous speed per vessel
        // length()   → Euclidean distance in coordinate units (degrees for
        //              SRID 0 from MobilityDuck's tgeompoint; metres for
        //              geodetic tgeogpoint when SRID 4326 is set).
        // maxSpeed() → peak speed in the same units/second.
        // ─────────────────────────────────────────────────────────────────
        System.out.println("\n=== Query A: path length and peak speed per vessel ===");
        spark.sql("""
            SELECT entity_id,
                   ping_count,
                   ROUND(CAST(length(traj) AS DECIMAL(20, 6)), 6) AS length,
                   ROUND(CAST(maxSpeed(traj) AS DECIMAL(20, 8)), 8) AS max_speed
            FROM   Trajectories
            ORDER BY length DESC
            """).show();

        // ─────────────────────────────────────────────────────────────────
        // Query B: vessels that entered the Copenhagen bounding box
        // Approximate Øresund / Danish straits region:
        //   lon 11.5–13.5, lat 55.0–56.5
        // eIntersects(trip, geomWkt) → true if the trip was ever inside.
        // ─────────────────────────────────────────────────────────────────
        System.out.println("=== Query B: vessels that entered the Copenhagen bounding box ===");
        spark.sql("""
            SELECT entity_id
            FROM   Trajectories
            WHERE  eIntersects(traj, '""" + COPENHAGEN_BOX + """
            ')
            ORDER BY entity_id
            """).show();

        // ─────────────────────────────────────────────────────────────────
        // Query C: trip duration per vessel
        // duration() → interval text (e.g. "01:50:00") via MEOS temporal_duration
        //              + pg_interval_out.
        // ─────────────────────────────────────────────────────────────────
        System.out.println("=== Query C: trip duration per vessel ===");
        spark.sql("""
            SELECT entity_id,
                   duration(traj) AS trip_duration
            FROM   Trajectories
            ORDER BY entity_id
            """).show();
    }
}
