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

package org.mobilitydb.spark;

import org.apache.spark.sql.SparkSession;
import org.mobilitydb.spark.geo.GeoUDFs;
import org.mobilitydb.spark.temporal.TemporalUDFs;

import static functions.functions.*;

/**
 * Entry point for MobilitySpark.
 *
 * Initialises MEOS and registers all UDFs with the given SparkSession.
 * Call {@link #create(SparkSession)} before running any temporal SQL,
 * and {@link #close()} after (or use try-with-resources).
 *
 * <pre>{@code
 *   SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
 *   try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
 *       spark.sql("SELECT atTime(trip, instant) FROM trips").show();
 *   }
 * }</pre>
 */
public final class MobilitySparkSession implements AutoCloseable {

    private MobilitySparkSession() {}

    public static MobilitySparkSession create(SparkSession spark) {
        meos_initialize();
        meos_initialize_timezone("UTC");
        TemporalUDFs.registerAll(spark);
        GeoUDFs.registerAll(spark);
        return new MobilitySparkSession();
    }

    @Override
    public void close() {
        meos_finalize();
    }
}
