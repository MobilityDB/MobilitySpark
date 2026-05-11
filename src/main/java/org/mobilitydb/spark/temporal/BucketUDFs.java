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

package org.mobilitydb.spark.temporal;

import functions.functions;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for bucketing scalar values onto a regular grid — used to
 * implement time-windowed aggregations and value histograms.
 *
 * MEOS: float_bucket / int_bucket round their input down to the nearest
 * bucket boundary, given a bucket size and an origin offset.
 *
 *   floatBucket(7.3, 1.0, 0.0) = 7.0      // bucket [7.0, 8.0)
 *   intBucket(17,    5,   0)   = 15       // bucket [15, 20)
 */
public final class BucketUDFs {

    private BucketUDFs() {}

    // floatBucket(value DOUBLE, size DOUBLE, origin DOUBLE) → DOUBLE
    // MEOS: float_get_bin (renamed from float_bucket; not in JMEOS-1.4)
    public static final UDF3<Double, Double, Double, Double> floatBucket =
        (value, size, origin) -> {
            if (value == null || size == null || origin == null) return null;
            MeosThread.ensureReady();
            return functions.float_get_bin(value, size, origin);
        };

    // intBucket(value INT, size INT, origin INT) → INT
    // MEOS: int_get_bin (renamed from int_bucket; not in JMEOS-1.4)
    public static final UDF3<Integer, Integer, Integer, Integer> intBucket =
        (value, size, origin) -> {
            if (value == null || size == null || origin == null) return null;
            MeosThread.ensureReady();
            return functions.int_get_bin(value, size, origin);
        };

    public static void registerAll(SparkSession spark) {
        spark.udf().register("floatBucket", floatBucket, DataTypes.DoubleType);
        spark.udf().register("intBucket",   intBucket,   DataTypes.IntegerType);
    }
}
