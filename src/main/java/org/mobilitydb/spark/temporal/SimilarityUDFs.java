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
import jnr.ffi.Pointer;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for trajectory similarity measures.
 *
 * Both measures operate on any pair of temporal values with the same base type
 * (tgeompoint × tgeompoint, tfloat × tfloat, etc.) and return a scalar Double.
 *
 * MEOS function authority: meos/include/meos.h (038_temporal_similarity)
 */
public final class SimilarityUDFs {

    private SimilarityUDFs() {}

    // ------------------------------------------------------------------
    // frechetDistance(t1 STRING, t2 STRING) → DOUBLE
    //
    // Discrete Fréchet distance between two temporal trajectories.
    // Returns null when the inputs have incompatible types or no instants.
    //
    // MEOS: temporal_frechet_distance(const Temporal *, const Temporal *) → double
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Double> frechetDistance =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    double d = functions.temporal_frechet_distance(p1, p2);
                    return (d == Double.MAX_VALUE) ? null : d;
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // dynamicTimeWarp(t1 STRING, t2 STRING) → DOUBLE
    //
    // Dynamic Time Warping distance between two temporal trajectories.
    // Returns null when the inputs have incompatible types or no instants.
    //
    // MEOS: temporal_dyntimewarp_distance(const Temporal *, const Temporal *) → double
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Double> dynamicTimeWarp =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    double d = functions.temporal_dyntimewarp_distance(p1, p2);
                    return (d == Double.MAX_VALUE) ? null : d;
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // REGISTRATION
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        spark.udf().register("frechetDistance",  frechetDistance,  DataTypes.DoubleType);
        spark.udf().register("dynamicTimeWarp",  dynamicTimeWarp,  DataTypes.DoubleType);
    }
}
