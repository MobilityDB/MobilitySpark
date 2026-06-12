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

import functions.GeneratedFunctions;
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
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    double d = GeneratedFunctions.temporal_frechet_distance(p1, p2);
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
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    double d = GeneratedFunctions.temporal_dyntimewarp_distance(p1, p2);
                    return (d == Double.MAX_VALUE) ? null : d;
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // hausdorffDistance(t1 STRING, t2 STRING) → DOUBLE
    //
    // Hausdorff distance between two temporal trajectories.
    // Returns null when the inputs have incompatible types or no instants.
    //
    // MEOS: temporal_hausdorff_distance(const Temporal *, const Temporal *) → double
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Double> hausdorffDistance =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    double d = GeneratedFunctions.temporal_hausdorff_distance(p1, p2);
                    return (d == Double.MAX_VALUE) ? null : d;
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // REGISTRATION
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        spark.udf().register("frechetDistance",    frechetDistance,    DataTypes.DoubleType);
        spark.udf().register("dynamicTimeWarp",    dynamicTimeWarp,    DataTypes.DoubleType);
        spark.udf().register("hausdorffDistance",  hausdorffDistance,  DataTypes.DoubleType);
        // MobilityDB SQL bare-name alias for dynamicTimeWarp
        spark.udf().register("dynTimeWarpDistance", dynamicTimeWarp,   DataTypes.DoubleType);
        // Similarity paths — return array of "i,j" pairs as Strings
        spark.udf().register("dynTimeWarpPath",   dynTimeWarpPath,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("frechetDistancePath", frechetDistancePath, DataTypes.createArrayType(DataTypes.StringType));
    }

    // dynTimeWarpPath / frechetDistancePath: return array of "i,j" pairs

    public static final org.apache.spark.sql.api.java.UDF2<String, String, String[]> dynTimeWarpPath =
        (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            org.mobilitydb.spark.MeosThread.ensureReady();
            jnr.ffi.Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(h1);
            if (p1 == null) return null;
            jnr.ffi.Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(h2);
            if (p2 == null) { org.mobilitydb.spark.MeosMemory.free(p1); return null; }
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                jnr.ffi.Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                jnr.ffi.Pointer arr = org.mobilitydb.spark.MeosNative.INSTANCE
                    .temporal_dyntimewarp_path(p1, p2, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) {
                        // Match = {int i, int j} — 8 bytes each
                        int mi = arr.getInt(i * 8L);
                        int mj = arr.getInt(i * 8L + 4);
                        out[i] = mi + "," + mj;
                    }
                    return out;
                } finally { org.mobilitydb.spark.MeosMemory.free(arr); }
            } finally { org.mobilitydb.spark.MeosMemory.free(p1, p2); }
        };

    public static final org.apache.spark.sql.api.java.UDF2<String, String, String[]> frechetDistancePath =
        (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            org.mobilitydb.spark.MeosThread.ensureReady();
            jnr.ffi.Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(h1);
            if (p1 == null) return null;
            jnr.ffi.Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(h2);
            if (p2 == null) { org.mobilitydb.spark.MeosMemory.free(p1); return null; }
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                jnr.ffi.Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                jnr.ffi.Pointer arr = org.mobilitydb.spark.MeosNative.INSTANCE
                    .temporal_frechet_path(p1, p2, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) {
                        int mi = arr.getInt(i * 8L);
                        int mj = arr.getInt(i * 8L + 4);
                        out[i] = mi + "," + mj;
                    }
                    return out;
                } finally { org.mobilitydb.spark.MeosMemory.free(arr); }
            } finally { org.mobilitydb.spark.MeosMemory.free(p1, p2); }
        };
}
