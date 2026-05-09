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
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for temporal comparisons and ever/always predicate lifting.
 *
 * Temporal order comparison (temporalEq, temporalLt, …) compares two temporal
 * values lexicographically by their sequence of instants.
 *
 * Ever/always predicates follow the MEOS convention of returning int:
 *   1 = predicate holds for every/some instant, 0 = it does not, -1 = error.
 * All UDFs here convert that int to Boolean (null for error).
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class PredicateUDFs {

    private PredicateUDFs() {}

    private static Pointer tempPtr(String hex) {
        return hex == null ? null : functions.temporal_from_hexwkb(hex);
    }

    /** Convert MEOS ever/always int result to Boolean; null on error (-1). */
    private static Boolean intToBool(int v) {
        if (v < 0) return null;
        return v != 0;
    }

    // ------------------------------------------------------------------
    // Temporal order comparisons  (hex, hex) → Boolean
    //
    // These compare two temporal values as ordered objects (lexicographic
    // by instant sequence), not instant-by-instant.
    //
    // MEOS: temporal_eq / temporal_ne / temporal_lt / temporal_le
    //       temporal_gt / temporal_ge
    // ------------------------------------------------------------------

    // temporalEq(t1, t2) → true if t1 and t2 have identical instant sequences
    public static final UDF2<String, String, Boolean> temporalEq =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.temporal_eq(p1, p2);
        };

    // temporalNe(t1, t2) → true if t1 and t2 differ
    public static final UDF2<String, String, Boolean> temporalNe =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.temporal_ne(p1, p2);
        };

    // temporalLt(t1, t2) → true if t1 < t2 (lexicographic)
    public static final UDF2<String, String, Boolean> temporalLt =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.temporal_lt(p1, p2);
        };

    // temporalLe(t1, t2) → true if t1 ≤ t2
    public static final UDF2<String, String, Boolean> temporalLe =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.temporal_le(p1, p2);
        };

    // temporalGt(t1, t2) → true if t1 > t2
    public static final UDF2<String, String, Boolean> temporalGt =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.temporal_gt(p1, p2);
        };

    // temporalGe(t1, t2) → true if t1 ≥ t2
    public static final UDF2<String, String, Boolean> temporalGe =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.temporal_ge(p1, p2);
        };

    // ------------------------------------------------------------------
    // ever_eq predicates: did the temporal value ever equal the scalar?
    //
    // MEOS: ever_eq_tint_int / ever_eq_tfloat_float
    //       ever_eq_temporal_temporal
    // ------------------------------------------------------------------

    // everEqTintInt(tint_hex, 5) → true if tint ever equals 5
    // MEOS: ever_eq_tint_int(Temporal *, int) → int
    public static final UDF2<String, Integer, Boolean> everEqTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_eq_tint_int(ptr, v));
        };

    // everEqTfloatFloat(tfloat_hex, 1.5) → true if tfloat ever equals 1.5
    // MEOS: ever_eq_tfloat_float(Temporal *, double) → int
    public static final UDF2<String, Double, Boolean> everEqTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_eq_tfloat_float(ptr, v));
        };

    // everEqTemporal(t1, t2) → true if t1 and t2 ever have the same value
    // MEOS: ever_eq_temporal_temporal(Temporal *, Temporal *) → int
    public static final UDF2<String, String, Boolean> everEqTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.ever_eq_temporal_temporal(p1, p2));
        };

    // ------------------------------------------------------------------
    // ever_lt predicates
    //
    // MEOS: ever_lt_tint_int / ever_lt_tfloat_float / ever_lt_temporal_temporal
    // ------------------------------------------------------------------

    // everLtTintInt(tint_hex, 10) → true if tint ever < 10
    public static final UDF2<String, Integer, Boolean> everLtTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_lt_tint_int(ptr, v));
        };

    // everLtTfloatFloat(tfloat_hex, 2.0) → true if tfloat ever < 2.0
    public static final UDF2<String, Double, Boolean> everLtTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_lt_tfloat_float(ptr, v));
        };

    // everLtTemporal(t1, t2) → true if t1 ever < t2 at any shared instant
    public static final UDF2<String, String, Boolean> everLtTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.ever_lt_temporal_temporal(p1, p2));
        };

    // ------------------------------------------------------------------
    // ever_le predicates
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, Boolean> everLeTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_le_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> everLeTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_le_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> everLeTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.ever_le_temporal_temporal(p1, p2));
        };

    // ------------------------------------------------------------------
    // ever_gt / ever_ge predicates
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, Boolean> everGtTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_gt_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> everGtTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_gt_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> everGtTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.ever_gt_temporal_temporal(p1, p2));
        };

    public static final UDF2<String, Integer, Boolean> everGeTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_ge_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> everGeTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_ge_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> everGeTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.ever_ge_temporal_temporal(p1, p2));
        };

    // ------------------------------------------------------------------
    // always_eq predicates
    //
    // MEOS: always_eq_tint_int / always_eq_tfloat_float
    //       always_eq_temporal_temporal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, Boolean> alwaysEqTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_eq_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> alwaysEqTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_eq_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> alwaysEqTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.always_eq_temporal_temporal(p1, p2));
        };

    // ------------------------------------------------------------------
    // always_lt predicates
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, Boolean> alwaysLtTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_lt_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> alwaysLtTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_lt_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> alwaysLtTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.always_lt_temporal_temporal(p1, p2));
        };

    // ------------------------------------------------------------------
    // always_le predicates
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, Boolean> alwaysLeTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_le_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> alwaysLeTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_le_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> alwaysLeTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.always_le_temporal_temporal(p1, p2));
        };

    // ------------------------------------------------------------------
    // always_gt / always_ge predicates
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, Boolean> alwaysGtTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_gt_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> alwaysGtTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_gt_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> alwaysGtTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.always_gt_temporal_temporal(p1, p2));
        };

    public static final UDF2<String, Integer, Boolean> alwaysGeTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_ge_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> alwaysGeTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_ge_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> alwaysGeTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.always_ge_temporal_temporal(p1, p2));
        };

    public static void registerAll(SparkSession spark) {
        // Temporal order comparisons
        spark.udf().register("temporalEq",           temporalEq,           DataTypes.BooleanType);
        spark.udf().register("temporalNe",           temporalNe,           DataTypes.BooleanType);
        spark.udf().register("temporalLt",           temporalLt,           DataTypes.BooleanType);
        spark.udf().register("temporalLe",           temporalLe,           DataTypes.BooleanType);
        spark.udf().register("temporalGt",           temporalGt,           DataTypes.BooleanType);
        spark.udf().register("temporalGe",           temporalGe,           DataTypes.BooleanType);
        // ever_eq
        spark.udf().register("everEqTintInt",        everEqTintInt,        DataTypes.BooleanType);
        spark.udf().register("everEqTfloatFloat",    everEqTfloatFloat,    DataTypes.BooleanType);
        spark.udf().register("everEqTemporal",       everEqTemporal,       DataTypes.BooleanType);
        // ever_lt
        spark.udf().register("everLtTintInt",        everLtTintInt,        DataTypes.BooleanType);
        spark.udf().register("everLtTfloatFloat",    everLtTfloatFloat,    DataTypes.BooleanType);
        spark.udf().register("everLtTemporal",       everLtTemporal,       DataTypes.BooleanType);
        // ever_le
        spark.udf().register("everLeTintInt",        everLeTintInt,        DataTypes.BooleanType);
        spark.udf().register("everLeTfloatFloat",    everLeTfloatFloat,    DataTypes.BooleanType);
        spark.udf().register("everLeTemporal",       everLeTemporal,       DataTypes.BooleanType);
        // ever_gt
        spark.udf().register("everGtTintInt",        everGtTintInt,        DataTypes.BooleanType);
        spark.udf().register("everGtTfloatFloat",    everGtTfloatFloat,    DataTypes.BooleanType);
        spark.udf().register("everGtTemporal",       everGtTemporal,       DataTypes.BooleanType);
        // ever_ge
        spark.udf().register("everGeTintInt",        everGeTintInt,        DataTypes.BooleanType);
        spark.udf().register("everGeTfloatFloat",    everGeTfloatFloat,    DataTypes.BooleanType);
        spark.udf().register("everGeTemporal",       everGeTemporal,       DataTypes.BooleanType);
        // always_eq
        spark.udf().register("alwaysEqTintInt",      alwaysEqTintInt,      DataTypes.BooleanType);
        spark.udf().register("alwaysEqTfloatFloat",  alwaysEqTfloatFloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysEqTemporal",     alwaysEqTemporal,     DataTypes.BooleanType);
        // always_lt
        spark.udf().register("alwaysLtTintInt",      alwaysLtTintInt,      DataTypes.BooleanType);
        spark.udf().register("alwaysLtTfloatFloat",  alwaysLtTfloatFloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysLtTemporal",     alwaysLtTemporal,     DataTypes.BooleanType);
        // always_le
        spark.udf().register("alwaysLeTintInt",      alwaysLeTintInt,      DataTypes.BooleanType);
        spark.udf().register("alwaysLeTfloatFloat",  alwaysLeTfloatFloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysLeTemporal",     alwaysLeTemporal,     DataTypes.BooleanType);
        // always_gt
        spark.udf().register("alwaysGtTintInt",      alwaysGtTintInt,      DataTypes.BooleanType);
        spark.udf().register("alwaysGtTfloatFloat",  alwaysGtTfloatFloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysGtTemporal",     alwaysGtTemporal,     DataTypes.BooleanType);
        // always_ge
        spark.udf().register("alwaysGeTintInt",      alwaysGeTintInt,      DataTypes.BooleanType);
        spark.udf().register("alwaysGeTfloatFloat",  alwaysGeTfloatFloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysGeTemporal",     alwaysGeTemporal,     DataTypes.BooleanType);
    }
}
