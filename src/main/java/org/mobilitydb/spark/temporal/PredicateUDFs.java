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
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import java.util.function.BiFunction;

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
    // ever_ne predicates: did the temporal value ever differ from the scalar?
    //
    // MEOS: ever_ne_tint_int / ever_ne_tfloat_float / ever_ne_temporal_temporal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, Boolean> everNeTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_ne_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> everNeTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.ever_ne_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> everNeTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.ever_ne_temporal_temporal(p1, p2));
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
    // always_ne predicates
    //
    // MEOS: always_ne_tint_int / always_ne_tfloat_float / always_ne_temporal_temporal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, Boolean> alwaysNeTintInt =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_ne_tint_int(ptr, v));
        };

    public static final UDF2<String, Double, Boolean> alwaysNeTfloatFloat =
        (s, v) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            return intToBool(functions.always_ne_tfloat_float(ptr, v));
        };

    public static final UDF2<String, String, Boolean> alwaysNeTemporal =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            return intToBool(functions.always_ne_temporal_temporal(p1, p2));
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

    // ------------------------------------------------------------------
    // Scalar-first reversed forms: (int OP tint), (float OP tfloat)
    // MEOS: always_eq_int_tint(int, Temporal *) → int, etc.
    // ------------------------------------------------------------------

    public static final UDF2<Integer, String, Boolean> alwaysEqIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_eq_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> alwaysNeIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_ne_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> alwaysLtIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_lt_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> alwaysLeIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_le_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> alwaysGtIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_gt_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> alwaysGeIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_ge_int_tint(v, p)); };

    public static final UDF2<Double, String, Boolean> alwaysEqFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_eq_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> alwaysNeFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_ne_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> alwaysLtFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_lt_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> alwaysLeFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_le_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> alwaysGtFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_gt_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> alwaysGeFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_ge_float_tfloat(v, p)); };

    public static final UDF2<Integer, String, Boolean> everEqIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_eq_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> everNeIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_ne_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> everLtIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_lt_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> everLeIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_le_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> everGtIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_gt_int_tint(v, p)); };
    public static final UDF2<Integer, String, Boolean> everGeIntTint =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_ge_int_tint(v, p)); };

    public static final UDF2<Double, String, Boolean> everEqFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_eq_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> everNeFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_ne_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> everLtFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_lt_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> everLeFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_le_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> everGtFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_gt_float_tfloat(v, p)); };
    public static final UDF2<Double, String, Boolean> everGeFloatTfloat =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_ge_float_tfloat(v, p)); };

    // ------------------------------------------------------------------
    // tbool × bool predicates (only eq and ne meaningful for booleans)
    // MEOS: ever_eq_tbool_bool(Temporal *, bool) → int, etc.
    // ------------------------------------------------------------------

    public static final UDF2<String, Boolean, Boolean> alwaysEqTboolBool =
        (s, v) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_eq_tbool_bool(p, v)); };
    public static final UDF2<String, Boolean, Boolean> alwaysNeTboolBool =
        (s, v) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_ne_tbool_bool(p, v)); };
    public static final UDF2<Boolean, String, Boolean> alwaysEqBoolTbool =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_eq_bool_tbool(v, p)); };
    public static final UDF2<Boolean, String, Boolean> alwaysNeBoolTbool =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.always_ne_bool_tbool(v, p)); };

    public static final UDF2<String, Boolean, Boolean> everEqTboolBool =
        (s, v) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_eq_tbool_bool(p, v)); };
    public static final UDF2<String, Boolean, Boolean> everNeTboolBool =
        (s, v) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_ne_tbool_bool(p, v)); };
    public static final UDF2<Boolean, String, Boolean> everEqBoolTbool =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_eq_bool_tbool(v, p)); };
    public static final UDF2<Boolean, String, Boolean> everNeBoolTbool =
        (v, s) -> { MeosThread.ensureReady(); Pointer p = tempPtr(s); if (p == null || v == null) return null; return intToBool(functions.ever_ne_bool_tbool(v, p)); };

    // ------------------------------------------------------------------
    // ttext × text predicates
    // text* is obtained via ttext_in + ttext_value_n (text_in not exposed).
    // MEOS: always_eq_text_ttext(text *, Temporal *) → int, etc.
    // ------------------------------------------------------------------

    private static Pointer[] makeTextPtr(String val) {
        Pointer dummy = functions.ttext_in(val + "@2000-01-01 00:00:00+00");
        if (dummy == null) return null;
        Pointer textPtr = functions.ttext_value_n(dummy, 1);
        if (textPtr == null) { MeosMemory.free(dummy); return null; }
        return new Pointer[]{textPtr, dummy};
    }

    private static Boolean textTtextPred(String textVal, String ttextHex,
            BiFunction<Pointer, Pointer, Integer> fn) {
        if (textVal == null || ttextHex == null) return null;
        MeosThread.ensureReady();
        Pointer[] tp = makeTextPtr(textVal);
        if (tp == null) return null;
        Pointer tptr = functions.temporal_from_hexwkb(ttextHex);
        if (tptr == null) { MeosMemory.free(tp[0]); MeosMemory.free(tp[1]); return null; }
        try {
            return intToBool(fn.apply(tp[0], tptr));
        } finally {
            MeosMemory.free(tptr);
            MeosMemory.free(tp[0]);
            MeosMemory.free(tp[1]);
        }
    }

    private static Boolean ttextTextPred(String ttextHex, String textVal,
            BiFunction<Pointer, Pointer, Integer> fn) {
        if (ttextHex == null || textVal == null) return null;
        MeosThread.ensureReady();
        Pointer tptr = functions.temporal_from_hexwkb(ttextHex);
        if (tptr == null) return null;
        Pointer[] tp = makeTextPtr(textVal);
        if (tp == null) { MeosMemory.free(tptr); return null; }
        try {
            return intToBool(fn.apply(tptr, tp[0]));
        } finally {
            MeosMemory.free(tptr);
            MeosMemory.free(tp[0]);
            MeosMemory.free(tp[1]);
        }
    }

    // always: text OP ttext
    public static final UDF2<String, String, Boolean> alwaysEqTextTtext =
        (t, s) -> textTtextPred(t, s, functions::always_eq_text_ttext);
    public static final UDF2<String, String, Boolean> alwaysNeTextTtext =
        (t, s) -> textTtextPred(t, s, functions::always_ne_text_ttext);
    public static final UDF2<String, String, Boolean> alwaysLtTextTtext =
        (t, s) -> textTtextPred(t, s, functions::always_lt_text_ttext);
    public static final UDF2<String, String, Boolean> alwaysLeTextTtext =
        (t, s) -> textTtextPred(t, s, functions::always_le_text_ttext);
    public static final UDF2<String, String, Boolean> alwaysGtTextTtext =
        (t, s) -> textTtextPred(t, s, functions::always_gt_text_ttext);
    public static final UDF2<String, String, Boolean> alwaysGeTextTtext =
        (t, s) -> textTtextPred(t, s, functions::always_ge_text_ttext);

    // always: ttext OP text
    public static final UDF2<String, String, Boolean> alwaysEqTtextText =
        (s, t) -> ttextTextPred(s, t, functions::always_eq_ttext_text);
    public static final UDF2<String, String, Boolean> alwaysNeTtextText =
        (s, t) -> ttextTextPred(s, t, functions::always_ne_ttext_text);
    public static final UDF2<String, String, Boolean> alwaysLtTtextText =
        (s, t) -> ttextTextPred(s, t, functions::always_lt_ttext_text);
    public static final UDF2<String, String, Boolean> alwaysLeTtextText =
        (s, t) -> ttextTextPred(s, t, functions::always_le_ttext_text);
    public static final UDF2<String, String, Boolean> alwaysGtTtextText =
        (s, t) -> ttextTextPred(s, t, functions::always_gt_ttext_text);
    public static final UDF2<String, String, Boolean> alwaysGeTtextText =
        (s, t) -> ttextTextPred(s, t, functions::always_ge_ttext_text);

    // ever: text OP ttext
    public static final UDF2<String, String, Boolean> everEqTextTtext =
        (t, s) -> textTtextPred(t, s, functions::ever_eq_text_ttext);
    public static final UDF2<String, String, Boolean> everNeTextTtext =
        (t, s) -> textTtextPred(t, s, functions::ever_ne_text_ttext);
    public static final UDF2<String, String, Boolean> everLtTextTtext =
        (t, s) -> textTtextPred(t, s, functions::ever_lt_text_ttext);
    public static final UDF2<String, String, Boolean> everLeTextTtext =
        (t, s) -> textTtextPred(t, s, functions::ever_le_text_ttext);
    public static final UDF2<String, String, Boolean> everGtTextTtext =
        (t, s) -> textTtextPred(t, s, functions::ever_gt_text_ttext);
    public static final UDF2<String, String, Boolean> everGeTextTtext =
        (t, s) -> textTtextPred(t, s, functions::ever_ge_text_ttext);

    // ever: ttext OP text
    public static final UDF2<String, String, Boolean> everEqTtextText =
        (s, t) -> ttextTextPred(s, t, functions::ever_eq_ttext_text);
    public static final UDF2<String, String, Boolean> everNeTtextText =
        (s, t) -> ttextTextPred(s, t, functions::ever_ne_ttext_text);
    public static final UDF2<String, String, Boolean> everLtTtextText =
        (s, t) -> ttextTextPred(s, t, functions::ever_lt_ttext_text);
    public static final UDF2<String, String, Boolean> everLeTtextText =
        (s, t) -> ttextTextPred(s, t, functions::ever_le_ttext_text);
    public static final UDF2<String, String, Boolean> everGtTtextText =
        (s, t) -> ttextTextPred(s, t, functions::ever_gt_ttext_text);
    public static final UDF2<String, String, Boolean> everGeTtextText =
        (s, t) -> ttextTextPred(s, t, functions::ever_ge_ttext_text);

    // tpointIsSimple(tpoint_hex) → Boolean
    // Returns true if the trajectory has no self-intersections.
    // MEOS: tpoint_is_simple(const Temporal *) → bool
    public static final UDF1<String, Boolean> tpointIsSimple =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return functions.tpoint_is_simple(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
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
        // ever_ne
        spark.udf().register("everNeTintInt",        everNeTintInt,        DataTypes.BooleanType);
        spark.udf().register("everNeTfloatFloat",    everNeTfloatFloat,    DataTypes.BooleanType);
        spark.udf().register("everNeTemporal",       everNeTemporal,       DataTypes.BooleanType);
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
        // always_ne
        spark.udf().register("alwaysNeTintInt",      alwaysNeTintInt,      DataTypes.BooleanType);
        spark.udf().register("alwaysNeTfloatFloat",  alwaysNeTfloatFloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysNeTemporal",     alwaysNeTemporal,     DataTypes.BooleanType);
        // scalar-first reversed forms
        spark.udf().register("alwaysEqIntTint",      alwaysEqIntTint,      DataTypes.BooleanType);
        spark.udf().register("alwaysNeIntTint",      alwaysNeIntTint,      DataTypes.BooleanType);
        spark.udf().register("alwaysLtIntTint",      alwaysLtIntTint,      DataTypes.BooleanType);
        spark.udf().register("alwaysLeIntTint",      alwaysLeIntTint,      DataTypes.BooleanType);
        spark.udf().register("alwaysGtIntTint",      alwaysGtIntTint,      DataTypes.BooleanType);
        spark.udf().register("alwaysGeIntTint",      alwaysGeIntTint,      DataTypes.BooleanType);
        spark.udf().register("alwaysEqFloatTfloat",  alwaysEqFloatTfloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysNeFloatTfloat",  alwaysNeFloatTfloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysLtFloatTfloat",  alwaysLtFloatTfloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysLeFloatTfloat",  alwaysLeFloatTfloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysGtFloatTfloat",  alwaysGtFloatTfloat,  DataTypes.BooleanType);
        spark.udf().register("alwaysGeFloatTfloat",  alwaysGeFloatTfloat,  DataTypes.BooleanType);
        spark.udf().register("everEqIntTint",        everEqIntTint,        DataTypes.BooleanType);
        spark.udf().register("everNeIntTint",        everNeIntTint,        DataTypes.BooleanType);
        spark.udf().register("everLtIntTint",        everLtIntTint,        DataTypes.BooleanType);
        spark.udf().register("everLeIntTint",        everLeIntTint,        DataTypes.BooleanType);
        spark.udf().register("everGtIntTint",        everGtIntTint,        DataTypes.BooleanType);
        spark.udf().register("everGeIntTint",        everGeIntTint,        DataTypes.BooleanType);
        spark.udf().register("everEqFloatTfloat",    everEqFloatTfloat,    DataTypes.BooleanType);
        spark.udf().register("everNeFloatTfloat",    everNeFloatTfloat,    DataTypes.BooleanType);
        spark.udf().register("everLtFloatTfloat",    everLtFloatTfloat,    DataTypes.BooleanType);
        spark.udf().register("everLeFloatTfloat",    everLeFloatTfloat,    DataTypes.BooleanType);
        spark.udf().register("everGtFloatTfloat",    everGtFloatTfloat,    DataTypes.BooleanType);
        spark.udf().register("everGeFloatTfloat",    everGeFloatTfloat,    DataTypes.BooleanType);
        // tbool × bool predicates
        spark.udf().register("alwaysEqTboolBool",   alwaysEqTboolBool,    DataTypes.BooleanType);
        spark.udf().register("alwaysNeTboolBool",   alwaysNeTboolBool,    DataTypes.BooleanType);
        spark.udf().register("alwaysEqBoolTbool",   alwaysEqBoolTbool,    DataTypes.BooleanType);
        spark.udf().register("alwaysNeBoolTbool",   alwaysNeBoolTbool,    DataTypes.BooleanType);
        spark.udf().register("everEqTboolBool",     everEqTboolBool,      DataTypes.BooleanType);
        spark.udf().register("everNeTboolBool",     everNeTboolBool,      DataTypes.BooleanType);
        spark.udf().register("everEqBoolTbool",     everEqBoolTbool,      DataTypes.BooleanType);
        spark.udf().register("everNeBoolTbool",     everNeBoolTbool,      DataTypes.BooleanType);
        // text × ttext predicates
        spark.udf().register("alwaysEqTextTtext",   alwaysEqTextTtext,    DataTypes.BooleanType);
        spark.udf().register("alwaysNeTextTtext",   alwaysNeTextTtext,    DataTypes.BooleanType);
        spark.udf().register("alwaysLtTextTtext",   alwaysLtTextTtext,    DataTypes.BooleanType);
        spark.udf().register("alwaysLeTextTtext",   alwaysLeTextTtext,    DataTypes.BooleanType);
        spark.udf().register("alwaysGtTextTtext",   alwaysGtTextTtext,    DataTypes.BooleanType);
        spark.udf().register("alwaysGeTextTtext",   alwaysGeTextTtext,    DataTypes.BooleanType);
        spark.udf().register("alwaysEqTtextText",   alwaysEqTtextText,    DataTypes.BooleanType);
        spark.udf().register("alwaysNeTtextText",   alwaysNeTtextText,    DataTypes.BooleanType);
        spark.udf().register("alwaysLtTtextText",   alwaysLtTtextText,    DataTypes.BooleanType);
        spark.udf().register("alwaysLeTtextText",   alwaysLeTtextText,    DataTypes.BooleanType);
        spark.udf().register("alwaysGtTtextText",   alwaysGtTtextText,    DataTypes.BooleanType);
        spark.udf().register("alwaysGeTtextText",   alwaysGeTtextText,    DataTypes.BooleanType);
        spark.udf().register("everEqTextTtext",     everEqTextTtext,      DataTypes.BooleanType);
        spark.udf().register("everNeTextTtext",     everNeTextTtext,      DataTypes.BooleanType);
        spark.udf().register("everLtTextTtext",     everLtTextTtext,      DataTypes.BooleanType);
        spark.udf().register("everLeTextTtext",     everLeTextTtext,      DataTypes.BooleanType);
        spark.udf().register("everGtTextTtext",     everGtTextTtext,      DataTypes.BooleanType);
        spark.udf().register("everGeTextTtext",     everGeTextTtext,      DataTypes.BooleanType);
        spark.udf().register("everEqTtextText",     everEqTtextText,      DataTypes.BooleanType);
        spark.udf().register("everNeTtextText",     everNeTtextText,      DataTypes.BooleanType);
        spark.udf().register("everLtTtextText",     everLtTtextText,      DataTypes.BooleanType);
        spark.udf().register("everLeTtextText",     everLeTtextText,      DataTypes.BooleanType);
        spark.udf().register("everGtTtextText",     everGtTtextText,      DataTypes.BooleanType);
        spark.udf().register("everGeTtextText",     everGeTtextText,      DataTypes.BooleanType);
        // tpoint geometry predicate
        spark.udf().register("tpointIsSimple",       tpointIsSimple,       DataTypes.BooleanType);
    }
}
