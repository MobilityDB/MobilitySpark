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
import java.util.function.Function;
import java.util.function.IntPredicate;

/**
 * Comparison and ordering UDFs for every MobilityDB scalar / set / span /
 * box type, plus the generic temporal compare and hash.
 *
 * <p>MobilityDB declares these as the btree / hash operator-class support
 * functions ({@code cbuffer_eq}, {@code span_cmp}, {@code tbox_hash}, ...) and
 * exposes them through the {@code =, <>, <, <=, >, >=} operators.  Spark SQL
 * has no operator-overloading extension point, so each is registered here as
 * the named function the MobilityDB SQL surface declares.
 *
 * <p>The six ordering predicates are derived from the reliable three-way
 * {@code *_cmp} (an {@code int}): by the btree operator-class contract
 * {@code eq <=> cmp == 0}, {@code lt <=> cmp < 0}, and so on, so the result is
 * identical to the dedicated MEOS {@code *_eq}/{@code *_lt}/... symbols while
 * avoiding the unreliable JNR marshalling of a C {@code bool} (1-byte) return.
 * {@code cmp}, the 32-bit {@code hash} and the 64-bit seeded
 * {@code hash_extended} are bound verbatim to their MEOS symbols.
 *
 * <p>Storage convention (identical to the rest of the codebase): every value
 * is carried as its canonical portable string -- hex-WKB for
 * cbuffer / npoint / pose / set / span / spanset / tbox / stbox / temporal,
 * and text for nsegment (which has no hex-WKB form in MEOS).
 *
 * <p>MEOS function authority: {@code meos/include/meos.h},
 * {@code meos_cbuffer.h}, {@code meos_npoint.h}, {@code meos_pose.h}.
 */
public final class ComparisonUDFs {

    private ComparisonUDFs() {}

    /** Parse a portable string into a freshly-allocated MEOS object. */
    @FunctionalInterface
    interface Parse { Pointer apply(String s); }

    /** Ordering predicate derived from the three-way compare. */
    private static UDF2<String, String, Boolean> ord(
            Parse parse, BiFunction<Pointer, Pointer, Integer> cmp, IntPredicate test) {
        return (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer pa = parse.apply(a);
            if (pa == null) return null;
            try {
                Pointer pb = parse.apply(b);
                if (pb == null) return null;
                try { return test.test(cmp.apply(pa, pb)); }
                finally { MeosMemory.free(pb); }
            } finally { MeosMemory.free(pa); }
        };
    }

    private static UDF2<String, String, Integer> cmpOp(
            Parse parse, BiFunction<Pointer, Pointer, Integer> op) {
        return (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer pa = parse.apply(a);
            if (pa == null) return null;
            try {
                Pointer pb = parse.apply(b);
                if (pb == null) return null;
                try { return op.apply(pa, pb); }
                finally { MeosMemory.free(pb); }
            } finally { MeosMemory.free(pa); }
        };
    }

    private static UDF1<String, Integer> hashOp(
            Parse parse, Function<Pointer, Integer> op) {
        return (a) -> {
            if (a == null) return null;
            MeosThread.ensureReady();
            Pointer pa = parse.apply(a);
            if (pa == null) return null;
            try { return op.apply(pa); }
            finally { MeosMemory.free(pa); }
        };
    }

    private static UDF2<String, Long, Long> hashExtOp(
            Parse parse, BiFunction<Pointer, Long, Long> op) {
        return (a, seed) -> {
            if (a == null || seed == null) return null;
            MeosThread.ensureReady();
            Pointer pa = parse.apply(a);
            if (pa == null) return null;
            try { return op.apply(pa, seed); }
            finally { MeosMemory.free(pa); }
        };
    }

    // ---- cbuffer ----
    public static final UDF2<String, String, Boolean> cbufferEq =
        ord(functions::cbuffer_from_hexwkb, functions::cbuffer_cmp, c -> c == 0);
    public static final UDF2<String, String, Boolean> cbufferNe =
        ord(functions::cbuffer_from_hexwkb, functions::cbuffer_cmp, c -> c != 0);
    public static final UDF2<String, String, Boolean> cbufferLt =
        ord(functions::cbuffer_from_hexwkb, functions::cbuffer_cmp, c -> c < 0);
    public static final UDF2<String, String, Boolean> cbufferLe =
        ord(functions::cbuffer_from_hexwkb, functions::cbuffer_cmp, c -> c <= 0);
    public static final UDF2<String, String, Boolean> cbufferGt =
        ord(functions::cbuffer_from_hexwkb, functions::cbuffer_cmp, c -> c > 0);
    public static final UDF2<String, String, Boolean> cbufferGe =
        ord(functions::cbuffer_from_hexwkb, functions::cbuffer_cmp, c -> c >= 0);
    public static final UDF2<String, String, Integer> cbufferCmp =
        cmpOp(functions::cbuffer_from_hexwkb, functions::cbuffer_cmp);
    // cbuffer_hash / cbuffer_hash_extended are intentionally NOT bound:
    // MEOS hashes the embedded geometry's raw bytes (gserialized_hash /
    // VARDATA), which include uninitialized padding under malloc-based
    // standalone MEOS, so the hash is non-deterministic across calls for
    // the same value (deterministic only in PostgreSQL's palloc0 context).
    // Binding a non-deterministic hash would silently break Spark hash
    // joins / grouping.  Tracked as an upstream MEOS gap; the other seven
    // cbuffer comparison ops (eq/ne/lt/le/gt/ge/cmp) are exact.

    // ---- npoint ----
    public static final UDF2<String, String, Boolean> npointEq =
        ord(functions::npoint_from_hexwkb, functions::npoint_cmp, c -> c == 0);
    public static final UDF2<String, String, Boolean> npointNe =
        ord(functions::npoint_from_hexwkb, functions::npoint_cmp, c -> c != 0);
    public static final UDF2<String, String, Boolean> npointLt =
        ord(functions::npoint_from_hexwkb, functions::npoint_cmp, c -> c < 0);
    public static final UDF2<String, String, Boolean> npointLe =
        ord(functions::npoint_from_hexwkb, functions::npoint_cmp, c -> c <= 0);
    public static final UDF2<String, String, Boolean> npointGt =
        ord(functions::npoint_from_hexwkb, functions::npoint_cmp, c -> c > 0);
    public static final UDF2<String, String, Boolean> npointGe =
        ord(functions::npoint_from_hexwkb, functions::npoint_cmp, c -> c >= 0);
    public static final UDF2<String, String, Integer> npointCmp =
        cmpOp(functions::npoint_from_hexwkb, functions::npoint_cmp);
    public static final UDF1<String, Integer> npointHash =
        hashOp(functions::npoint_from_hexwkb, functions::npoint_hash);
    public static final UDF2<String, Long, Long> npointHashExtended =
        hashExtOp(functions::npoint_from_hexwkb, functions::npoint_hash_extended);

    // ---- pose ----
    public static final UDF2<String, String, Boolean> poseEq =
        ord(functions::pose_from_hexwkb, functions::pose_cmp, c -> c == 0);
    public static final UDF2<String, String, Boolean> poseNe =
        ord(functions::pose_from_hexwkb, functions::pose_cmp, c -> c != 0);
    public static final UDF2<String, String, Boolean> poseLt =
        ord(functions::pose_from_hexwkb, functions::pose_cmp, c -> c < 0);
    public static final UDF2<String, String, Boolean> poseLe =
        ord(functions::pose_from_hexwkb, functions::pose_cmp, c -> c <= 0);
    public static final UDF2<String, String, Boolean> poseGt =
        ord(functions::pose_from_hexwkb, functions::pose_cmp, c -> c > 0);
    public static final UDF2<String, String, Boolean> poseGe =
        ord(functions::pose_from_hexwkb, functions::pose_cmp, c -> c >= 0);
    public static final UDF2<String, String, Integer> poseCmp =
        cmpOp(functions::pose_from_hexwkb, functions::pose_cmp);
    public static final UDF1<String, Integer> poseHash =
        hashOp(functions::pose_from_hexwkb, functions::pose_hash);
    public static final UDF2<String, Long, Long> poseHashExtended =
        hashExtOp(functions::pose_from_hexwkb, functions::pose_hash_extended);

    // ---- nsegment ----
    public static final UDF2<String, String, Boolean> nsegmentEq =
        ord(functions::nsegment_in, functions::nsegment_cmp, c -> c == 0);
    public static final UDF2<String, String, Boolean> nsegmentNe =
        ord(functions::nsegment_in, functions::nsegment_cmp, c -> c != 0);
    public static final UDF2<String, String, Boolean> nsegmentLt =
        ord(functions::nsegment_in, functions::nsegment_cmp, c -> c < 0);
    public static final UDF2<String, String, Boolean> nsegmentLe =
        ord(functions::nsegment_in, functions::nsegment_cmp, c -> c <= 0);
    public static final UDF2<String, String, Boolean> nsegmentGt =
        ord(functions::nsegment_in, functions::nsegment_cmp, c -> c > 0);
    public static final UDF2<String, String, Boolean> nsegmentGe =
        ord(functions::nsegment_in, functions::nsegment_cmp, c -> c >= 0);
    public static final UDF2<String, String, Integer> nsegmentCmp =
        cmpOp(functions::nsegment_in, functions::nsegment_cmp);

    // ---- set ----
    public static final UDF2<String, String, Boolean> setEq =
        ord(functions::set_from_hexwkb, functions::set_cmp, c -> c == 0);
    public static final UDF2<String, String, Boolean> setNe =
        ord(functions::set_from_hexwkb, functions::set_cmp, c -> c != 0);
    public static final UDF2<String, String, Boolean> setLt =
        ord(functions::set_from_hexwkb, functions::set_cmp, c -> c < 0);
    public static final UDF2<String, String, Boolean> setLe =
        ord(functions::set_from_hexwkb, functions::set_cmp, c -> c <= 0);
    public static final UDF2<String, String, Boolean> setGt =
        ord(functions::set_from_hexwkb, functions::set_cmp, c -> c > 0);
    public static final UDF2<String, String, Boolean> setGe =
        ord(functions::set_from_hexwkb, functions::set_cmp, c -> c >= 0);
    public static final UDF2<String, String, Integer> setCmp =
        cmpOp(functions::set_from_hexwkb, functions::set_cmp);
    public static final UDF1<String, Integer> setHash =
        hashOp(functions::set_from_hexwkb, functions::set_hash);
    public static final UDF2<String, Long, Long> setHashExtended =
        hashExtOp(functions::set_from_hexwkb, functions::set_hash_extended);

    // ---- span ----
    public static final UDF2<String, String, Boolean> spanEq =
        ord(functions::span_from_hexwkb, functions::span_cmp, c -> c == 0);
    public static final UDF2<String, String, Boolean> spanNe =
        ord(functions::span_from_hexwkb, functions::span_cmp, c -> c != 0);
    public static final UDF2<String, String, Boolean> spanLt =
        ord(functions::span_from_hexwkb, functions::span_cmp, c -> c < 0);
    public static final UDF2<String, String, Boolean> spanLe =
        ord(functions::span_from_hexwkb, functions::span_cmp, c -> c <= 0);
    public static final UDF2<String, String, Boolean> spanGt =
        ord(functions::span_from_hexwkb, functions::span_cmp, c -> c > 0);
    public static final UDF2<String, String, Boolean> spanGe =
        ord(functions::span_from_hexwkb, functions::span_cmp, c -> c >= 0);
    public static final UDF2<String, String, Integer> spanCmp =
        cmpOp(functions::span_from_hexwkb, functions::span_cmp);
    public static final UDF1<String, Integer> spanHash =
        hashOp(functions::span_from_hexwkb, functions::span_hash);
    public static final UDF2<String, Long, Long> spanHashExtended =
        hashExtOp(functions::span_from_hexwkb, functions::span_hash_extended);

    // ---- spanset ----
    public static final UDF2<String, String, Boolean> spansetEq =
        ord(functions::spanset_from_hexwkb, functions::spanset_cmp, c -> c == 0);
    public static final UDF2<String, String, Boolean> spansetNe =
        ord(functions::spanset_from_hexwkb, functions::spanset_cmp, c -> c != 0);
    public static final UDF2<String, String, Boolean> spansetLt =
        ord(functions::spanset_from_hexwkb, functions::spanset_cmp, c -> c < 0);
    public static final UDF2<String, String, Boolean> spansetLe =
        ord(functions::spanset_from_hexwkb, functions::spanset_cmp, c -> c <= 0);
    public static final UDF2<String, String, Boolean> spansetGt =
        ord(functions::spanset_from_hexwkb, functions::spanset_cmp, c -> c > 0);
    public static final UDF2<String, String, Boolean> spansetGe =
        ord(functions::spanset_from_hexwkb, functions::spanset_cmp, c -> c >= 0);
    public static final UDF2<String, String, Integer> spansetCmp =
        cmpOp(functions::spanset_from_hexwkb, functions::spanset_cmp);
    public static final UDF1<String, Integer> spansetHash =
        hashOp(functions::spanset_from_hexwkb, functions::spanset_hash);
    public static final UDF2<String, Long, Long> spansetHashExtended =
        hashExtOp(functions::spanset_from_hexwkb, functions::spanset_hash_extended);

    // ---- tbox ----
    public static final UDF2<String, String, Boolean> tboxEq =
        ord(functions::tbox_from_hexwkb, functions::tbox_cmp, c -> c == 0);
    public static final UDF2<String, String, Boolean> tboxNe =
        ord(functions::tbox_from_hexwkb, functions::tbox_cmp, c -> c != 0);
    public static final UDF2<String, String, Boolean> tboxLt =
        ord(functions::tbox_from_hexwkb, functions::tbox_cmp, c -> c < 0);
    public static final UDF2<String, String, Boolean> tboxLe =
        ord(functions::tbox_from_hexwkb, functions::tbox_cmp, c -> c <= 0);
    public static final UDF2<String, String, Boolean> tboxGt =
        ord(functions::tbox_from_hexwkb, functions::tbox_cmp, c -> c > 0);
    public static final UDF2<String, String, Boolean> tboxGe =
        ord(functions::tbox_from_hexwkb, functions::tbox_cmp, c -> c >= 0);
    public static final UDF2<String, String, Integer> tboxCmp =
        cmpOp(functions::tbox_from_hexwkb, functions::tbox_cmp);
    public static final UDF1<String, Integer> tboxHash =
        hashOp(functions::tbox_from_hexwkb, functions::tbox_hash);
    public static final UDF2<String, Long, Long> tboxHashExtended =
        hashExtOp(functions::tbox_from_hexwkb, functions::tbox_hash_extended);

    // ---- stbox ----
    public static final UDF2<String, String, Boolean> stboxEq =
        ord(functions::stbox_from_hexwkb, functions::stbox_cmp, c -> c == 0);
    public static final UDF2<String, String, Boolean> stboxNe =
        ord(functions::stbox_from_hexwkb, functions::stbox_cmp, c -> c != 0);
    public static final UDF2<String, String, Boolean> stboxLt =
        ord(functions::stbox_from_hexwkb, functions::stbox_cmp, c -> c < 0);
    public static final UDF2<String, String, Boolean> stboxLe =
        ord(functions::stbox_from_hexwkb, functions::stbox_cmp, c -> c <= 0);
    public static final UDF2<String, String, Boolean> stboxGt =
        ord(functions::stbox_from_hexwkb, functions::stbox_cmp, c -> c > 0);
    public static final UDF2<String, String, Boolean> stboxGe =
        ord(functions::stbox_from_hexwkb, functions::stbox_cmp, c -> c >= 0);
    public static final UDF2<String, String, Integer> stboxCmp =
        cmpOp(functions::stbox_from_hexwkb, functions::stbox_cmp);
    public static final UDF1<String, Integer> stboxHash =
        hashOp(functions::stbox_from_hexwkb, functions::stbox_hash);
    public static final UDF2<String, Long, Long> stboxHashExtended =
        hashExtOp(functions::stbox_from_hexwkb, functions::stbox_hash_extended);

    // ---- generic temporal (covers tbool/tint/tfloat/ttext/tgeompoint/
    //      tgeo/tcbuffer/tnpoint/tpose/trgeometry via the hex-WKB type tag) ----
    public static final UDF2<String, String, Integer> temporalCmp =
        cmpOp(functions::temporal_from_hexwkb, functions::temporal_cmp);
    public static final UDF1<String, Integer> temporalHash =
        hashOp(functions::temporal_from_hexwkb, functions::temporal_hash);

    public static void registerAll(SparkSession spark) {
        spark.udf().register("cbufferEq", cbufferEq, DataTypes.BooleanType);
        spark.udf().register("cbufferNe", cbufferNe, DataTypes.BooleanType);
        spark.udf().register("cbufferLt", cbufferLt, DataTypes.BooleanType);
        spark.udf().register("cbufferLe", cbufferLe, DataTypes.BooleanType);
        spark.udf().register("cbufferGt", cbufferGt, DataTypes.BooleanType);
        spark.udf().register("cbufferGe", cbufferGe, DataTypes.BooleanType);
        spark.udf().register("cbufferCmp", cbufferCmp, DataTypes.IntegerType);
        spark.udf().register("npointEq", npointEq, DataTypes.BooleanType);
        spark.udf().register("npointNe", npointNe, DataTypes.BooleanType);
        spark.udf().register("npointLt", npointLt, DataTypes.BooleanType);
        spark.udf().register("npointLe", npointLe, DataTypes.BooleanType);
        spark.udf().register("npointGt", npointGt, DataTypes.BooleanType);
        spark.udf().register("npointGe", npointGe, DataTypes.BooleanType);
        spark.udf().register("npointCmp", npointCmp, DataTypes.IntegerType);
        spark.udf().register("npointHash", npointHash, DataTypes.IntegerType);
        spark.udf().register("npointHashExtended", npointHashExtended, DataTypes.LongType);
        spark.udf().register("poseEq", poseEq, DataTypes.BooleanType);
        spark.udf().register("poseNe", poseNe, DataTypes.BooleanType);
        spark.udf().register("poseLt", poseLt, DataTypes.BooleanType);
        spark.udf().register("poseLe", poseLe, DataTypes.BooleanType);
        spark.udf().register("poseGt", poseGt, DataTypes.BooleanType);
        spark.udf().register("poseGe", poseGe, DataTypes.BooleanType);
        spark.udf().register("poseCmp", poseCmp, DataTypes.IntegerType);
        spark.udf().register("poseHash", poseHash, DataTypes.IntegerType);
        spark.udf().register("poseHashExtended", poseHashExtended, DataTypes.LongType);
        spark.udf().register("nsegmentEq", nsegmentEq, DataTypes.BooleanType);
        spark.udf().register("nsegmentNe", nsegmentNe, DataTypes.BooleanType);
        spark.udf().register("nsegmentLt", nsegmentLt, DataTypes.BooleanType);
        spark.udf().register("nsegmentLe", nsegmentLe, DataTypes.BooleanType);
        spark.udf().register("nsegmentGt", nsegmentGt, DataTypes.BooleanType);
        spark.udf().register("nsegmentGe", nsegmentGe, DataTypes.BooleanType);
        spark.udf().register("nsegmentCmp", nsegmentCmp, DataTypes.IntegerType);
        spark.udf().register("setEq", setEq, DataTypes.BooleanType);
        spark.udf().register("setNe", setNe, DataTypes.BooleanType);
        spark.udf().register("setLt", setLt, DataTypes.BooleanType);
        spark.udf().register("setLe", setLe, DataTypes.BooleanType);
        spark.udf().register("setGt", setGt, DataTypes.BooleanType);
        spark.udf().register("setGe", setGe, DataTypes.BooleanType);
        spark.udf().register("setCmp", setCmp, DataTypes.IntegerType);
        spark.udf().register("setHash", setHash, DataTypes.IntegerType);
        spark.udf().register("setHashExtended", setHashExtended, DataTypes.LongType);
        spark.udf().register("spanEq", spanEq, DataTypes.BooleanType);
        spark.udf().register("spanNe", spanNe, DataTypes.BooleanType);
        spark.udf().register("spanLt", spanLt, DataTypes.BooleanType);
        spark.udf().register("spanLe", spanLe, DataTypes.BooleanType);
        spark.udf().register("spanGt", spanGt, DataTypes.BooleanType);
        spark.udf().register("spanGe", spanGe, DataTypes.BooleanType);
        spark.udf().register("spanCmp", spanCmp, DataTypes.IntegerType);
        spark.udf().register("spanHash", spanHash, DataTypes.IntegerType);
        spark.udf().register("spanHashExtended", spanHashExtended, DataTypes.LongType);
        spark.udf().register("spansetEq", spansetEq, DataTypes.BooleanType);
        spark.udf().register("spansetNe", spansetNe, DataTypes.BooleanType);
        spark.udf().register("spansetLt", spansetLt, DataTypes.BooleanType);
        spark.udf().register("spansetLe", spansetLe, DataTypes.BooleanType);
        spark.udf().register("spansetGt", spansetGt, DataTypes.BooleanType);
        spark.udf().register("spansetGe", spansetGe, DataTypes.BooleanType);
        spark.udf().register("spansetCmp", spansetCmp, DataTypes.IntegerType);
        spark.udf().register("spansetHash", spansetHash, DataTypes.IntegerType);
        spark.udf().register("spansetHashExtended", spansetHashExtended, DataTypes.LongType);
        spark.udf().register("tboxEq", tboxEq, DataTypes.BooleanType);
        spark.udf().register("tboxNe", tboxNe, DataTypes.BooleanType);
        spark.udf().register("tboxLt", tboxLt, DataTypes.BooleanType);
        spark.udf().register("tboxLe", tboxLe, DataTypes.BooleanType);
        spark.udf().register("tboxGt", tboxGt, DataTypes.BooleanType);
        spark.udf().register("tboxGe", tboxGe, DataTypes.BooleanType);
        spark.udf().register("tboxCmp", tboxCmp, DataTypes.IntegerType);
        spark.udf().register("tboxHash", tboxHash, DataTypes.IntegerType);
        spark.udf().register("tboxHashExtended", tboxHashExtended, DataTypes.LongType);
        spark.udf().register("stboxEq", stboxEq, DataTypes.BooleanType);
        spark.udf().register("stboxNe", stboxNe, DataTypes.BooleanType);
        spark.udf().register("stboxLt", stboxLt, DataTypes.BooleanType);
        spark.udf().register("stboxLe", stboxLe, DataTypes.BooleanType);
        spark.udf().register("stboxGt", stboxGt, DataTypes.BooleanType);
        spark.udf().register("stboxGe", stboxGe, DataTypes.BooleanType);
        spark.udf().register("stboxCmp", stboxCmp, DataTypes.IntegerType);
        spark.udf().register("stboxHash", stboxHash, DataTypes.IntegerType);
        spark.udf().register("stboxHashExtended", stboxHashExtended, DataTypes.LongType);
        spark.udf().register("temporalCmp", temporalCmp, DataTypes.IntegerType);
        spark.udf().register("temporalHash", temporalHash, DataTypes.IntegerType);
    }
}
