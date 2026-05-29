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

import java.util.function.BiFunction;
import java.util.function.ObjDoubleConsumer;

/**
 * Spark SQL UDFs for temporal comparison operators (`teq`, `tne`, `tlt`,
 * `tle`, `tgt`, `tge`) returning a temporal boolean (hex-WKB tbool).
 *
 * MobilityDB exposes `temporal_teq(value, temporal)` and operators `#=`,
 * `#&lt;>`, etc. Spark SQL has no operator extension API, so these are
 * registered as named UDFs. Equality/inequality are symmetric so only the
 * forward direction (temporal first) is provided.
 *
 * MEOS function authority: meos/include/meos.h — teq_tint_int, teq_tfloat_float,
 *   teq_ttext_text, teq_tbool_bool, teq_temporal_temporal, and similarly
 *   for tne/tlt/tle/tgt/tge.
 */
public final class TemporalCompUDFs {

    private TemporalCompUDFs() {}

    // ------------------------------------------------------------------
    // Helpers — five families based on right-hand input type.
    // ------------------------------------------------------------------

    private static UDF2<String, Integer, String> hexInt(BiFunction<Pointer, Integer, Pointer> fn) {
        return (hex, v) -> {
            if (hex == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = fn.apply(p, v);
                if (r == null) return null;
                try { return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };
    }

    private static UDF2<String, Double, String> hexDouble(java.util.function.ToDoubleBiFunction<Pointer, Double> _unused) {
        // Not used — Java's BiFunction with primitive double is awkward; we
        // inline the double calls below instead.
        return null;
    }

    @FunctionalInterface
    private interface PointerDoubleFn { Pointer apply(Pointer a, double b); }
    @FunctionalInterface
    private interface PointerBoolFn   { Pointer apply(Pointer a, boolean b); }

    private static UDF2<String, Double, String> hexFloat(PointerDoubleFn fn) {
        return (hex, v) -> {
            if (hex == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = fn.apply(p, v);
                if (r == null) return null;
                try { return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };
    }

    private static UDF2<String, Boolean, String> hexBool(PointerBoolFn fn) {
        return (hex, v) -> {
            if (hex == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = fn.apply(p, v);
                if (r == null) return null;
                try { return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };
    }

    private static UDF2<String, String, String> hexText(BiFunction<Pointer, Pointer, Pointer> fn) {
        return (hex, txt) -> {
            if (hex == null || txt == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            Pointer t = GeneratedFunctions.cstring2text(txt);
            if (t == null) { MeosMemory.free(p); return null; }
            try {
                Pointer r = fn.apply(p, t);
                if (r == null) return null;
                try { return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p, t); }
        };
    }

    private static UDF2<String, String, String> hexHex(BiFunction<Pointer, Pointer, Pointer> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(h1);
            if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = fn.apply(p1, p2);
                if (r == null) return null;
                try { return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p1, p2); }
        };
    }

    // ------------------------------------------------------------------
    // teq — temporal equality
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> teqTintInt   = hexInt(GeneratedFunctions::teq_tint_int);
    public static final UDF2<String, Double, String>  teqTfloatFloat = hexFloat(GeneratedFunctions::teq_tfloat_float);
    public static final UDF2<String, Boolean, String> teqTboolBool = hexBool(GeneratedFunctions::teq_tbool_bool);
    public static final UDF2<String, String, String>  teqTtextText = hexText(GeneratedFunctions::teq_ttext_text);
    public static final UDF2<String, String, String>  teqTemporal  = hexHex(GeneratedFunctions::teq_temporal_temporal);

    // ------------------------------------------------------------------
    // tne — temporal inequality
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tneTintInt   = hexInt(GeneratedFunctions::tne_tint_int);
    public static final UDF2<String, Double, String>  tneTfloatFloat = hexFloat(GeneratedFunctions::tne_tfloat_float);
    public static final UDF2<String, Boolean, String> tneTboolBool = hexBool(GeneratedFunctions::tne_tbool_bool);
    public static final UDF2<String, String, String>  tneTtextText = hexText(GeneratedFunctions::tne_ttext_text);
    public static final UDF2<String, String, String>  tneTemporal  = hexHex(GeneratedFunctions::tne_temporal_temporal);

    // ------------------------------------------------------------------
    // tlt — temporal less-than
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tltTintInt   = hexInt(GeneratedFunctions::tlt_tint_int);
    public static final UDF2<String, Double, String>  tltTfloatFloat = hexFloat(GeneratedFunctions::tlt_tfloat_float);
    public static final UDF2<String, String, String>  tltTtextText = hexText(GeneratedFunctions::tlt_ttext_text);
    public static final UDF2<String, String, String>  tltTemporal  = hexHex(GeneratedFunctions::tlt_temporal_temporal);

    // ------------------------------------------------------------------
    // tle — temporal less-or-equal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tleTintInt   = hexInt(GeneratedFunctions::tle_tint_int);
    public static final UDF2<String, Double, String>  tleTfloatFloat = hexFloat(GeneratedFunctions::tle_tfloat_float);
    public static final UDF2<String, String, String>  tleTtextText = hexText(GeneratedFunctions::tle_ttext_text);
    public static final UDF2<String, String, String>  tleTemporal  = hexHex(GeneratedFunctions::tle_temporal_temporal);

    // ------------------------------------------------------------------
    // tgt — temporal greater-than
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tgtTintInt   = hexInt(GeneratedFunctions::tgt_tint_int);
    public static final UDF2<String, Double, String>  tgtTfloatFloat = hexFloat(GeneratedFunctions::tgt_tfloat_float);
    public static final UDF2<String, String, String>  tgtTtextText = hexText(GeneratedFunctions::tgt_ttext_text);
    public static final UDF2<String, String, String>  tgtTemporal  = hexHex(GeneratedFunctions::tgt_temporal_temporal);

    // ------------------------------------------------------------------
    // tge — temporal greater-or-equal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tgeTintInt   = hexInt(GeneratedFunctions::tge_tint_int);
    public static final UDF2<String, Double, String>  tgeTfloatFloat = hexFloat(GeneratedFunctions::tge_tfloat_float);
    public static final UDF2<String, String, String>  tgeTtextText = hexText(GeneratedFunctions::tge_ttext_text);
    public static final UDF2<String, String, String>  tgeTemporal  = hexHex(GeneratedFunctions::tge_temporal_temporal);

    public static void registerAll(SparkSession spark) {
        // teq
        spark.udf().register("teqTintInt",     teqTintInt,     DataTypes.StringType);
        spark.udf().register("teqTfloatFloat", teqTfloatFloat, DataTypes.StringType);
        spark.udf().register("teqTboolBool",   teqTboolBool,   DataTypes.StringType);
        spark.udf().register("teqTtextText",   teqTtextText,   DataTypes.StringType);
        // tne
        spark.udf().register("tneTintInt",     tneTintInt,     DataTypes.StringType);
        spark.udf().register("tneTfloatFloat", tneTfloatFloat, DataTypes.StringType);
        spark.udf().register("tneTboolBool",   tneTboolBool,   DataTypes.StringType);
        spark.udf().register("tneTtextText",   tneTtextText,   DataTypes.StringType);
        // tlt
        spark.udf().register("tltTintInt",     tltTintInt,     DataTypes.StringType);
        spark.udf().register("tltTfloatFloat", tltTfloatFloat, DataTypes.StringType);
        spark.udf().register("tltTtextText",   tltTtextText,   DataTypes.StringType);
        // tle
        spark.udf().register("tleTintInt",     tleTintInt,     DataTypes.StringType);
        spark.udf().register("tleTfloatFloat", tleTfloatFloat, DataTypes.StringType);
        spark.udf().register("tleTtextText",   tleTtextText,   DataTypes.StringType);
        // tgt
        spark.udf().register("tgtTintInt",     tgtTintInt,     DataTypes.StringType);
        spark.udf().register("tgtTfloatFloat", tgtTfloatFloat, DataTypes.StringType);
        spark.udf().register("tgtTtextText",   tgtTtextText,   DataTypes.StringType);
        // tge
        spark.udf().register("tgeTintInt",     tgeTintInt,     DataTypes.StringType);
        spark.udf().register("tgeTfloatFloat", tgeTfloatFloat, DataTypes.StringType);
        spark.udf().register("tgeTtextText",   tgeTtextText,   DataTypes.StringType);
        // The temporal × temporal forms teq/tne/tlt/tle/tgt/tge are
        // superseded 1:1 by the portable bare names, registered by
        // org.mobilitydb.spark.portable.PortableOperatorAliasUDFs reusing
        // these very backing fields (one bare name, all six families).
    }
}
