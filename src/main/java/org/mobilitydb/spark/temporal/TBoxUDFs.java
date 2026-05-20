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
import jnr.ffi.Runtime;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Spark SQL UDFs for TBox (temporal numeric bounding box) accessor operations.
 *
 * TBox values are stored as hex-WKB strings (tbox_as_hexwkb output).
 *
 * Numeric bound accessors (xmin/xmax) use output-pointer pattern: JMEOS
 * allocates an 8-byte buffer, passes it as out-pointer, returns null if
 * the box has no X component.
 *
 * Temporal bound accessors (tmin/tmax) use the same pattern with int64
 * PG-epoch microseconds.  Inclusivity accessors use a 1-byte output buffer.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class TBoxUDFs {

    private TBoxUDFs() {}

    // milliseconds from Unix epoch (1970-01-01) to PG epoch (2000-01-01)
    private static final long PG_UNIX_OFFSET_MS = 946684800L * 1000L;

    private static Pointer tboxPtr(String hex) {
        if (hex == null) return null;
        return functions.tbox_from_hexwkb(hex);
    }

    // ------------------------------------------------------------------
    // Has-component flags
    // ------------------------------------------------------------------

    public static final UDF1<String, Boolean> tboxHasx =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            return functions.tbox_hasx(p);
        };

    public static final UDF1<String, Boolean> tboxHast =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            return functions.tbox_hast(p);
        };

    // ------------------------------------------------------------------
    // Numeric (X) bound accessors  (Pointer → double at offset 0)
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> tboxXmin =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tbox_xmin(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> tboxXmax =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tbox_xmax(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Boolean> tboxXminInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tbox_xmin_inc(p);
            return r == null ? null : r.getByte(0) != 0;
        };

    public static final UDF1<String, Boolean> tboxXmaxInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tbox_xmax_inc(p);
            return r == null ? null : r.getByte(0) != 0;
        };

    // ------------------------------------------------------------------
    // Temporal (T) bound accessors  (Pointer → int64 PG-epoch μs at offset 0)
    // ------------------------------------------------------------------

    public static final UDF1<String, java.sql.Timestamp> tboxTmin =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tbox_tmin(p);
            if (r == null) return null;
            return new java.sql.Timestamp(r.getLong(0) / 1000L + PG_UNIX_OFFSET_MS);
        };

    public static final UDF1<String, java.sql.Timestamp> tboxTmax =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tbox_tmax(p);
            if (r == null) return null;
            return new java.sql.Timestamp(r.getLong(0) / 1000L + PG_UNIX_OFFSET_MS);
        };

    public static final UDF1<String, Boolean> tboxTminInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tbox_tmin_inc(p);
            return r == null ? null : r.getByte(0) != 0;
        };

    public static final UDF1<String, Boolean> tboxTmaxInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tbox_tmax_inc(p);
            return r == null ? null : r.getByte(0) != 0;
        };

    // ------------------------------------------------------------------
    // Span conversions  (tbox_hex → span hex-WKB)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tboxToIntspan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer span = functions.tbox_to_intspan(p);
            if (span == null) return null;
            return functions.span_as_hexwkb(span, (byte) 0);
        };

    public static final UDF1<String, String> tboxToFloatspan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer span = functions.tbox_to_floatspan(p);
            if (span == null) return null;
            return functions.span_as_hexwkb(span, (byte) 0);
        };

    public static final UDF1<String, String> tboxToTstzspan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer span = functions.tbox_to_tstzspan(p);
            if (span == null) return null;
            return functions.span_as_hexwkb(span, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Conversion from span / spanset / set to TBox
    // ------------------------------------------------------------------

    // spanToTbox(spanHex STRING) → STRING
    // MEOS: span_to_tbox(const Span *) → TBox *
    public static final UDF1<String, String> spanToTbox =
        (spanHex) -> {
            if (spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(spanHex);
            if (p == null) return null;
            Pointer tb = functions.span_to_tbox(p);
            if (tb == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                return functions.tbox_as_hexwkb(tb, (byte) 0, rt.getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(tb);
            }
        };

    // spansetToTbox(spansetHex STRING) → STRING
    // MEOS: spanset_to_tbox(const SpanSet *) → TBox *
    public static final UDF1<String, String> spansetToTbox =
        (spansetHex) -> {
            if (spansetHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(spansetHex);
            if (p == null) return null;
            Pointer tb = functions.spanset_to_tbox(p);
            if (tb == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                return functions.tbox_as_hexwkb(tb, (byte) 0, rt.getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(tb);
            }
        };

    // setToTbox(setHex STRING) → STRING
    // MEOS: set_to_tbox(const Set *) → TBox *
    public static final UDF1<String, String> setToTbox =
        (setHex) -> {
            if (setHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.set_from_hexwkb(setHex);
            if (p == null) return null;
            Pointer tb = functions.set_to_tbox(p);
            if (tb == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                return functions.tbox_as_hexwkb(tb, (byte) 0, rt.getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(tb);
            }
        };

    // ------------------------------------------------------------------
    // Typed X-bound accessors  (tboxfloat / tboxint variants)
    //
    // These differ from the generic tboxXmin/tboxXmax above in that they
    // return integer values for tboxint and preserve float precision for
    // tboxfloat — using the typed MEOS accessors rather than the generic ones.
    //
    // MEOS: tboxfloat_xmin, tboxfloat_xmax → double *
    //       tboxint_xmin,   tboxint_xmax   → int *  (MEOS uses int32)
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> tboxfloatXmin =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tboxfloat_xmin(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> tboxfloatXmax =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tboxfloat_xmax(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Integer> tboxintXmin =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tboxint_xmin(p);
            return r == null ? null : r.getInt(0);
        };

    public static final UDF1<String, Integer> tboxintXmax =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.tboxint_xmax(p);
            return r == null ? null : r.getInt(0);
        };

    // ------------------------------------------------------------------
    // TBox constructors  (make, from numspan+timestamptz, from timestamptz)
    //
    // MEOS: tbox_make, numspan_timestamptz_to_tbox, timestamptz_to_tbox
    // ------------------------------------------------------------------

    // tboxMake(numspanHex STRING, tstzspanHex STRING) → STRING
    // Either argument may be null to produce an X-only or T-only TBox.
    public static final UDF2<String, String, String> tboxMake =
        (numspanHex, tstzspanHex) -> {
            if (numspanHex == null && tstzspanHex == null) return null;
            MeosThread.ensureReady();
            Pointer numspan  = numspanHex  != null ? functions.span_from_hexwkb(numspanHex)  : null;
            Pointer tstzspan = tstzspanHex != null ? functions.span_from_hexwkb(tstzspanHex) : null;
            Pointer result = functions.tbox_make(numspan, tstzspan);
            if (result == null) return null;
            try {
                return functions.tbox_as_hexwkb(result, (byte) 0,
                    Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(result);
            }
        };

    // timestamptzToTbox(ts TIMESTAMP) → STRING
    // MEOS: timestamptz_to_tbox(TimestampTz) → TBox *
    public static final UDF1<java.sql.Timestamp, String> timestamptzToTbox =
        (ts) -> {
            if (ts == null) return null;
            MeosThread.ensureReady();
            long pgMicros = (ts.getTime() - PG_UNIX_OFFSET_MS) * 1000L;
            OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(pgMicros, 0), ZoneOffset.UTC);
            Pointer result = functions.timestamptz_to_tbox(odt);
            if (result == null) return null;
            try {
                return functions.tbox_as_hexwkb(result, (byte) 0,
                    Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(result);
            }
        };

    // numspanTimestamptzToTbox(spanHex STRING, ts TIMESTAMP) → STRING
    // MEOS: numspan_timestamptz_to_tbox(const Span *, TimestampTz) → TBox *
    public static final UDF2<String, java.sql.Timestamp, String> numspanTimestamptzToTbox =
        (spanHex, ts) -> {
            if (spanHex == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer span = functions.span_from_hexwkb(spanHex);
            if (span == null) return null;
            long pgMicros = (ts.getTime() - PG_UNIX_OFFSET_MS) * 1000L;
            OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(pgMicros, 0), ZoneOffset.UTC);
            Pointer result = functions.numspan_timestamptz_to_tbox(span, odt);
            if (result == null) return null;
            try {
                return functions.tbox_as_hexwkb(result, (byte) 0,
                    Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(result);
            }
        };

    // ------------------------------------------------------------------
    // TBox time-dimension transforms
    //
    // MEOS: tbox_expand_time, tbox_shift_scale_time
    // ------------------------------------------------------------------

    // tboxExpandTime(hex STRING, intervalStr STRING) → STRING
    public static final UDF2<String, String, String> tboxExpandTime =
        (hex, intervalStr) -> {
            if (hex == null || intervalStr == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) return null;
            Pointer result = functions.tbox_expand_time(p, iv);
            if (result == null) return null;
            try {
                return functions.tbox_as_hexwkb(result, (byte) 0,
                    Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(result);
            }
        };

    // tboxExpandFloat(hex STRING, value DOUBLE) → STRING
    // MEOS: tfloatbox_expand (renamed; not in JMEOS-1.4)
    public static final UDF2<String, Double, String> tboxExpandFloat =
        (hex, v) -> {
            if (hex == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer result = org.mobilitydb.spark.MeosNative.INSTANCE.tfloatbox_expand(p, v);
            if (result == null) return null;
            try {
                return functions.tbox_as_hexwkb(result, (byte) 0,
                    Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(result);
            }
        };

    // tboxExpandInt(hex STRING, value INT) → STRING
    // MEOS: tintbox_expand (renamed; not in JMEOS-1.4)
    public static final UDF2<String, Integer, String> tboxExpandInt =
        (hex, v) -> {
            if (hex == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer result = org.mobilitydb.spark.MeosNative.INSTANCE.tintbox_expand(p, v);
            if (result == null) return null;
            try {
                return functions.tbox_as_hexwkb(result, (byte) 0,
                    Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(result);
            }
        };

    // tboxShiftScaleTime(hex STRING, shiftStr STRING, scaleStr STRING) → STRING
    // Either shiftStr or scaleStr (but not both) may be null.
    public static final UDF3<String, String, String, String> tboxShiftScaleTime =
        (hex, shiftStr, scaleStr) -> {
            if (hex == null) return null;
            if (shiftStr == null && scaleStr == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer shiftIv = shiftStr != null ? functions.pg_interval_in(shiftStr, -1) : null;
            Pointer scaleIv = scaleStr != null ? functions.pg_interval_in(scaleStr, -1) : null;
            Pointer result = functions.tbox_shift_scale_time(p, shiftIv, scaleIv);
            if (result == null) return null;
            try {
                return functions.tbox_as_hexwkb(result, (byte) 0,
                    Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8));
            } finally {
                MeosMemory.free(result);
            }
        };

    // ------------------------------------------------------------------
    // Rounding
    // ------------------------------------------------------------------

    // tboxRound(hex STRING, maxDecimals INT) → STRING
    // MEOS: tbox_round(const TBox *, int) → TBox *
    public static final UDF2<String, Integer, String> tboxRound =
        (hex, maxDecimals) -> {
            if (hex == null || maxDecimals == null) return null;
            MeosThread.ensureReady();
            Pointer p = tboxPtr(hex);
            if (p == null) return null;
            Pointer result = functions.tbox_round(p, maxDecimals);
            if (result == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                return functions.tbox_as_hexwkb(result, (byte) 0, sizeOut);
            } finally {
                MeosMemory.free(result);
            }
        };

    // ------------------------------------------------------------------
    // Registration
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        spark.udf().register("tboxHasx",        tboxHasx,        DataTypes.BooleanType);
        spark.udf().register("tboxHast",        tboxHast,        DataTypes.BooleanType);
        spark.udf().register("tboxXmin",        tboxXmin,        DataTypes.DoubleType);
        spark.udf().register("tboxXmax",        tboxXmax,        DataTypes.DoubleType);
        spark.udf().register("tboxXminInc",     tboxXminInc,     DataTypes.BooleanType);
        spark.udf().register("tboxXmaxInc",     tboxXmaxInc,     DataTypes.BooleanType);
        spark.udf().register("tboxTmin",        tboxTmin,        DataTypes.TimestampType);
        spark.udf().register("tboxTmax",        tboxTmax,        DataTypes.TimestampType);
        spark.udf().register("tboxTminInc",     tboxTminInc,     DataTypes.BooleanType);
        spark.udf().register("tboxTmaxInc",     tboxTmaxInc,     DataTypes.BooleanType);
        spark.udf().register("tboxToIntspan",   tboxToIntspan,   DataTypes.StringType);
        spark.udf().register("tboxToFloatspan", tboxToFloatspan, DataTypes.StringType);
        spark.udf().register("tboxToTstzspan",  tboxToTstzspan,  DataTypes.StringType);
        spark.udf().register("tboxRound",                 tboxRound,                 DataTypes.StringType);
        // Conversion from span / spanset / set to TBox
        spark.udf().register("spanToTbox",               spanToTbox,               DataTypes.StringType);
        spark.udf().register("spansetToTbox",             spansetToTbox,             DataTypes.StringType);
        spark.udf().register("setToTbox",                setToTbox,                DataTypes.StringType);
        // Typed X-bound accessors
        spark.udf().register("tboxfloatXmin",            tboxfloatXmin,            DataTypes.DoubleType);
        spark.udf().register("tboxfloatXmax",            tboxfloatXmax,            DataTypes.DoubleType);
        spark.udf().register("tboxintXmin",              tboxintXmin,              DataTypes.IntegerType);
        spark.udf().register("tboxintXmax",              tboxintXmax,              DataTypes.IntegerType);
        // TBox constructors
        spark.udf().register("tboxMake",                 tboxMake,                 DataTypes.StringType);
        spark.udf().register("timestamptzToTbox",        timestamptzToTbox,        DataTypes.StringType);
        spark.udf().register("numspanTimestamptzToTbox", numspanTimestamptzToTbox, DataTypes.StringType);
        // TBox time-dimension transforms
        spark.udf().register("tboxExpandTime",           tboxExpandTime,           DataTypes.StringType);
        spark.udf().register("tboxExpandFloat",          tboxExpandFloat,          DataTypes.StringType);
        spark.udf().register("tboxExpandInt",            tboxExpandInt,            DataTypes.StringType);
        spark.udf().register("tboxShiftScaleTime",       tboxShiftScaleTime,       DataTypes.StringType);
        // TBox set operations
        spark.udf().register("intersectionTboxTbox",     intersectionTboxTbox,     DataTypes.StringType);
        spark.udf().register("unionTboxTbox",            unionTboxTbox,            DataTypes.StringType);
        // MobilityDB SQL bare-name aliases for the same lambdas
        spark.udf().register("tboxIntersection",         intersectionTboxTbox,     DataTypes.StringType);
        spark.udf().register("tboxUnion",                unionTboxTbox,            DataTypes.StringType);
        // expandValue alias — covers float/int dispatch via Object input;
        // most users will use the typed tboxExpandFloat/tboxExpandInt directly.
        spark.udf().register("expandValue",              tboxExpandFloat,          DataTypes.StringType);
        // TBox topology predicates (tbox, tbox)
        spark.udf().register("tboxContains",    tboxContains,    DataTypes.BooleanType);
        spark.udf().register("tboxContained",   tboxContained,   DataTypes.BooleanType);
        spark.udf().register("tboxOverlaps",    tboxOverlaps,    DataTypes.BooleanType);
        // TBox positional predicates (tbox, tbox)
        spark.udf().register("tboxLeft",        tboxLeft,        DataTypes.BooleanType);
        spark.udf().register("tboxOverleft",    tboxOverleft,    DataTypes.BooleanType);
        spark.udf().register("tboxRight",       tboxRight,       DataTypes.BooleanType);
        spark.udf().register("tboxOverright",   tboxOverright,   DataTypes.BooleanType);
        spark.udf().register("tboxBefore",      tboxBefore,      DataTypes.BooleanType);
        spark.udf().register("tboxOverbefore",  tboxOverbefore,  DataTypes.BooleanType);
        spark.udf().register("tboxAfter",       tboxAfter,       DataTypes.BooleanType);
        spark.udf().register("tboxOverafter",   tboxOverafter,   DataTypes.BooleanType);
        spark.udf().register("tboxAdjacent",    tboxAdjacent,    DataTypes.BooleanType);
    }

    // ------------------------------------------------------------------
    // TBox set operations
    // MEOS: intersection_tbox_tbox(TBox *, TBox *) → TBox * (NULL if empty)
    //       union_tbox_tbox(TBox *, TBox *, bool strict) → TBox *
    // ------------------------------------------------------------------

    private static String tboxBinOp(String h1, String h2,
            java.util.function.BiFunction<Pointer, Pointer, Pointer> fn) {
        if (h1 == null || h2 == null) return null;
        MeosThread.ensureReady();
        Pointer p1 = tboxPtr(h1), p2 = tboxPtr(h2);
        if (p1 == null || p2 == null) return null;
        Runtime rt = Runtime.getSystemRuntime();
        try {
            Pointer r = fn.apply(p1, p2);
            if (r == null) return null;
            try {
                return functions.tbox_as_hexwkb(r, (byte) 0, rt.getMemoryManager().allocateDirect(8));
            } finally { MeosMemory.free(r); }
        } finally {
            MeosMemory.free(p1);
            MeosMemory.free(p2);
        }
    }

    public static final UDF2<String, String, String> intersectionTboxTbox =
        (h1, h2) -> tboxBinOp(h1, h2, functions::intersection_tbox_tbox);

    public static final UDF2<String, String, String> unionTboxTbox =
        (h1, h2) -> tboxBinOp(h1, h2, (p1, p2) -> functions.union_tbox_tbox(p1, p2, false));

    // ------------------------------------------------------------------
    // TBox positional predicates  (tbox, tbox) → Boolean
    // MEOS: left/overleft/right/overright/before/overbefore/after/overafter/adjacent
    //       _tbox_tbox(TBox *, TBox *) → bool
    // ------------------------------------------------------------------

    private static Boolean tboxBoolOp(String h1, String h2,
            java.util.function.BiFunction<Pointer, Pointer, Boolean> fn) {
        if (h1 == null || h2 == null) return null;
        MeosThread.ensureReady();
        Pointer p1 = tboxPtr(h1), p2 = tboxPtr(h2);
        if (p1 == null || p2 == null) return null;
        return fn.apply(p1, p2);
    }

    // ------------------------------------------------------------------
    // TBox topology predicates  (tbox, tbox) → Boolean
    // MEOS: contains/contained/overlaps_tbox_tbox → bool
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tboxContains =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::contains_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxContained =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::contained_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverlaps =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::overlaps_tbox_tbox);

    public static final UDF2<String, String, Boolean> tboxLeft =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::left_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverleft =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::overleft_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxRight =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::right_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverright =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::overright_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxBefore =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::before_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverbefore =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::overbefore_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxAfter =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::after_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverafter =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::overafter_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxAdjacent =
        (h1, h2) -> tboxBoolOp(h1, h2, functions::adjacent_tbox_tbox);
}
