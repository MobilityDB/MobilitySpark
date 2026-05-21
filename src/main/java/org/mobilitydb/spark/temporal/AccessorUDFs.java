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

import java.sql.Timestamp;

/**
 * Spark SQL UDFs for temporal accessor and manipulation operations.
 *
 * Naming convention mirrors MobilityDuck where possible, using camelCase for
 * Spark SQL UDF names. Type-specific UDFs use a type prefix (tfloat*, tint*)
 * where the return type depends on the base temporal type.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class AccessorUDFs {

    private AccessorUDFs() {}

    // ------------------------------------------------------------------
    // Type-agnostic accessors (return type does not depend on base type)
    // ------------------------------------------------------------------

    // numSequences(trip STRING) → INT
    // MEOS: temporal_num_sequences(const Temporal *) → int
    public static final UDF1<String, Integer> numSequences =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.temporal_num_sequences(ptr);
        };

    // interp(trip STRING) → STRING  ("Discrete" | "Stepwise" | "Linear")
    // MEOS: temporal_interp(const Temporal *) → char *
    public static final UDF1<String, String> interp =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.temporal_interp(ptr);
        };

    // time(trip STRING) → STRING  (hex-WKB of tstzspanset bounding the instants)
    // MEOS: temporal_time(const Temporal *) → SpanSet *
    public static final UDF1<String, String> time =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer ss = functions.temporal_time(ptr);
            if (ss == null) return null;
            return functions.spanset_as_hexwkb(ss, (byte) 0);
        };

    // timespan(trip STRING) → STRING  (hex-WKB of tstzspan — overall bounding period)
    // MEOS: temporal_to_tstzspan(const Temporal *) → Span *
    public static final UDF1<String, String> timespan =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer s = functions.temporal_to_tstzspan(ptr);
            if (s == null) return null;
            return functions.span_as_hexwkb(s, (byte) 0);
        };

    // merge(trip1 STRING, trip2 STRING) → STRING
    // MEOS: temporal_merge(const Temporal *, const Temporal *) → Temporal *
    public static final UDF2<String, String, String> merge =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            Pointer result = functions.temporal_merge(p1, p2);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // shift(trip STRING, deltaStr STRING) → STRING
    // deltaStr is a PostgreSQL interval literal, e.g. "1 day" or "01:00:00".
    // MEOS: temporal_shift_time(const Temporal *, const Interval *) → Temporal *
    public static final UDF2<String, String, String> shift =
        (trip, deltaStr) -> {
            if (trip == null || deltaStr == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer ivPtr = functions.pg_interval_in(deltaStr, -1);
            if (tptr == null || ivPtr == null) return null;
            Pointer result = functions.temporal_shift_time(tptr, ivPtr);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // scale(trip STRING, durationStr STRING) → STRING
    // MEOS: temporal_scale_time(const Temporal *, const Interval *) → Temporal *
    public static final UDF2<String, String, String> scale =
        (trip, durationStr) -> {
            if (trip == null || durationStr == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer ivPtr = functions.pg_interval_in(durationStr, -1);
            if (tptr == null || ivPtr == null) return null;
            Pointer result = functions.temporal_scale_time(tptr, ivPtr);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // atSpan(trip STRING, spanHex STRING) → STRING
    // MEOS: temporal_at_tstzspan(const Temporal *, const Span *) → Temporal *
    public static final UDF2<String, String, String> atSpan =
        (trip, spanHex) -> {
            if (trip == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer sptr = functions.span_from_hexwkb(spanHex);
            if (tptr == null || sptr == null) return null;
            Pointer result = functions.temporal_at_tstzspan(tptr, sptr);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // atSpanset(trip STRING, spansetHex STRING) → STRING
    // MEOS: temporal_at_tstzspanset(const Temporal *, const SpanSet *) → Temporal *
    public static final UDF2<String, String, String> atSpanset =
        (trip, spansetHex) -> {
            if (trip == null || spansetHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer ssptr = functions.spanset_from_hexwkb(spansetHex);
            if (tptr == null || ssptr == null) return null;
            Pointer result = functions.temporal_at_tstzspanset(tptr, ssptr);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // insert(trip1 STRING, trip2 STRING) → STRING
    // MEOS: temporal_insert(const Temporal *, const Temporal *, bool connect) → Temporal *
    public static final UDF2<String, String, String> insert =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            Pointer result = functions.temporal_insert(p1, p2, true);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // update(trip1 STRING, trip2 STRING) → STRING
    // MEOS: temporal_update(const Temporal *, const Temporal *, bool connect) → Temporal *
    public static final UDF2<String, String, String> update =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            Pointer result = functions.temporal_update(p1, p2, true);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Type-specific value accessors (return type matches the base type)
    // ------------------------------------------------------------------

    // tfloatStartValue(trip STRING) → DOUBLE
    // MEOS: tfloat_start_value(const Temporal *) → double
    public static final UDF1<String, Double> tfloatStartValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tfloat_start_value(ptr);
        };

    // tfloatEndValue(trip STRING) → DOUBLE
    // MEOS: tfloat_end_value(const Temporal *) → double
    public static final UDF1<String, Double> tfloatEndValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tfloat_end_value(ptr);
        };

    // tfloatMinValue(trip STRING) → DOUBLE
    // MEOS: tfloat_min_value(const Temporal *) → double
    public static final UDF1<String, Double> tfloatMinValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tfloat_min_value(ptr);
        };

    // tfloatMaxValue(trip STRING) → DOUBLE
    // MEOS: tfloat_max_value(const Temporal *) → double
    public static final UDF1<String, Double> tfloatMaxValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tfloat_max_value(ptr);
        };

    // tintStartValue(trip STRING) → INT
    // MEOS: tint_start_value(const Temporal *) → int
    public static final UDF1<String, Integer> tintStartValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tint_start_value(ptr);
        };

    // tintEndValue(trip STRING) → INT
    // MEOS: tint_end_value(const Temporal *) → int
    public static final UDF1<String, Integer> tintEndValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tint_end_value(ptr);
        };

    // tintMinValue(trip STRING) → INT
    // MEOS: tint_min_value(const Temporal *) → int
    public static final UDF1<String, Integer> tintMinValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tint_min_value(ptr);
        };

    // tintMaxValue(trip STRING) → INT
    // MEOS: tint_max_value(const Temporal *) → int
    public static final UDF1<String, Integer> tintMaxValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tint_max_value(ptr);
        };

    // tboolStartValue(trip STRING) → BOOLEAN
    // MEOS: tbool_start_value(const Temporal *) → bool
    public static final UDF1<String, Boolean> tboolStartValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tbool_start_value(ptr);
        };

    // tboolEndValue(trip STRING) → BOOLEAN
    // MEOS: tbool_end_value(const Temporal *) → bool
    public static final UDF1<String, Boolean> tboolEndValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tbool_end_value(ptr);
        };

    // tpointStartValue(trip STRING) → STRING  (WKT of start geometry)
    // MEOS: tpoint_start_value(const Temporal *) → GSERIALIZED *
    public static final UDF1<String, String> tpointStartValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer gs = functions.tgeo_start_value(ptr);
            if (gs == null) return null;
            return functions.geo_as_text(gs, 6);
        };

    // tpointEndValue(trip STRING) → STRING  (WKT of end geometry)
    // MEOS: tpoint_end_value(const Temporal *) → GSERIALIZED *
    public static final UDF1<String, String> tpointEndValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer gs = functions.tgeo_end_value(ptr);
            if (gs == null) return null;
            return functions.geo_as_text(gs, 6);
        };

    // ttextStartValue(trip STRING) → STRING  (text value at start)
    // MEOS: ttext_start_value(const Temporal *) → text *
    public static final UDF1<String, String> ttextStartValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer txt = functions.ttext_start_value(ptr);
            if (txt == null) return null;
            return functions.text_out(txt);
        };

    // ttextEndValue(trip STRING) → STRING  (text value at end)
    // MEOS: ttext_end_value(const Temporal *) → text *
    public static final UDF1<String, String> ttextEndValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer txt = functions.ttext_end_value(ptr);
            if (txt == null) return null;
            return functions.text_out(txt);
        };

    // ------------------------------------------------------------------
    // Value restriction: atMin / atMax / atValues / minusTime / minusMin / minusMax
    // ------------------------------------------------------------------

    // atMin(trip STRING) → STRING
    public static final UDF1<String, String> atMin =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer r = functions.temporal_at_min(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // atMax(trip STRING) → STRING
    public static final UDF1<String, String> atMax =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer r = functions.temporal_at_max(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // atValues(trip STRING, setHex STRING) → STRING
    public static final UDF2<String, String, String> atValues =
        (trip, setHex) -> {
            if (trip == null || setHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer sptr = functions.set_from_hexwkb(setHex);
            if (tptr == null || sptr == null) return null;
            Pointer r = functions.temporal_at_values(tptr, sptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // minusTime(trip STRING, tstzspanHex STRING) → STRING
    public static final UDF2<String, String, String> minusTime =
        (trip, spanHex) -> {
            if (trip == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer sptr = functions.span_from_hexwkb(spanHex);
            if (tptr == null || sptr == null) return null;
            Pointer r = functions.temporal_minus_tstzspan(tptr, sptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // minusMin(trip STRING) → STRING
    public static final UDF1<String, String> minusMin =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer r = functions.temporal_minus_min(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // minusMax(trip STRING) → STRING
    public static final UDF1<String, String> minusMax =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer r = functions.temporal_minus_max(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Spatio-temporal restriction: atStbox / minusStbox / tnumberAtTbox / tnumberMinusTbox
    // ------------------------------------------------------------------

    // atStbox(trip STRING, stboxHex STRING) → STRING
    public static final UDF2<String, String, String> atStbox =
        (trip, stboxHex) -> {
            if (trip == null || stboxHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer bptr = functions.stbox_from_hexwkb(stboxHex);
            if (tptr == null || bptr == null) return null;
            Pointer r = functions.tgeo_at_stbox(tptr, bptr, true);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // minusStbox(trip STRING, stboxHex STRING) → STRING
    public static final UDF2<String, String, String> minusStbox =
        (trip, stboxHex) -> {
            if (trip == null || stboxHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer bptr = functions.stbox_from_hexwkb(stboxHex);
            if (tptr == null || bptr == null) return null;
            Pointer r = functions.tgeo_minus_stbox(tptr, bptr, true);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tnumberAtTbox(trip STRING, tboxHex STRING) → STRING
    public static final UDF2<String, String, String> tnumberAtTbox =
        (trip, tboxHex) -> {
            if (trip == null || tboxHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer bptr = functions.tbox_from_hexwkb(tboxHex);
            if (tptr == null || bptr == null) return null;
            Pointer r = functions.tnumber_at_tbox(tptr, bptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tnumberMinusTbox(trip STRING, tboxHex STRING) → STRING
    public static final UDF2<String, String, String> tnumberMinusTbox =
        (trip, tboxHex) -> {
            if (trip == null || tboxHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer bptr = functions.tbox_from_hexwkb(tboxHex);
            if (tptr == null || bptr == null) return null;
            Pointer r = functions.tnumber_minus_tbox(tptr, bptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Append operations
    // ------------------------------------------------------------------

    // appendInstant(trip STRING, instantHex STRING) → STRING
    public static final UDF2<String, String, String> appendInstant =
        (trip, instantHex) -> {
            if (trip == null || instantHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer iptr = functions.temporal_from_hexwkb(instantHex);
            if (tptr == null || iptr == null) return null;
            Pointer r = functions.temporal_append_tinstant(tptr, iptr, 3 /* LINEAR */, 0.0, null, false);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // appendSequence(trip STRING, seqHex STRING) → STRING
    public static final UDF2<String, String, String> appendSequence =
        (trip, seqHex) -> {
            if (trip == null || seqHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer sptr = functions.temporal_from_hexwkb(seqHex);
            if (tptr == null || sptr == null) return null;
            Pointer r = functions.temporal_append_tsequence(tptr, sptr, false);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Value span: tnumberValuespans, tnumberToSpan, tnumberToTbox
    // ------------------------------------------------------------------

    // tnumberValuespans(trip STRING) → STRING  (hex-WKB of value spanset)
    public static final UDF1<String, String> tnumberValuespans =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer ss = functions.tnumber_valuespans(ptr);
            if (ss == null) return null;
            return functions.spanset_as_hexwkb(ss, (byte) 0);
        };

    // tnumberToSpan(trip STRING) → STRING  (hex-WKB of value span covering all values)
    // MEOS: tnumber_to_span(const Temporal *) → Span *
    public static final UDF1<String, String> tnumberToSpan =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer sp = functions.tnumber_to_span(ptr);
                if (sp == null) return null;
                try {
                    return functions.span_as_hexwkb(sp, (byte) 0);
                } finally {
                    MeosMemory.free(sp);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tnumberToTbox(trip STRING) → STRING  (hex-WKB of TBox bounding box)
    // MEOS: tnumber_to_tbox(const Temporal *) → TBox *
    public static final UDF1<String, String> tnumberToTbox =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer tb = functions.tnumber_to_tbox(ptr);
                if (tb == null) return null;
                try {
                    jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                    jnr.ffi.Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    return functions.tbox_as_hexwkb(tb, (byte) 0, sizeOut);
                } finally {
                    MeosMemory.free(tb);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    public static void registerAll(SparkSession spark) {
        // Type-agnostic
        spark.udf().register("numSequences",     numSequences,     DataTypes.IntegerType);
        spark.udf().register("interp",            interp,           DataTypes.StringType);
        spark.udf().register("time",              time,             DataTypes.StringType);
        spark.udf().register("timespan",          timespan,         DataTypes.StringType);
        spark.udf().register("merge",             merge,            DataTypes.StringType);
        spark.udf().register("shift",             shift,            DataTypes.StringType);
        spark.udf().register("scale",             scale,            DataTypes.StringType);
        spark.udf().register("atSpan",            atSpan,           DataTypes.StringType);
        spark.udf().register("atSpanset",         atSpanset,        DataTypes.StringType);
        spark.udf().register("insert",            insert,           DataTypes.StringType);
        spark.udf().register("update",            update,           DataTypes.StringType);
        // Type-specific float
        spark.udf().register("tfloatStartValue",  tfloatStartValue, DataTypes.DoubleType);
        spark.udf().register("tfloatEndValue",    tfloatEndValue,   DataTypes.DoubleType);
        spark.udf().register("tfloatMinValue",    tfloatMinValue,   DataTypes.DoubleType);
        spark.udf().register("tfloatMaxValue",    tfloatMaxValue,   DataTypes.DoubleType);
        // Type-specific int
        spark.udf().register("tintStartValue",    tintStartValue,   DataTypes.IntegerType);
        spark.udf().register("tintEndValue",      tintEndValue,     DataTypes.IntegerType);
        spark.udf().register("tintMinValue",      tintMinValue,     DataTypes.IntegerType);
        spark.udf().register("tintMaxValue",      tintMaxValue,     DataTypes.IntegerType);
        // Type-specific bool
        spark.udf().register("tboolStartValue",   tboolStartValue,  DataTypes.BooleanType);
        spark.udf().register("tboolEndValue",     tboolEndValue,    DataTypes.BooleanType);
        // Type-specific point (returns WKT)
        spark.udf().register("tpointStartValue",  tpointStartValue, DataTypes.StringType);
        spark.udf().register("tpointEndValue",    tpointEndValue,   DataTypes.StringType);
        // Type-specific text
        spark.udf().register("ttextStartValue",   ttextStartValue,  DataTypes.StringType);
        spark.udf().register("ttextEndValue",     ttextEndValue,    DataTypes.StringType);
        // MobilityDB SQL bare-name `getValue` aliases — for an instant temporal,
        // value-at-instant === start-value. Per-type variants for type safety.
        spark.udf().register("tintGetValue",       tintStartValue,   DataTypes.IntegerType);
        spark.udf().register("tfloatGetValue",     tfloatStartValue, DataTypes.DoubleType);
        spark.udf().register("tboolGetValue",      tboolStartValue,  DataTypes.BooleanType);
        spark.udf().register("ttextGetValue",      ttextStartValue,  DataTypes.StringType);
        spark.udf().register("tpointGetValue",     tpointStartValue, DataTypes.StringType);
        // Bare-name alias defaults to tfloat (most common analytics case)
        spark.udf().register("getValue",           tfloatStartValue, DataTypes.DoubleType);
        // Value restriction
        spark.udf().register("atMin",             atMin,            DataTypes.StringType);
        spark.udf().register("atMax",             atMax,            DataTypes.StringType);
        spark.udf().register("atValues",          atValues,         DataTypes.StringType);
        spark.udf().register("minusTime",         minusTime,        DataTypes.StringType);
        spark.udf().register("minusMin",          minusMin,         DataTypes.StringType);
        spark.udf().register("minusMax",          minusMax,         DataTypes.StringType);
        // Spatio-temporal restriction
        spark.udf().register("atStbox",           atStbox,          DataTypes.StringType);
        spark.udf().register("minusStbox",        minusStbox,       DataTypes.StringType);
        spark.udf().register("tnumberAtTbox",     tnumberAtTbox,    DataTypes.StringType);
        spark.udf().register("tnumberMinusTbox",  tnumberMinusTbox, DataTypes.StringType);
        // Append
        spark.udf().register("appendInstant",     appendInstant,    DataTypes.StringType);
        spark.udf().register("appendSequence",    appendSequence,   DataTypes.StringType);
        // Value spans
        spark.udf().register("tnumberValuespans", tnumberValuespans, DataTypes.StringType);
        spark.udf().register("tnumberToSpan",     tnumberToSpan,     DataTypes.StringType);
        spark.udf().register("tnumberToTbox",     tnumberToTbox,     DataTypes.StringType);
    }
}
