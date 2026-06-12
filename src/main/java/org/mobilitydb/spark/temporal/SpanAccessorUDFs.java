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
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import org.mobilitydb.spark.util.TimeUtil;

/**
 * Spark SQL UDFs for span, spanset, and set bound/count accessors.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class SpanAccessorUDFs {

    private SpanAccessorUDFs() {}

    // milliseconds from Unix epoch (1970-01-01) to PG epoch (2000-01-01)
    // days from Unix epoch (1970-01-01) to PG epoch (2000-01-01)
    // PG_UNIX_OFFSET_DAYS moved to org.mobilitydb.spark.util.TimeUtil.PG_UNIX_EPOCH_OFFSET_DAYS

    // tstzspan_lower/upper returns OffsetDateTime where toEpochSecond()
    // holds raw PG-epoch microseconds; divide by 1000 then add PG→Unix offset
    private static java.sql.Timestamp odtToTimestamp(OffsetDateTime odt) {
        if (odt == null) return null;
        return new java.sql.Timestamp(odt.toEpochSecond() / 1000L + TimeUtil.PG_UNIX_EPOCH_OFFSET_MS);
    }

    // ------------------------------------------------------------------
    // intspan accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> intspanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.intspan_lower(p);
        };

    public static final UDF1<String, Integer> intspanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.intspan_upper(p);
        };

    public static final UDF1<String, Integer> intspanWidth =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.intspan_width(p);
        };

    // ------------------------------------------------------------------
    // floatspan accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> floatspanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.floatspan_lower(p);
        };

    public static final UDF1<String, Double> floatspanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.floatspan_upper(p);
        };

    public static final UDF1<String, Double> floatspanWidth =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.floatspan_width(p);
        };

    // ------------------------------------------------------------------
    // bigintspan accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Long> bigintspanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.bigintspan_lower(p);
        };

    public static final UDF1<String, Long> bigintspanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.bigintspan_upper(p);
        };

    // ------------------------------------------------------------------
    // datespan accessors (MEOS returns int = days from PG epoch 2000-01-01)
    // ------------------------------------------------------------------

    public static final UDF1<String, java.sql.Date> datespanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            int days = GeneratedFunctions.datespan_lower(p);
            return new java.sql.Date((days + TimeUtil.PG_UNIX_EPOCH_OFFSET_DAYS) * 86400000L);
        };

    public static final UDF1<String, java.sql.Date> datespanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            int days = GeneratedFunctions.datespan_upper(p);
            return new java.sql.Date((days + TimeUtil.PG_UNIX_EPOCH_OFFSET_DAYS) * 86400000L);
        };

    // ------------------------------------------------------------------
    // tstzspan accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, java.sql.Timestamp> tstzspanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return odtToTimestamp(GeneratedFunctions.tstzspan_lower(p));
        };

    public static final UDF1<String, java.sql.Timestamp> tstzspanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return odtToTimestamp(GeneratedFunctions.tstzspan_upper(p));
        };

    // ------------------------------------------------------------------
    // Generic span inclusivity flags
    // ------------------------------------------------------------------

    public static final UDF1<String, Boolean> spanLowerInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.span_lower_inc(p);
        };

    public static final UDF1<String, Boolean> spanUpperInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.span_upper_inc(p);
        };

    // ------------------------------------------------------------------
    // Spanset accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> spansetNumSpans =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.spanset_num_spans(p);
        };

    public static final UDF1<String, String> spansetStartSpan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            Pointer span = GeneratedFunctions.spanset_start_span(p);
            if (span == null) return null;
            return GeneratedFunctions.span_as_hexwkb(span, (byte) 0);
        };

    public static final UDF1<String, String> spansetEndSpan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            Pointer span = GeneratedFunctions.spanset_end_span(p);
            if (span == null) return null;
            return GeneratedFunctions.span_as_hexwkb(span, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Spanset inclusivity flags
    // ------------------------------------------------------------------

    // spansetLowerInc(hex STRING) → BOOLEAN  (lower bound of the first span)
    // MEOS: spanset_lower_inc(const SpanSet *) → bool
    public static final UDF1<String, Boolean> spansetLowerInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.spanset_lower_inc(p);
        };

    // spansetUpperInc(hex STRING) → BOOLEAN  (upper bound of the last span)
    // MEOS: spanset_upper_inc(const SpanSet *) → bool
    public static final UDF1<String, Boolean> spansetUpperInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.spanset_upper_inc(p);
        };

    // ------------------------------------------------------------------
    // Span-to-spanset conversion
    // ------------------------------------------------------------------

    // spanToSpanset(hex STRING) → STRING  (wrap a span in a single-element spanset)
    // MEOS: span_to_spanset(const Span *) → SpanSet *
    public static final UDF1<String, String> spanToSpanset =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.span_from_hexwkb(hex);
            if (p == null) return null;
            Pointer ss = GeneratedFunctions.span_to_spanset(p);
            if (ss == null) return null;
            return GeneratedFunctions.spanset_as_hexwkb(ss, (byte) 0);
        };

    // ------------------------------------------------------------------
    // TstzSpanSet temporal boundary accessors
    //
    // MEOS: tstzspanset_lower, tstzspanset_upper,
    //       tstzspanset_start_timestamptz, tstzspanset_end_timestamptz
    // All return OffsetDateTime (PG-epoch microseconds via toEpochSecond()).
    // ------------------------------------------------------------------

    public static final UDF1<String, java.sql.Timestamp> tstzspansetLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            return odtToTimestamp(GeneratedFunctions.tstzspanset_lower(p));
        };

    public static final UDF1<String, java.sql.Timestamp> tstzspansetUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            return odtToTimestamp(GeneratedFunctions.tstzspanset_upper(p));
        };

    public static final UDF1<String, java.sql.Timestamp> tstzspansetStartTimestamptz =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            return odtToTimestamp(GeneratedFunctions.tstzspanset_start_timestamptz(p));
        };

    public static final UDF1<String, java.sql.Timestamp> tstzspansetEndTimestamptz =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            return odtToTimestamp(GeneratedFunctions.tstzspanset_end_timestamptz(p));
        };

    // ------------------------------------------------------------------
    // Set accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> setNumValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            return GeneratedFunctions.set_num_values(p);
        };

    // ------------------------------------------------------------------
    // intset value accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> intsetStartValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return GeneratedFunctions.intset_start_value(p); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, Integer> intsetEndValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return GeneratedFunctions.intset_end_value(p); }
            finally { MeosMemory.free(p); }
        };

    // intset_values(Set *) → int *  (palloc'd int32 array, count via set_num_values)
    public static final UDF1<String, List<Integer>> intsetValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = GeneratedFunctions.set_num_values(p);
                Pointer arr = GeneratedFunctions.intset_values(p, Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4));
                if (arr == null) return null;
                try {
                    List<Integer> result = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) result.add(arr.getInt((long) i * 4));
                    return result;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // floatset value accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> floatsetStartValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return GeneratedFunctions.floatset_start_value(p); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, Double> floatsetEndValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return GeneratedFunctions.floatset_end_value(p); }
            finally { MeosMemory.free(p); }
        };

    // floatset_values(Set *) → double *
    public static final UDF1<String, List<Double>> floatsetValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = GeneratedFunctions.set_num_values(p);
                Pointer arr = GeneratedFunctions.floatset_values(p, Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4));
                if (arr == null) return null;
                try {
                    List<Double> result = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) result.add(arr.getDouble((long) i * 8));
                    return result;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // dateset value accessors  (int32 = days from PG epoch 2000-01-01)
    // ------------------------------------------------------------------

    public static final UDF1<String, java.sql.Date> datesetStartValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int days = GeneratedFunctions.dateset_start_value(p);
                return new java.sql.Date((days + TimeUtil.PG_UNIX_EPOCH_OFFSET_DAYS) * 86400000L);
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, java.sql.Date> datesetEndValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int days = GeneratedFunctions.dateset_end_value(p);
                return new java.sql.Date((days + TimeUtil.PG_UNIX_EPOCH_OFFSET_DAYS) * 86400000L);
            } finally { MeosMemory.free(p); }
        };

    // dateset_values(Set *) → int *  (int32 days from PG epoch per element)
    public static final UDF1<String, List<java.sql.Date>> datesetValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = GeneratedFunctions.set_num_values(p);
                Pointer arr = GeneratedFunctions.dateset_values(p, Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4));
                if (arr == null) return null;
                try {
                    List<java.sql.Date> result = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        int days = arr.getInt((long) i * 4);
                        result.add(new java.sql.Date((days + TimeUtil.PG_UNIX_EPOCH_OFFSET_DAYS) * 86400000L));
                    }
                    return result;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // tstzset value accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, java.sql.Timestamp> tstzsetStartValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return odtToTimestamp(GeneratedFunctions.tstzset_start_value(p)); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, java.sql.Timestamp> tstzsetEndValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return odtToTimestamp(GeneratedFunctions.tstzset_end_value(p)); }
            finally { MeosMemory.free(p); }
        };

    // tstzset_values(Set *) → int64 * (PG-epoch microseconds per element)
    public static final UDF1<String, List<java.sql.Timestamp>> tstzsetValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = GeneratedFunctions.set_num_values(p);
                Pointer arr = GeneratedFunctions.tstzset_values(p, Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4));
                if (arr == null) return null;
                try {
                    List<java.sql.Timestamp> result = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        long pgMicros = arr.getLong((long) i * 8);
                        result.add(new java.sql.Timestamp(pgMicros / 1000L + TimeUtil.PG_UNIX_EPOCH_OFFSET_MS));
                    }
                    return result;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // textset value accessors  (text * elements → String via text_out)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> textsetStartValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer textPtr = GeneratedFunctions.textset_start_value(p);
                if (textPtr == null) return null;
                return GeneratedFunctions.text_out(textPtr);
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String> textsetEndValue =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer textPtr = GeneratedFunctions.textset_end_value(p);
                if (textPtr == null) return null;
                return GeneratedFunctions.text_out(textPtr);
            } finally { MeosMemory.free(p); }
        };

    // textset_values(Set *) → text **  (pointer array; elements are views — do NOT free)
    public static final UDF1<String, List<String>> textsetValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = GeneratedFunctions.set_num_values(p);
                Pointer arr = GeneratedFunctions.textset_values(p, Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4));
                if (arr == null) return null;
                try {
                    List<String> result = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        Pointer textPtr = arr.getPointer((long) i * 8);
                        if (textPtr != null) result.add(GeneratedFunctions.text_out(textPtr));
                    }
                    return result;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    public static void registerAll(SparkSession spark) {
        spark.udf().register("intspanLower",     intspanLower,     DataTypes.IntegerType);
        spark.udf().register("intspanUpper",     intspanUpper,     DataTypes.IntegerType);
        spark.udf().register("intspanWidth",     intspanWidth,     DataTypes.IntegerType);
        spark.udf().register("floatspanLower",   floatspanLower,   DataTypes.DoubleType);
        spark.udf().register("floatspanUpper",   floatspanUpper,   DataTypes.DoubleType);
        spark.udf().register("floatspanWidth",   floatspanWidth,   DataTypes.DoubleType);
        spark.udf().register("bigintspanLower",  bigintspanLower,  DataTypes.LongType);
        spark.udf().register("bigintspanUpper",  bigintspanUpper,  DataTypes.LongType);
        spark.udf().register("datespanLower",    datespanLower,    DataTypes.DateType);
        spark.udf().register("datespanUpper",    datespanUpper,    DataTypes.DateType);
        spark.udf().register("tstzspanLower",    tstzspanLower,    DataTypes.TimestampType);
        spark.udf().register("tstzspanUpper",    tstzspanUpper,    DataTypes.TimestampType);
        spark.udf().register("spanLowerInc",     spanLowerInc,     DataTypes.BooleanType);
        spark.udf().register("spanUpperInc",     spanUpperInc,     DataTypes.BooleanType);
        spark.udf().register("spansetNumSpans",   spansetNumSpans,   DataTypes.IntegerType);
        spark.udf().register("spansetStartSpan", spansetStartSpan,  DataTypes.StringType);
        spark.udf().register("spansetEndSpan",   spansetEndSpan,    DataTypes.StringType);
        spark.udf().register("spansetLowerInc",  spansetLowerInc,   DataTypes.BooleanType);
        spark.udf().register("spansetUpperInc",  spansetUpperInc,   DataTypes.BooleanType);
        spark.udf().register("spanToSpanset",    spanToSpanset,     DataTypes.StringType);
        spark.udf().register("setNumValues",      setNumValues,      DataTypes.IntegerType);
        // intset value accessors
        spark.udf().register("intsetStartValue", intsetStartValue,  DataTypes.IntegerType);
        spark.udf().register("intsetEndValue",   intsetEndValue,    DataTypes.IntegerType);
        spark.udf().register("intsetValues",     intsetValues,
            DataTypes.createArrayType(DataTypes.IntegerType));
        // floatset value accessors
        spark.udf().register("floatsetStartValue", floatsetStartValue, DataTypes.DoubleType);
        spark.udf().register("floatsetEndValue",   floatsetEndValue,   DataTypes.DoubleType);
        spark.udf().register("floatsetValues",     floatsetValues,
            DataTypes.createArrayType(DataTypes.DoubleType));
        // dateset value accessors
        spark.udf().register("datesetStartValue", datesetStartValue, DataTypes.DateType);
        spark.udf().register("datesetEndValue",   datesetEndValue,   DataTypes.DateType);
        spark.udf().register("datesetValues",     datesetValues,
            DataTypes.createArrayType(DataTypes.DateType));
        // tstzset value accessors
        spark.udf().register("tstzsetStartValue", tstzsetStartValue, DataTypes.TimestampType);
        spark.udf().register("tstzsetEndValue",   tstzsetEndValue,   DataTypes.TimestampType);
        spark.udf().register("tstzsetValues",     tstzsetValues,
            DataTypes.createArrayType(DataTypes.TimestampType));
        // textset value accessors
        spark.udf().register("textsetStartValue", textsetStartValue, DataTypes.StringType);
        spark.udf().register("textsetEndValue",   textsetEndValue,   DataTypes.StringType);
        spark.udf().register("textsetValues",     textsetValues,
            DataTypes.createArrayType(DataTypes.StringType));
        // TstzSpanSet temporal boundary accessors
        spark.udf().register("tstzspansetLower",             tstzspansetLower,             DataTypes.TimestampType);
        spark.udf().register("tstzspansetUpper",             tstzspansetUpper,             DataTypes.TimestampType);
        spark.udf().register("tstzspansetStartTimestamptz",  tstzspansetStartTimestamptz,  DataTypes.TimestampType);
        spark.udf().register("tstzspansetEndTimestamptz",    tstzspansetEndTimestamptz,    DataTypes.TimestampType);
        // spanset nth-span accessor
        spark.udf().register("spansetSpanN", spansetSpanN, DataTypes.StringType);
        // intspanset / floatspanset bound accessors
        spark.udf().register("intspansetLower",    intspansetLower,    DataTypes.IntegerType);
        spark.udf().register("intspansetUpper",    intspansetUpper,    DataTypes.IntegerType);
        spark.udf().register("intspansetWidth",    intspansetWidth,    DataTypes.IntegerType);
        spark.udf().register("floatspansetLower",  floatspansetLower,  DataTypes.DoubleType);
        spark.udf().register("floatspansetUpper",  floatspansetUpper,  DataTypes.DoubleType);
        spark.udf().register("floatspansetWidth",  floatspansetWidth,  DataTypes.DoubleType);
        // tstzspanset extra accessors
        spark.udf().register("tstzspansetNumTimestamps", tstzspansetNumTimestamps, DataTypes.IntegerType);
        spark.udf().register("tstzspansetTimestamps",    tstzspansetTimestamps,
            DataTypes.createArrayType(DataTypes.TimestampType));
        spark.udf().register("tstzspansetDuration",      tstzspansetDuration,      DataTypes.StringType);
    }

    // ------------------------------------------------------------------
    // ------------------------------------------------------------------
    // intspanset / floatspanset bound accessors
    // ------------------------------------------------------------------

    // intspansetLower(hex STRING) → INTEGER
    // MEOS: intspanset_lower(const SpanSet *) → int
    public static final UDF1<String, Integer> intspansetLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return GeneratedFunctions.intspanset_lower(p); }
            finally { MeosMemory.free(p); }
        };

    // intspansetUpper(hex STRING) → INTEGER
    // MEOS: intspanset_upper(const SpanSet *) → int
    public static final UDF1<String, Integer> intspansetUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return GeneratedFunctions.intspanset_upper(p); }
            finally { MeosMemory.free(p); }
        };

    // intspansetWidth(hex STRING, ignoreGaps BOOLEAN) → INTEGER
    // MEOS: intspanset_width(const SpanSet *, bool) → int
    public static final UDF2<String, Boolean, Integer> intspansetWidth =
        (hex, ignoreGaps) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try {
                return GeneratedFunctions.intspanset_width(p, ignoreGaps != null && ignoreGaps);
            } finally { MeosMemory.free(p); }
        };

    // floatspansetLower(hex STRING) → DOUBLE
    // Workaround: floatspanset_lower in MEOS uses Float8GetDatum (wrong direction),
    // so we extract the first span and use floatspan_lower which is correct.
    public static final UDF1<String, Double> floatspansetLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer firstSpan = GeneratedFunctions.spanset_start_span(p);
                if (firstSpan == null) return null;
                return GeneratedFunctions.floatspan_lower(firstSpan);
            } finally { MeosMemory.free(p); }
        };

    // floatspansetUpper(hex STRING) → DOUBLE
    // Workaround: same Float8GetDatum bug as floatspanset_lower.
    // Uses spanset_end_span + floatspan_upper instead.
    public static final UDF1<String, Double> floatspansetUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer lastSpan = GeneratedFunctions.spanset_end_span(p);
                if (lastSpan == null) return null;
                return GeneratedFunctions.floatspan_upper(lastSpan);
            } finally { MeosMemory.free(p); }
        };

    // floatspansetWidth(hex STRING, ignoreGaps BOOLEAN) → DOUBLE
    // MEOS: floatspanset_width(const SpanSet *, bool) → double
    public static final UDF2<String, Boolean, Double> floatspansetWidth =
        (hex, ignoreGaps) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try {
                return GeneratedFunctions.floatspanset_width(p, ignoreGaps != null && ignoreGaps);
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // tstzspanset extra accessors
    // ------------------------------------------------------------------

    // tstzspansetNumTimestamps(hex STRING) → INTEGER
    // MEOS: tstzspanset_num_timestamps(const SpanSet *) → int
    public static final UDF1<String, Integer> tstzspansetNumTimestamps =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return GeneratedFunctions.tstzspanset_num_timestamps(p); }
            finally { MeosMemory.free(p); }
        };

    // tstzspansetTimestamps(hex STRING) → ARRAY<TIMESTAMP>
    // MEOS: tstzspanset_timestamps(const SpanSet *) → TimestampTz *
    // Returns int64* array (PG-epoch microseconds); count via tstzspanset_num_timestamps.
    public static final UDF1<String, List<java.sql.Timestamp>> tstzspansetTimestamps =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = GeneratedFunctions.tstzspanset_num_timestamps(p);
                Pointer arr = GeneratedFunctions.tstzspanset_timestamps(p);
                if (arr == null) return null;
                try {
                    List<java.sql.Timestamp> result = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        long pgMicros = arr.getLong((long) i * 8);
                        result.add(new java.sql.Timestamp(pgMicros / 1000L + TimeUtil.PG_UNIX_EPOCH_OFFSET_MS));
                    }
                    return result;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    // tstzspansetDuration(hex STRING, ignoreGaps BOOLEAN) → STRING (interval)
    // MEOS: tstzspanset_duration(const SpanSet *, bool) → Interval *
    public static final UDF2<String, Boolean, String> tstzspansetDuration =
        (hex, ignoreGaps) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            boolean ignore = (ignoreGaps != null && ignoreGaps);
            Pointer p = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer iv = GeneratedFunctions.tstzspanset_duration(p, ignore);
                if (iv == null) return null;
                try { return GeneratedFunctions.pg_interval_out(iv); }
                finally { MeosMemory.free(iv); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // spanset_span_n(spanset, n) → span hex-WKB  (1-based index)
    // MEOS: spanset_span_n(SpanSet *, int) → Span * (view — must NOT free)
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> spansetSpanN =
        (hex, n) -> {
            if (hex == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ss = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (ss == null) return null;
            try {
                Pointer span = GeneratedFunctions.spanset_span_n(ss, n);
                if (span == null) return null;
                return GeneratedFunctions.span_as_hexwkb(span, (byte) 0);
            } finally {
                MeosMemory.free(ss);
            }
        };
}
