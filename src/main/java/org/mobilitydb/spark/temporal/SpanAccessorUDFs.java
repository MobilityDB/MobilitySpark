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
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.time.OffsetDateTime;

/**
 * Spark SQL UDFs for span, spanset, and set bound/count accessors.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class SpanAccessorUDFs {

    private SpanAccessorUDFs() {}

    // milliseconds from Unix epoch (1970-01-01) to PG epoch (2000-01-01)
    private static final long PG_UNIX_OFFSET_MS   = 946684800L * 1000L;
    // days from Unix epoch (1970-01-01) to PG epoch (2000-01-01)
    private static final long PG_UNIX_OFFSET_DAYS = 10957L;

    // tstzspan_lower/upper returns OffsetDateTime where toEpochSecond()
    // holds raw PG-epoch microseconds; divide by 1000 then add PG→Unix offset
    private static java.sql.Timestamp odtToTimestamp(OffsetDateTime odt) {
        if (odt == null) return null;
        return new java.sql.Timestamp(odt.toEpochSecond() / 1000L + PG_UNIX_OFFSET_MS);
    }

    // ------------------------------------------------------------------
    // intspan accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> intspanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.intspan_lower(p);
        };

    public static final UDF1<String, Integer> intspanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.intspan_upper(p);
        };

    public static final UDF1<String, Integer> intspanWidth =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.intspan_width(p);
        };

    // ------------------------------------------------------------------
    // floatspan accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> floatspanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.floatspan_lower(p);
        };

    public static final UDF1<String, Double> floatspanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.floatspan_upper(p);
        };

    public static final UDF1<String, Double> floatspanWidth =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.floatspan_width(p);
        };

    // ------------------------------------------------------------------
    // bigintspan accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Long> bigintspanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.bigintspan_lower(p);
        };

    public static final UDF1<String, Long> bigintspanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.bigintspan_upper(p);
        };

    // ------------------------------------------------------------------
    // datespan accessors (MEOS returns int = days from PG epoch 2000-01-01)
    // ------------------------------------------------------------------

    public static final UDF1<String, java.sql.Date> datespanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            int days = functions.datespan_lower(p);
            return new java.sql.Date((days + PG_UNIX_OFFSET_DAYS) * 86400000L);
        };

    public static final UDF1<String, java.sql.Date> datespanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            int days = functions.datespan_upper(p);
            return new java.sql.Date((days + PG_UNIX_OFFSET_DAYS) * 86400000L);
        };

    // ------------------------------------------------------------------
    // tstzspan accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, java.sql.Timestamp> tstzspanLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return odtToTimestamp(functions.tstzspan_lower(p));
        };

    public static final UDF1<String, java.sql.Timestamp> tstzspanUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return odtToTimestamp(functions.tstzspan_upper(p));
        };

    // ------------------------------------------------------------------
    // Generic span inclusivity flags
    // ------------------------------------------------------------------

    public static final UDF1<String, Boolean> spanLowerInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.span_lower_inc(p);
        };

    public static final UDF1<String, Boolean> spanUpperInc =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            return functions.span_upper_inc(p);
        };

    // ------------------------------------------------------------------
    // Spanset accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> spansetNumSpans =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            return functions.spanset_num_spans(p);
        };

    public static final UDF1<String, String> spansetStartSpan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            Pointer span = functions.spanset_start_span(p);
            if (span == null) return null;
            return functions.span_as_hexwkb(span, (byte) 0);
        };

    public static final UDF1<String, String> spansetEndSpan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            Pointer span = functions.spanset_end_span(p);
            if (span == null) return null;
            return functions.span_as_hexwkb(span, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Set accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> setNumValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            return functions.set_num_values(p);
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
        spark.udf().register("spansetNumSpans",  spansetNumSpans,  DataTypes.IntegerType);
        spark.udf().register("spansetStartSpan", spansetStartSpan, DataTypes.StringType);
        spark.udf().register("spansetEndSpan",   spansetEndSpan,   DataTypes.StringType);
        spark.udf().register("setNumValues",     setNumValues,     DataTypes.IntegerType);
    }
}
