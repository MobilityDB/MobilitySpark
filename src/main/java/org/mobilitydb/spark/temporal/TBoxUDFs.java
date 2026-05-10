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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosThread;

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
    }
}
