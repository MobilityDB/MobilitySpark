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

package org.mobilitydb.spark.geo;

import functions.functions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for STBox accessor and expansion operations.
 *
 * Storage convention: STBox values are stored as hex-WKB strings produced by
 * stbox_as_hexwkb (which requires a non-null size_out scratch Pointer).
 *
 * Spatial bound accessors (xmin/xmax/ymin/ymax/zmin/zmax): the JMEOS wrapper
 * allocates an 8-byte buffer, passes it as out-pointer to the C function which
 * writes the double there, and returns the buffer Pointer (null = absent).
 * Temporal bound accessors (tmin/tmax): same pattern, int64 PG-epoch μs.
 * Inclusivity flag accessors (tmin_inc/tmax_inc): same pattern, byte (0/1).
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
public final class STBoxUDFs {

    private STBoxUDFs() {}

    // milliseconds from Unix epoch (1970-01-01) to PG epoch (2000-01-01)
    private static final long PG_UNIX_OFFSET_MS = 946684800L * 1000L;

    private static Pointer stboxPtr(String hex) {
        if (hex == null) return null;
        return functions.stbox_from_hexwkb(hex);
    }

    // stbox_as_hexwkb requires a non-null size_out scratch Pointer
    private static String stboxHex(Pointer p) {
        if (p == null) return null;
        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        return functions.stbox_as_hexwkb(p, (byte) 0, sizeOut);
    }

    // ------------------------------------------------------------------
    // Has-component flags
    // ------------------------------------------------------------------

    public static final UDF1<String, Boolean> stboxHasx =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            return p == null ? null : functions.stbox_hasx(p);
        };

    public static final UDF1<String, Boolean> stboxHast =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            return p == null ? null : functions.stbox_hast(p);
        };

    public static final UDF1<String, Boolean> stboxHasz =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            return p == null ? null : functions.stbox_hasz(p);
        };

    // ------------------------------------------------------------------
    // Spatial bound accessors (Pointer → double at offset 0)
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> stboxXmin =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_xmin(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxXmax =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_xmax(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxYmin =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_ymin(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxYmax =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_ymax(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxZmin =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_zmin(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxZmax =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_zmax(p);
            return r == null ? null : r.getDouble(0);
        };

    // ------------------------------------------------------------------
    // Temporal bound accessors (Pointer → int64 PG-epoch μs at offset 0)
    // ------------------------------------------------------------------

    public static final UDF1<String, java.sql.Timestamp> stboxTmin =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_tmin(p);
            if (r == null) return null;
            return new java.sql.Timestamp(r.getLong(0) / 1000L + PG_UNIX_OFFSET_MS);
        };

    public static final UDF1<String, java.sql.Timestamp> stboxTmax =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_tmax(p);
            if (r == null) return null;
            return new java.sql.Timestamp(r.getLong(0) / 1000L + PG_UNIX_OFFSET_MS);
        };

    // ------------------------------------------------------------------
    // Temporal inclusivity flags (Pointer → byte at offset 0)
    // ------------------------------------------------------------------

    public static final UDF1<String, Boolean> stboxTminInc =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_tmin_inc(p);
            return r == null ? null : r.getByte(0) != 0;
        };

    public static final UDF1<String, Boolean> stboxTmaxInc =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_tmax_inc(p);
            return r == null ? null : r.getByte(0) != 0;
        };

    // ------------------------------------------------------------------
    // SRID
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> stboxSrid =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            return p == null ? null : functions.stbox_srid(p);
        };

    // ------------------------------------------------------------------
    // Expansion operations
    // ------------------------------------------------------------------

    // stboxExpandSpace(stboxHex STRING, d DOUBLE) → STRING
    public static final UDF2<String, Double, String> stboxExpandSpace =
        (hex, d) -> {
            if (hex == null || d == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = functions.stbox_expand_space(p, d);
            return stboxHex(r);
        };

    // stboxExpandTime(stboxHex STRING, intervalStr STRING) → STRING
    public static final UDF2<String, String, String> stboxExpandTime =
        (hex, intervalStr) -> {
            if (hex == null || intervalStr == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) return null;
            Pointer r = functions.stbox_expand_time(p, iv);
            return stboxHex(r);
        };

    // ------------------------------------------------------------------
    // Spatial analytics  (hex-WKB in, scalar out)
    //
    // MEOS: stbox_area(box, spheroid)  meos_geo.h
    //       stbox_perimeter(box, spheroid)  meos_geo.h
    //       stbox_volume(box)  meos_geo.h
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> stboxArea =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            return functions.stbox_area(p, false);
        };

    public static final UDF1<String, Double> stboxPerimeter =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            return functions.stbox_perimeter(p, false);
        };

    public static final UDF1<String, Double> stboxVolume =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            return functions.stbox_volume(p);
        };

    // stboxIsGeodetic(hex) → Boolean
    public static final UDF1<String, Boolean> stboxIsGeodetic =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            return functions.stbox_isgeodetic(p);
        };

    // stboxToGeo(hex) → WKT of the bounding envelope polygon
    public static final UDF1<String, String> stboxToGeo =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer g = functions.stbox_to_geo(p);
            if (g == null) return null;
            return functions.geo_as_text(g, 15);
        };

    // stboxToTstzspan(hex) → tstzspan hex-WKB
    public static final UDF1<String, String> stboxToTstzspan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer span = functions.stbox_to_tstzspan(p);
            if (span == null) return null;
            return functions.span_as_hexwkb(span, (byte) 0);
        };

    public static void registerAll(SparkSession spark) {
        spark.udf().register("stboxHasx",        stboxHasx,        DataTypes.BooleanType);
        spark.udf().register("stboxHast",        stboxHast,        DataTypes.BooleanType);
        spark.udf().register("stboxHasz",        stboxHasz,        DataTypes.BooleanType);
        spark.udf().register("stboxXmin",        stboxXmin,        DataTypes.DoubleType);
        spark.udf().register("stboxXmax",        stboxXmax,        DataTypes.DoubleType);
        spark.udf().register("stboxYmin",        stboxYmin,        DataTypes.DoubleType);
        spark.udf().register("stboxYmax",        stboxYmax,        DataTypes.DoubleType);
        spark.udf().register("stboxZmin",        stboxZmin,        DataTypes.DoubleType);
        spark.udf().register("stboxZmax",        stboxZmax,        DataTypes.DoubleType);
        spark.udf().register("stboxTmin",        stboxTmin,        DataTypes.TimestampType);
        spark.udf().register("stboxTmax",        stboxTmax,        DataTypes.TimestampType);
        spark.udf().register("stboxTminInc",     stboxTminInc,     DataTypes.BooleanType);
        spark.udf().register("stboxTmaxInc",     stboxTmaxInc,     DataTypes.BooleanType);
        spark.udf().register("stboxSrid",        stboxSrid,        DataTypes.IntegerType);
        spark.udf().register("stboxExpandSpace", stboxExpandSpace, DataTypes.StringType);
        spark.udf().register("stboxExpandTime",  stboxExpandTime,  DataTypes.StringType);
        spark.udf().register("stboxArea",        stboxArea,        DataTypes.DoubleType);
        spark.udf().register("stboxPerimeter",   stboxPerimeter,   DataTypes.DoubleType);
        spark.udf().register("stboxVolume",      stboxVolume,      DataTypes.DoubleType);
        spark.udf().register("stboxIsGeodetic",  stboxIsGeodetic,  DataTypes.BooleanType);
        spark.udf().register("stboxToGeo",       stboxToGeo,       DataTypes.StringType);
        spark.udf().register("stboxToTstzspan",  stboxToTstzspan,  DataTypes.StringType);
    }
}
