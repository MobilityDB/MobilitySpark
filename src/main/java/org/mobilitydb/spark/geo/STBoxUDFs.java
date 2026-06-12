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

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.api.java.UDF7;
import org.apache.spark.sql.types.DataTypes;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.mobilitydb.spark.util.TimeUtil;

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
    private static Pointer stboxPtr(String hex) {
        if (hex == null) return null;
        return GeneratedFunctions.stbox_from_hexwkb(hex);
    }

    // stbox_as_hexwkb requires a non-null size_out scratch Pointer
    private static String stboxHex(Pointer p) {
        if (p == null) return null;
        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        return GeneratedFunctions.stbox_as_hexwkb(p, (byte) 0, sizeOut);
    }

    // ------------------------------------------------------------------
    // Has-component flags
    // ------------------------------------------------------------------

    public static final UDF1<String, Boolean> stboxHasx =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            return p == null ? null : GeneratedFunctions.stbox_hasx(p);
        };

    public static final UDF1<String, Boolean> stboxHast =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            return p == null ? null : GeneratedFunctions.stbox_hast(p);
        };

    public static final UDF1<String, Boolean> stboxHasz =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            return p == null ? null : GeneratedFunctions.stbox_hasz(p);
        };

    // ------------------------------------------------------------------
    // Spatial bound accessors (Pointer → double at offset 0)
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> stboxXmin =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = GeneratedFunctions.stbox_xmin(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxXmax =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = GeneratedFunctions.stbox_xmax(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxYmin =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = GeneratedFunctions.stbox_ymin(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxYmax =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = GeneratedFunctions.stbox_ymax(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxZmin =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = GeneratedFunctions.stbox_zmin(p);
            return r == null ? null : r.getDouble(0);
        };

    public static final UDF1<String, Double> stboxZmax =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = GeneratedFunctions.stbox_zmax(p);
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
            Pointer r = GeneratedFunctions.stbox_tmin(p);
            if (r == null) return null;
            return new java.sql.Timestamp(r.getLong(0) / 1000L + TimeUtil.PG_UNIX_EPOCH_OFFSET_MS);
        };

    public static final UDF1<String, java.sql.Timestamp> stboxTmax =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = GeneratedFunctions.stbox_tmax(p);
            if (r == null) return null;
            return new java.sql.Timestamp(r.getLong(0) / 1000L + TimeUtil.PG_UNIX_EPOCH_OFFSET_MS);
        };

    // ------------------------------------------------------------------
    // Temporal inclusivity flags (Pointer → byte at offset 0)
    // ------------------------------------------------------------------

    public static final UDF1<String, Boolean> stboxTminInc =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = GeneratedFunctions.stbox_tmin_inc(p);
            return r == null ? null : r.getByte(0) != 0;
        };

    public static final UDF1<String, Boolean> stboxTmaxInc =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer r = GeneratedFunctions.stbox_tmax_inc(p);
            return r == null ? null : r.getByte(0) != 0;
        };

    // ------------------------------------------------------------------
    // SRID
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> stboxSrid =
        (hex) -> {
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            return p == null ? null : GeneratedFunctions.stbox_srid(p);
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
            Pointer r = GeneratedFunctions.stbox_expand_space(p, d);
            return stboxHex(r);
        };

    // stboxExpandTime(stboxHex STRING, intervalStr STRING) → STRING
    public static final UDF2<String, String, String> stboxExpandTime =
        (hex, intervalStr) -> {
            if (hex == null || intervalStr == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer iv = GeneratedFunctions.pg_interval_in(intervalStr, -1);
            if (iv == null) return null;
            Pointer r = GeneratedFunctions.stbox_expand_time(p, iv);
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
            return GeneratedFunctions.stbox_area(p, false);
        };

    public static final UDF1<String, Double> stboxPerimeter =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            return GeneratedFunctions.stbox_perimeter(p, false);
        };

    public static final UDF1<String, Double> stboxVolume =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            return GeneratedFunctions.stbox_volume(p);
        };

    // stboxIsGeodetic(hex) → Boolean
    public static final UDF1<String, Boolean> stboxIsGeodetic =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            return GeneratedFunctions.stbox_isgeodetic(p);
        };

    // stboxToGeo(hex) → WKT of the bounding envelope polygon
    public static final UDF1<String, String> stboxToGeo =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer g = GeneratedFunctions.stbox_to_geo(p);
            if (g == null) return null;
            return GeneratedFunctions.geo_as_text(g, 15);
        };

    // stboxToTstzspan(hex) → tstzspan hex-WKB
    public static final UDF1<String, String> stboxToTstzspan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer span = GeneratedFunctions.stbox_to_tstzspan(p);
            if (span == null) return null;
            return GeneratedFunctions.span_as_hexwkb(span, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Rounding
    // ------------------------------------------------------------------

    // stboxRound(hex STRING, maxDecimals INT) → STRING
    // MEOS: stbox_round(const STBox *, int) → STBox *
    public static final UDF2<String, Integer, String> stboxRound =
        (hex, maxDecimals) -> {
            if (hex == null || maxDecimals == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer result = GeneratedFunctions.stbox_round(p, maxDecimals);
            if (result == null) return null;
            try {
                return stboxHex(result);
            } finally {
                MeosMemory.free(result);
            }
        };

    // ------------------------------------------------------------------
    // SRID assignment
    // ------------------------------------------------------------------

    // stboxSetSrid(hex STRING, srid INT) → STRING
    // MEOS: stbox_set_srid(const STBox *, int) → STBox *
    public static final UDF2<String, Integer, String> stboxSetSrid =
        (hex, srid) -> {
            if (hex == null || srid == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer result = GeneratedFunctions.stbox_set_srid(p, srid);
            if (result == null) return null;
            try {
                return stboxHex(result);
            } finally {
                MeosMemory.free(result);
            }
        };

    // ------------------------------------------------------------------
    // Time-domain shifting and scaling
    // ------------------------------------------------------------------

    // stboxShiftScaleTime(hex STRING, shift STRING, scale STRING) → STRING
    // MEOS: stbox_shift_scale_time(const STBox *, Interval *, Interval *) → STBox *
    //       Either shift or scale may be null (pass null to MEOS for no-op).
    public static final UDF3<String, String, String, String> stboxShiftScaleTime =
        (hex, shift, scale) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer shiftIv = shift == null ? null : GeneratedFunctions.pg_interval_in(shift, Integer.MIN_VALUE);
            Pointer scaleIv = scale == null ? null : GeneratedFunctions.pg_interval_in(scale, Integer.MIN_VALUE);
            try {
                Pointer result = GeneratedFunctions.stbox_shift_scale_time(p, shiftIv, scaleIv);
                if (result == null) return null;
                try {
                    return stboxHex(result);
                } finally {
                    MeosMemory.free(result);
                }
            } finally {
                if (shiftIv != null) MeosMemory.free(shiftIv);
                if (scaleIv != null) MeosMemory.free(scaleIv);
            }
        };

    // ------------------------------------------------------------------
    // STBox constructors from geometry / span / timestamptz
    //
    // MEOS: geo_to_stbox, tstzspan_to_stbox, timestamptz_to_stbox
    // ------------------------------------------------------------------

    // geoToStbox(wkt STRING) → STBox hex-WKB
    // Creates an STBox with the bounding box of a geometry.
    // MEOS: geo_to_stbox(const GSERIALIZED *) → STBox *
    public static final UDF1<String, String> geoToStbox =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer geo = GeneratedFunctions.geo_from_text(wkt, 0);
            if (geo == null) return null;
            Pointer result = GeneratedFunctions.geo_to_stbox(geo);
            if (result == null) return null;
            try {
                return stboxHex(result);
            } finally {
                MeosMemory.free(result);
            }
        };

    // tstzspanToStbox(spanHex STRING) → STBox hex-WKB
    // Creates a time-only STBox from a tstzspan.
    // MEOS: tstzspan_to_stbox(const Span *) → STBox *
    public static final UDF1<String, String> tstzspanToStbox =
        (spanHex) -> {
            if (spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer span = GeneratedFunctions.span_from_hexwkb(spanHex);
            if (span == null) return null;
            Pointer result = GeneratedFunctions.tstzspan_to_stbox(span);
            if (result == null) return null;
            try {
                return stboxHex(result);
            } finally {
                MeosMemory.free(result);
            }
        };

    // timestamptzToStbox(ts TIMESTAMP) → STBox hex-WKB
    // Creates a point-time STBox from a single timestamp.
    // MEOS: timestamptz_to_stbox(TimestampTz) → STBox *
    public static final UDF1<java.sql.Timestamp, String> timestamptzToStbox =
        (ts) -> {
            if (ts == null) return null;
            MeosThread.ensureReady();
            long pgMicros = (ts.getTime() - TimeUtil.PG_UNIX_EPOCH_OFFSET_MS) * 1000L;
            OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(pgMicros, 0), ZoneOffset.UTC);
            Pointer result = GeneratedFunctions.timestamptz_to_stbox(odt);
            if (result == null) return null;
            try {
                return stboxHex(result);
            } finally {
                MeosMemory.free(result);
            }
        };

    // ------------------------------------------------------------------
    // Spatial component extraction
    //
    // MEOS: stbox_get_space(const STBox *) → STBox * (spatial dims only, no T)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> stboxGetSpace =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = stboxPtr(hex);
            if (p == null) return null;
            Pointer result = GeneratedFunctions.stbox_get_space(p);
            if (result == null) return null;
            try {
                return stboxHex(result);
            } finally {
                MeosMemory.free(result);
            }
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
        spark.udf().register("stboxToGeo",            stboxToGeo,            DataTypes.StringType);
        spark.udf().register("stboxToTstzspan",       stboxToTstzspan,       DataTypes.StringType);
        spark.udf().register("stboxRound",            stboxRound,            DataTypes.StringType);
        spark.udf().register("stboxSetSrid",          stboxSetSrid,          DataTypes.StringType);
        spark.udf().register("stboxShiftScaleTime",   stboxShiftScaleTime,   DataTypes.StringType);
        spark.udf().register("stboxGetSpace",          stboxGetSpace,         DataTypes.StringType);
        // STBox constructors from geometry / span / timestamp
        spark.udf().register("geoToStbox",             geoToStbox,            DataTypes.StringType);
        spark.udf().register("tstzspanToStbox",        tstzspanToStbox,       DataTypes.StringType);
        spark.udf().register("timestamptzToStbox",     timestamptzToStbox,    DataTypes.StringType);
        // STBox set operations
        spark.udf().register("intersectionStboxStbox", intersectionStboxStbox, DataTypes.StringType);
        spark.udf().register("unionStboxStbox",        unionStboxStbox,        DataTypes.StringType);
        // MobilityDB SQL bare-name aliases for the same lambdas
        spark.udf().register("stboxIntersection",      intersectionStboxStbox, DataTypes.StringType);
        spark.udf().register("stboxUnion",             unionStboxStbox,        DataTypes.StringType);
        // Typed STBox constructors
        spark.udf().register("stboxX",      stboxX,      DataTypes.StringType);
        spark.udf().register("stboxT",      stboxT,      DataTypes.StringType);
        spark.udf().register("stboxXT",     stboxXT,     DataTypes.StringType);
        spark.udf().register("stboxZ",      stboxZ,      DataTypes.StringType);
        spark.udf().register("stboxZT",     stboxZT,     DataTypes.StringType);
        spark.udf().register("geodstboxZ",  geodstboxZ,  DataTypes.StringType);
        spark.udf().register("geodstboxT",  geodstboxT,  DataTypes.StringType);
        spark.udf().register("geodstboxZT", geodstboxZT, DataTypes.StringType);
        // STBox topology predicates (stbox, stbox)
        spark.udf().register("stboxContains",    stboxContains,    DataTypes.BooleanType);
        spark.udf().register("stboxContained",   stboxContained,   DataTypes.BooleanType);
        spark.udf().register("stboxOverlaps",    stboxOverlaps,    DataTypes.BooleanType);
        // STBox positional predicates (stbox, stbox)
        spark.udf().register("stboxLeft",        stboxLeft,        DataTypes.BooleanType);
        spark.udf().register("stboxOverleft",    stboxOverleft,    DataTypes.BooleanType);
        spark.udf().register("stboxRight",       stboxRight,       DataTypes.BooleanType);
        spark.udf().register("stboxOverright",   stboxOverright,   DataTypes.BooleanType);
        spark.udf().register("stboxBelow",       stboxBelow,       DataTypes.BooleanType);
        spark.udf().register("stboxOverbelow",   stboxOverbelow,   DataTypes.BooleanType);
        spark.udf().register("stboxAbove",       stboxAbove,       DataTypes.BooleanType);
        spark.udf().register("stboxOverabove",   stboxOverabove,   DataTypes.BooleanType);
        spark.udf().register("stboxBefore",      stboxBefore,      DataTypes.BooleanType);
        spark.udf().register("stboxOverbefore",  stboxOverbefore,  DataTypes.BooleanType);
        spark.udf().register("stboxAfter",       stboxAfter,       DataTypes.BooleanType);
        spark.udf().register("stboxOverafter",   stboxOverafter,   DataTypes.BooleanType);
        spark.udf().register("stboxAdjacent",    stboxAdjacent,    DataTypes.BooleanType);
    }

    // ------------------------------------------------------------------
    // STBox set operations
    // MEOS: intersection_stbox_stbox(STBox *, STBox *) → STBox * (NULL if empty)
    //       union_stbox_stbox(STBox *, STBox *, bool strict) → STBox *
    // ------------------------------------------------------------------

    private static String stboxBinOp(String h1, String h2,
            java.util.function.BiFunction<Pointer, Pointer, Pointer> fn) {
        if (h1 == null || h2 == null) return null;
        MeosThread.ensureReady();
        Pointer p1 = stboxPtr(h1), p2 = stboxPtr(h2);
        if (p1 == null || p2 == null) return null;
        try {
            Pointer r = fn.apply(p1, p2);
            if (r == null) return null;
            try {
                return stboxHex(r);
            } finally { MeosMemory.free(r); }
        } finally {
            MeosMemory.free(p1);
            MeosMemory.free(p2);
        }
    }

    public static final UDF2<String, String, String> intersectionStboxStbox =
        (h1, h2) -> stboxBinOp(h1, h2, GeneratedFunctions::intersection_stbox_stbox);

    public static final UDF2<String, String, String> unionStboxStbox =
        (h1, h2) -> stboxBinOp(h1, h2, (p1, p2) -> GeneratedFunctions.union_stbox_stbox(p1, p2, false));

    // ------------------------------------------------------------------
    // STBox positional predicates  (stbox, stbox) → Boolean
    // MEOS: left/overleft/right/overright/below/overbelow/above/overabove/
    //       before/overbefore/after/overafter/adjacent_stbox_stbox → bool
    // ------------------------------------------------------------------

    private static Boolean stboxBoolOp(String h1, String h2,
            java.util.function.BiFunction<Pointer, Pointer, Boolean> fn) {
        if (h1 == null || h2 == null) return null;
        MeosThread.ensureReady();
        Pointer p1 = stboxPtr(h1), p2 = stboxPtr(h2);
        if (p1 == null || p2 == null) return null;
        return fn.apply(p1, p2);
    }

    // ------------------------------------------------------------------
    // STBox topology predicates  (stbox, stbox) → Boolean
    // MEOS: contains/contained/overlaps_stbox_stbox → bool
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> stboxContains =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::contains_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxContained =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::contained_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxOverlaps =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::overlaps_stbox_stbox);

    public static final UDF2<String, String, Boolean> stboxLeft =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::left_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxOverleft =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::overleft_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxRight =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::right_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxOverright =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::overright_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxBelow =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::below_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxOverbelow =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::overbelow_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxAbove =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::above_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxOverabove =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::overabove_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxBefore =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::before_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxOverbefore =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::overbefore_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxAfter =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::after_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxOverafter =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::overafter_stbox_stbox);
    public static final UDF2<String, String, Boolean> stboxAdjacent =
        (h1, h2) -> stboxBoolOp(h1, h2, GeneratedFunctions::adjacent_stbox_stbox);

    // ------------------------------------------------------------------
    // Typed STBox constructors — delegate to stbox_make with the correct
    // hasx/hasz/geodetic/srid flags. tstzspan input is hex-WKB.
    // MEOS: stbox_make(hasx, hasz, geodetic, srid, xmin, ymin, zmin, xmax,
    //                  ymax, zmax, periodPtr) → STBox*
    // ------------------------------------------------------------------

    private static String stboxMakeHelper(boolean hasx, boolean hasz, boolean geodetic, int srid,
            double xmin, double ymin, double zmin, double xmax, double ymax, double zmax,
            String tstzspanHex) {
        MeosThread.ensureReady();
        Pointer period = (tstzspanHex == null) ? null : GeneratedFunctions.span_from_hexwkb(tstzspanHex);
        try {
            Pointer p = GeneratedFunctions.stbox_make(hasx, hasz, geodetic, srid,
                xmin, ymin, zmin, xmax, ymax, zmax, period);
            if (p == null) return null;
            try {
                Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                return GeneratedFunctions.stbox_as_hexwkb(p, (byte) 0, sizeOut);
            } finally { MeosMemory.free(p); }
        } finally { if (period != null) MeosMemory.free(period); }
    }

    // stboxX(xmin, ymin, xmax, ymax) — 2D spatial box, no time, no geodetic
    public static final UDF4<Double, Double, Double, Double, String> stboxX =
        (xmin, ymin, xmax, ymax) -> {
            if (xmin == null || ymin == null || xmax == null || ymax == null) return null;
            return stboxMakeHelper(true, false, false, 0, xmin, ymin, 0, xmax, ymax, 0, null);
        };

    // stboxT(tstzspanHex) — time-only box
    public static final UDF1<String, String> stboxT =
        tstzspanHex -> {
            if (tstzspanHex == null) return null;
            return stboxMakeHelper(false, false, false, 0, 0, 0, 0, 0, 0, 0, tstzspanHex);
        };

    // stboxXT(xmin, ymin, xmax, ymax, tstzspanHex)
    public static final UDF5<Double, Double, Double, Double, String, String> stboxXT =
        (xmin, ymin, xmax, ymax, tstzspanHex) -> {
            if (xmin == null || ymin == null || xmax == null || ymax == null || tstzspanHex == null) return null;
            return stboxMakeHelper(true, false, false, 0, xmin, ymin, 0, xmax, ymax, 0, tstzspanHex);
        };

    // stboxZ(xmin, ymin, zmin, xmax, ymax, zmax) — 3D spatial box
    public static final UDF6<Double, Double, Double, Double, Double, Double, String> stboxZ =
        (xmin, ymin, zmin, xmax, ymax, zmax) -> {
            if (xmin == null || ymin == null || zmin == null || xmax == null || ymax == null || zmax == null) return null;
            return stboxMakeHelper(true, true, false, 0, xmin, ymin, zmin, xmax, ymax, zmax, null);
        };

    // stboxZT(xmin, ymin, zmin, xmax, ymax, zmax, tstzspanHex) — 3D + time
    public static final UDF7<Double, Double, Double, Double, Double, Double, String, String> stboxZT =
        (xmin, ymin, zmin, xmax, ymax, zmax, tstzspanHex) -> {
            if (xmin == null || ymin == null || zmin == null || xmax == null || ymax == null || zmax == null || tstzspanHex == null) return null;
            return stboxMakeHelper(true, true, false, 0, xmin, ymin, zmin, xmax, ymax, zmax, tstzspanHex);
        };

    // Geodetic variants — geodetic=true, default SRID 4326
    public static final UDF6<Double, Double, Double, Double, Double, Double, String> geodstboxZ =
        (xmin, ymin, zmin, xmax, ymax, zmax) -> {
            if (xmin == null || ymin == null || zmin == null || xmax == null || ymax == null || zmax == null) return null;
            return stboxMakeHelper(true, true, true, 4326, xmin, ymin, zmin, xmax, ymax, zmax, null);
        };

    public static final UDF1<String, String> geodstboxT =
        tstzspanHex -> {
            if (tstzspanHex == null) return null;
            return stboxMakeHelper(false, false, true, 4326, 0, 0, 0, 0, 0, 0, tstzspanHex);
        };

    public static final UDF7<Double, Double, Double, Double, Double, Double, String, String> geodstboxZT =
        (xmin, ymin, zmin, xmax, ymax, zmax, tstzspanHex) -> {
            if (xmin == null || ymin == null || zmin == null || xmax == null || ymax == null || zmax == null || tstzspanHex == null) return null;
            return stboxMakeHelper(true, true, true, 4326, xmin, ymin, zmin, xmax, ymax, zmax, tstzspanHex);
        };
}
