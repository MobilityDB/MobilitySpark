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
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

import java.sql.Timestamp;

/**
 * Spark SQL UDFs for generic temporal operations (type-agnostic).
 *
 * Storage convention: temporal values are hex-WKB strings produced by
 * temporal_as_hexwkb(ptr, (byte) 0) and parsed back with
 * temporal_from_hexwkb(hex).
 *
 * Epoch note: MEOS uses PostgreSQL epoch (µs since 2000-01-01); Spark uses
 * UNIX epoch (ms since 1970-01-01). Conversion is done via pg_timestamptz_in()
 * which stores the raw PG-epoch value in the OffsetDateTime's seconds field.
 * Never call toEpochSecond() on a java.sql.Timestamp and pass it directly.
 *
 * MEOS function authority: meos/include/meos.h
 * JMEOS PR: github.com/MobilityDB/JMEOS/pull/9
 */
public final class TemporalUDFs {

    private TemporalUDFs() {}

    // PG epoch is 2000-01-01; Unix epoch is 1970-01-01. Difference = 946684800 s.
    // JMEOS stores PG-epoch µs in the OffsetDateTime's epoch-seconds field.
    private static final long PG_UNIX_EPOCH_OFFSET_MS = 946684800L * 1000L;

    /** Convert a JMEOS OffsetDateTime (PG-epoch µs in epoch-seconds field) to Spark Timestamp. */
    static Timestamp fromJmeosTimestamp(java.time.OffsetDateTime odt) {
        // odt.toEpochSecond() holds the raw PG-epoch µs (not real seconds).
        long unixEpochMillis = odt.toEpochSecond() / 1000L + PG_UNIX_EPOCH_OFFSET_MS;
        return new Timestamp(unixEpochMillis);
    }

    // ------------------------------------------------------------------
    // atTime(trip STRING, timeArg STRING|TIMESTAMP) → STRING
    //
    // timeArg may be:
    //   - java.sql.Timestamp  (Q3: QueryInstants.instant column, Spark TIMESTAMP type)
    //   - String span literal "[t1,t2]"/"(t1,t2]"/...  (Q7: QueryPeriods.period)
    //   - String instant literal "YYYY-MM-DD HH:MM:SS+TZ"  (plain string instant)
    //
    // MEOS: tstzspan_in + temporal_at_tstzspan  (span case)
    //       temporal_at_timestamptz              (instant case)
    // ------------------------------------------------------------------
    public static final UDF2<String, Object, String> atTime =
        (trip, timeArg) -> {
            if (trip == null || timeArg == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer result;
                if (timeArg instanceof java.sql.Timestamp) {
                    long pgEpochMicros = (((java.sql.Timestamp) timeArg).getTime() - 946684800L * 1000L) * 1000L;
                    java.time.OffsetDateTime odt = java.time.OffsetDateTime.ofInstant(
                        java.time.Instant.ofEpochSecond(pgEpochMicros, 0),
                        java.time.ZoneOffset.UTC);
                    result = functions.temporal_at_timestamptz(tptr, odt);
                } else {
                    String s = timeArg.toString().trim();
                    if (!s.isEmpty() && (s.charAt(0) == '[' || s.charAt(0) == '(')) {
                        Pointer spanPtr = functions.tstzspan_in(s);
                        if (spanPtr == null) return null;
                        try {
                            result = functions.temporal_at_tstzspan(tptr, spanPtr);
                        } finally {
                            MeosMemory.free(spanPtr);
                        }
                    } else {
                        java.time.OffsetDateTime odt = functions.pg_timestamptz_in(s, -1);
                        if (odt == null) return null;
                        result = functions.temporal_at_timestamptz(tptr, odt);
                    }
                }
                if (result == null) return null;
                try {
                    return functions.temporal_as_hexwkb(result, (byte) 0);
                } finally {
                    MeosMemory.free(result);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // startTimestamp(trip STRING) → TIMESTAMP
    //
    // MEOS: temporal_start_timestamptz(const Temporal *) → TimestampTz
    // ------------------------------------------------------------------
    public static final UDF1<String, Timestamp> startTimestamp =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return fromJmeosTimestamp(functions.temporal_start_timestamptz(ptr));
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // endTimestamp(trip STRING) → TIMESTAMP
    //
    // MEOS: temporal_end_timestamptz(const Temporal *) → TimestampTz
    // ------------------------------------------------------------------
    public static final UDF1<String, Timestamp> endTimestamp =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return fromJmeosTimestamp(functions.temporal_end_timestamptz(ptr));
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // numInstants(trip STRING) → INT
    //
    // MEOS: temporal_num_instants(const Temporal *) → int
    // ------------------------------------------------------------------
    public static final UDF1<String, Integer> numInstants =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return functions.temporal_num_instants(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // speed(trip STRING) → STRING (hex-WKB of tfloat)
    //
    // MEOS: tpoint_speed(const Temporal *) → Temporal *  (tfloat)
    // ------------------------------------------------------------------
    public static final UDF1<String, String> speed =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer result = functions.tpoint_speed(ptr);
                if (result == null) return null;
                try {
                    return functions.temporal_as_hexwkb(result, (byte) 0);
                } finally {
                    MeosMemory.free(result);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // atGeometry(trip STRING, geomWKT STRING) → STRING
    //
    // Restricts a tgeompoint to the instants when it was inside geomWkt.
    //
    // MEOS: geo_from_text(const char *, int32_t) → GSERIALIZED *
    //       tgeo_at_geom(const Temporal *, const GSERIALIZED *) → Temporal *
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> atGeometry =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer gptr = functions.geo_from_text(geomWkt, 0);
                if (gptr == null) return null;
                try {
                    Pointer result = functions.tgeo_at_geom(tptr, gptr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // asHexWKB(trip STRING) → STRING
    //
    // Serializes a temporal value to the canonical MEOS hex-WKB string
    // (little-endian, variant 0) — byte-for-byte identical across all platforms.
    //
    // MEOS: temporal_as_hexwkb(const Temporal *, uint8_t variant) → char *
    // ------------------------------------------------------------------
    public static final UDF1<String, String> asHexWKB =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return functions.temporal_as_hexwkb(ptr, (byte) 0);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // MFJSON output  (hex-WKB in → JSON string out)
    //
    // MEOS: temporal_as_mfjson(temp, withbbox, flags, precision, srs)
    //       flags=0 → WKT geometry (not EWKT), precision controls decimal places
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> temporalAsMfjson =
        (trip, precision) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            int prec = (precision == null) ? 6 : precision;
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return functions.temporal_as_mfjson(ptr, false, 0, prec, null);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Text output  (hex-WKB in → WKT-like text string out)
    //
    // MEOS: tbool_out, tint_out, tfloat_out (with precision), ttext_out
    // These mirror the PostgreSQL temporal type output functions.
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tboolOut =
        (tbool) -> {
            if (tbool == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(tbool);
            if (ptr == null) return null;
            try {
                return functions.tbool_out(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    public static final UDF1<String, String> tintOut =
        (tint) -> {
            if (tint == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(tint);
            if (ptr == null) return null;
            try {
                return functions.tint_out(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    public static final UDF2<String, Integer, String> tfloatOut =
        (tfloat, precision) -> {
            if (tfloat == null) return null;
            MeosThread.ensureReady();
            int prec = (precision == null) ? 6 : precision;
            Pointer ptr = functions.temporal_from_hexwkb(tfloat);
            if (ptr == null) return null;
            try {
                return functions.tfloat_out(ptr, prec);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    public static final UDF1<String, String> ttextOut =
        (ttext) -> {
            if (ttext == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(ttext);
            if (ptr == null) return null;
            try {
                return functions.ttext_out(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    public static void registerAll(org.apache.spark.sql.SparkSession spark) {
        spark.udf().register("atTime",            atTime,            DataTypes.StringType);
        spark.udf().register("startTimestamp",    startTimestamp,    DataTypes.TimestampType);
        spark.udf().register("endTimestamp",      endTimestamp,      DataTypes.TimestampType);
        spark.udf().register("numInstants",       numInstants,       DataTypes.IntegerType);
        spark.udf().register("speed",             speed,             DataTypes.StringType);
        spark.udf().register("atGeometry",        atGeometry,        DataTypes.StringType);
        spark.udf().register("asHexWKB",          asHexWKB,          DataTypes.StringType);
        spark.udf().register("temporalAsMfjson",  temporalAsMfjson,  DataTypes.StringType);
        spark.udf().register("tboolOut",          tboolOut,          DataTypes.StringType);
        spark.udf().register("tintOut",           tintOut,           DataTypes.StringType);
        spark.udf().register("tfloatOut",         tfloatOut,         DataTypes.StringType);
        spark.udf().register("ttextOut",          ttextOut,          DataTypes.StringType);

        // MobilityDB SQL bare-name aliases for temporal-instant accessors
        // (work for any single-instant temporal value)
        spark.udf().register("getTimestamp",      startTimestamp,    DataTypes.TimestampType);
    }
}
