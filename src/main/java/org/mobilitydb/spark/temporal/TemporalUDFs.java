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

import java.sql.Timestamp;
import java.util.HexFormat;

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
    // atTime(trip STRING, timeArg STRING) → STRING
    //
    // Portable: timeArg is either a tstzspan literal "[t1,t2]"/"(t1,t2]"/...
    // (for Q7-style period restriction) or a timestamptz string "YYYY-MM-DD HH:MM:SS"
    // (Spark implicitly casts a TIMESTAMP column to STRING for Q3-style instant
    // restriction).  Dispatch is based on the first character of timeArg.
    //
    // MEOS: tstzspan_in + temporal_at_tstzspan  (span case)
    //       pg_timestamptz_in + temporal_at_timestamptz  (instant case)
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> atTime =
        (trip, timeArg) -> {
            if (trip == null || timeArg == null) return null;
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            String s = timeArg.trim();
            Pointer result;
            if (!s.isEmpty() && (s.charAt(0) == '[' || s.charAt(0) == '(')) {
                Pointer spanPtr = functions.tstzspan_in(s);
                if (spanPtr == null) return null;
                result = functions.temporal_at_tstzspan(tptr, spanPtr);
            } else {
                java.time.OffsetDateTime odt = functions.pg_timestamptz_in(s, -1);
                if (odt == null) return null;
                result = functions.temporal_at_timestamptz(tptr, odt);
            }
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // ------------------------------------------------------------------
    // startTimestamp(trip STRING) → TIMESTAMP
    //
    // MEOS: temporal_start_timestamptz(const Temporal *) → TimestampTz
    // ------------------------------------------------------------------
    public static final UDF1<String, Timestamp> startTimestamp =
        (trip) -> {
            if (trip == null) return null;
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return fromJmeosTimestamp(functions.temporal_start_timestamptz(ptr));
        };

    // ------------------------------------------------------------------
    // endTimestamp(trip STRING) → TIMESTAMP
    //
    // MEOS: temporal_end_timestamptz(const Temporal *) → TimestampTz
    // ------------------------------------------------------------------
    public static final UDF1<String, Timestamp> endTimestamp =
        (trip) -> {
            if (trip == null) return null;
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return fromJmeosTimestamp(functions.temporal_end_timestamptz(ptr));
        };

    // ------------------------------------------------------------------
    // numInstants(trip STRING) → INT
    //
    // MEOS: temporal_num_instants(const Temporal *) → int
    // ------------------------------------------------------------------
    public static final UDF1<String, Integer> numInstants =
        (trip) -> {
            if (trip == null) return null;
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.temporal_num_instants(ptr);
        };

    // ------------------------------------------------------------------
    // speed(trip STRING) → STRING (hex-WKB of tfloat)
    //
    // Returns the instantaneous speed of a tgeompoint as a tfloat hex-WKB
    // string.  The result can be passed to asHexWKB() for binary serialization.
    //
    // MEOS: tpoint_speed(const Temporal *) → Temporal *  (tfloat)
    // ------------------------------------------------------------------
    public static final UDF1<String, String> speed =
        (trip) -> {
            if (trip == null) return null;
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            Pointer result = functions.tpoint_speed(ptr);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // ------------------------------------------------------------------
    // atGeometry(trip STRING, geomWKT STRING) → STRING
    //
    // Restricts a tgeompoint to the instants when it was inside the given
    // geometry (WKT string with SRID 0).
    //
    // MEOS: geo_from_text(const char *, int32_t) → GSERIALIZED *
    //       tgeo_at_geom(const Temporal *, const GSERIALIZED *) → Temporal *
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> atGeometry =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer gptr = functions.geo_from_text(geomWkt, 0);
            if (tptr == null || gptr == null) return null;
            Pointer result = functions.tgeo_at_geom(tptr, gptr);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // ------------------------------------------------------------------
    // asHexWKB(trip STRING) → STRING
    //
    // Serializes a temporal value to the canonical MEOS hex-WKB string
    // (little-endian, variant 0, no SRID flag) — the RFC #861 portable
    // binary return format, byte-for-byte identical across MobilityDB,
    // MobilityDuck, and MobilitySpark.
    //
    // MEOS: temporal_as_hexwkb(const Temporal *, uint8_t variant) → char *
    // ------------------------------------------------------------------
    public static final UDF1<String, String> asHexWKB =
        (trip) -> {
            if (trip == null) return null;
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.temporal_as_hexwkb(ptr, (byte) 0);
        };

    // ------------------------------------------------------------------
    // TemporalParquet readers: xFromBinary(BINARY) → STRING
    //
    // Each converts a Parquet BYTE_ARRAY column (written by MobilityDuck's
    // asBinary()) to the internal hex-WKB string used throughout MobilitySpark.
    //
    // Implementation is type-agnostic: temporal_from_hexwkb handles all
    // MEOS temporal types uniformly via the WKB type-code embedded in the
    // byte stream.  The type-specific names exist for SQL discoverability
    // and to match MobilityDuck's tgeompointFromBinary / tintFromBinary /
    // tfloatFromBinary / tboolFromBinary / ttextFromBinary surface.
    //
    // MEOS: temporal_from_hexwkb(const char *) → Temporal *
    //       temporal_as_hexwkb(const Temporal *, uint8_t variant) → char *
    // ------------------------------------------------------------------
    private static String fromBinaryImpl(byte[] bytes) throws Exception {
        if (bytes == null) return null;
        String hex = HexFormat.of().formatHex(bytes).toUpperCase();
        Pointer ptr = functions.temporal_from_hexwkb(hex);
        if (ptr == null) return null;
        return functions.temporal_as_hexwkb(ptr, (byte) 0);
    }

    public static final UDF1<byte[], String> tgeompointFromBinary =
        (bytes) -> fromBinaryImpl(bytes);

    public static final UDF1<byte[], String> tgeogpointFromBinary =
        (bytes) -> fromBinaryImpl(bytes);

    public static final UDF1<byte[], String> tintFromBinary =
        (bytes) -> fromBinaryImpl(bytes);

    public static final UDF1<byte[], String> tfloatFromBinary =
        (bytes) -> fromBinaryImpl(bytes);

    public static final UDF1<byte[], String> tboolFromBinary =
        (bytes) -> fromBinaryImpl(bytes);

    public static final UDF1<byte[], String> ttextFromBinary =
        (bytes) -> fromBinaryImpl(bytes);

    // ------------------------------------------------------------------
    // asBinary(trip STRING) → BINARY
    //
    // Converts an internal hex-WKB string back to raw bytes for writing to
    // a Parquet BYTE_ARRAY column — the inverse of xFromBinary().  No MEOS
    // call is needed: the internal format is already hex-encoded MEOS-WKB,
    // so hex-decoding is sufficient.
    //
    // Use this to write any temporal type (tgeompoint, tint, tfloat, …)
    // back to Parquet after processing in Spark SQL.
    // ------------------------------------------------------------------
    public static final UDF1<String, byte[]> asBinary =
        (hexWkb) -> {
            if (hexWkb == null) return null;
            return HexFormat.of().parseHex(hexWkb.toLowerCase());
        };

    public static void registerAll(org.apache.spark.sql.SparkSession spark) {
        spark.udf().register("atTime",           atTime,           DataTypes.StringType);
        spark.udf().register("startTimestamp",    startTimestamp,   DataTypes.TimestampType);
        spark.udf().register("endTimestamp",      endTimestamp,     DataTypes.TimestampType);
        spark.udf().register("numInstants",       numInstants,      DataTypes.IntegerType);
        spark.udf().register("speed",             speed,            DataTypes.StringType);
        spark.udf().register("atGeometry",        atGeometry,       DataTypes.StringType);
        spark.udf().register("asHexWKB",          asHexWKB,         DataTypes.StringType);
        spark.udf().register("tgeompointFromBinary", tgeompointFromBinary, DataTypes.StringType);
        spark.udf().register("tgeogpointFromBinary", tgeogpointFromBinary, DataTypes.StringType);
        spark.udf().register("tintFromBinary",    tintFromBinary,   DataTypes.StringType);
        spark.udf().register("tfloatFromBinary",  tfloatFromBinary, DataTypes.StringType);
        spark.udf().register("tboolFromBinary",   tboolFromBinary,  DataTypes.StringType);
        spark.udf().register("ttextFromBinary",   ttextFromBinary,  DataTypes.StringType);
        spark.udf().register("asBinary",          asBinary,         DataTypes.BinaryType);
    }
}
