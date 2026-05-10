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
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Spark SQL UDFs for temporal structure and value accessors not covered by
 * AccessorUDFs.java or TemporalUDFs.java.
 *
 * Covers: subtype, instant/sequence navigation, timestampN, inclusivity flags,
 * duration, type-specific valueN accessors (tbool, tfloat, ttext, tpoint),
 * and tpoint geometry accessors (SRID, convex hull).
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
public final class MoreAccessorUDFs {

    private MoreAccessorUDFs() {}

    // Milliseconds from Unix epoch (1970-01-01) to PG epoch (2000-01-01).
    private static final long PG_UNIX_EPOCH_OFFSET_MS = 946684800L * 1000L;

    /** Convert a raw PG-epoch microsecond value to a Spark Timestamp. */
    static Timestamp fromPgMicros(long pgMicros) {
        return new Timestamp(pgMicros / 1000L + PG_UNIX_EPOCH_OFFSET_MS);
    }

    // ------------------------------------------------------------------
    // Subtype
    // ------------------------------------------------------------------

    // temporalSubtype(trip STRING) → STRING  ("Instant" | "Sequence" | "SequenceSet")
    // MEOS: temporal_subtype(const Temporal *) → char *
    public static final UDF1<String, String> temporalSubtype =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return functions.temporal_subtype(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Instant navigation
    // ------------------------------------------------------------------

    // startInstant(trip STRING) → STRING  (hex-WKB of first instant)
    // MEOS: temporal_start_instant(const Temporal *) → TInstant *  (owned copy)
    public static final UDF1<String, String> startInstant =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_start_instant(ptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // endInstant(trip STRING) → STRING  (hex-WKB of last instant)
    // MEOS: temporal_end_instant(const Temporal *) → TInstant *  (owned copy)
    public static final UDF1<String, String> endInstant =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_end_instant(ptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // instantN(trip STRING, n INT) → STRING  (hex-WKB of n-th instant, 1-based)
    // MEOS: temporal_instant_n(const Temporal *, int) → TInstant *  (owned copy)
    public static final UDF2<String, Integer, String> instantN =
        (trip, n) -> {
            if (trip == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_instant_n(ptr, n);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Sequence navigation
    // ------------------------------------------------------------------

    // startSequence(trip STRING) → STRING  (hex-WKB of first sequence)
    // MEOS: temporal_start_sequence(const Temporal *) → TSequence *  (owned copy)
    public static final UDF1<String, String> startSequence =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_start_sequence(ptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // endSequence(trip STRING) → STRING  (hex-WKB of last sequence)
    // MEOS: temporal_end_sequence(const Temporal *) → TSequence *  (owned copy)
    public static final UDF1<String, String> endSequence =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_end_sequence(ptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // sequenceN(trip STRING, n INT) → STRING  (hex-WKB of n-th sequence, 1-based)
    // MEOS: temporal_sequence_n(const Temporal *, int) → TSequence *  (owned copy)
    public static final UDF2<String, Integer, String> sequenceN =
        (trip, n) -> {
            if (trip == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_sequence_n(ptr, n);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Min/max instant
    // ------------------------------------------------------------------

    // minInstant(trip STRING) → STRING  (hex-WKB of instant with minimum value)
    // MEOS: temporal_min_instant(const Temporal *) → TInstant *  (owned copy)
    public static final UDF1<String, String> minInstant =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_min_instant(ptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // maxInstant(trip STRING) → STRING  (hex-WKB of instant with maximum value)
    // MEOS: temporal_max_instant(const Temporal *) → TInstant *  (owned copy)
    public static final UDF1<String, String> maxInstant =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_max_instant(ptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Timestamp accessors
    // ------------------------------------------------------------------

    // numTimestamps(trip STRING) → INT
    // MEOS: temporal_num_timestamps(const Temporal *) → int
    public static final UDF1<String, Integer> numTimestamps =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return functions.temporal_num_timestamps(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // timestampN(trip STRING, n INT) → TIMESTAMP  (n-th timestamp, 1-based)
    // MEOS: temporal_timestamptz_n writes TimestampTz (int64) into a JNR-FFI buffer;
    //       the returned Pointer is JNR-FFI managed — DO NOT call MeosMemory.free on it.
    public static final UDF2<String, Integer, Timestamp> timestampN =
        (trip, n) -> {
            if (trip == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer tsPtr = functions.temporal_timestamptz_n(ptr, n);
                if (tsPtr == null) return null;
                return fromPgMicros(tsPtr.getLong(0));
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Inclusivity flags
    // ------------------------------------------------------------------

    // lowerInc(trip STRING) → BOOLEAN  (true if the lower bound is inclusive)
    // MEOS: temporal_lower_inc(const Temporal *) → int  (nonzero = true)
    public static final UDF1<String, Boolean> lowerInc =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return functions.temporal_lower_inc(ptr) != 0;
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // upperInc(trip STRING) → BOOLEAN  (true if the upper bound is inclusive)
    // MEOS: temporal_upper_inc(const Temporal *) → int  (nonzero = true)
    public static final UDF1<String, Boolean> upperInc =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return functions.temporal_upper_inc(ptr) != 0;
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Duration
    // ------------------------------------------------------------------

    // duration(trip STRING) → STRING  (PostgreSQL interval literal, e.g. "01:30:00")
    // MEOS: temporal_duration(const Temporal *, bool) → Interval *
    //       pg_interval_out(const Interval *) → char *
    public static final UDF1<String, String> duration =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer ivPtr = functions.temporal_duration(ptr, false);
                if (ivPtr == null) return null;
                try {
                    return functions.pg_interval_out(ivPtr);
                } finally {
                    MeosMemory.free(ivPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // tbool value accessor
    // ------------------------------------------------------------------

    // tboolValueN(trip STRING, n INT) → BOOLEAN  (n-th value, 1-based)
    // MEOS: tbool_value_n writes bool directly into a JNR-FFI buffer;
    //       the returned Pointer is JNR-FFI managed — DO NOT call MeosMemory.free on it.
    public static final UDF2<String, Integer, Boolean> tboolValueN =
        (trip, n) -> {
            if (trip == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer bPtr = functions.tbool_value_n(ptr, n);
                if (bPtr == null) return null;
                return bPtr.getByte(0) != 0;
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // tfloat value accessor
    // ------------------------------------------------------------------

    // tfloatValueN(trip STRING, n INT) → DOUBLE  (n-th value, 1-based)
    // MEOS: tfloat_value_n writes double directly into a JNR-FFI buffer;
    //       the returned Pointer is JNR-FFI managed — DO NOT call MeosMemory.free on it.
    public static final UDF2<String, Integer, Double> tfloatValueN =
        (trip, n) -> {
            if (trip == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer dPtr = functions.tfloat_value_n(ptr, n);
                if (dPtr == null) return null;
                return dPtr.getDouble(0);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // ttext value accessors
    // ------------------------------------------------------------------

    // ttextMinValue(trip STRING) → STRING
    // MEOS: ttext_min_value(const Temporal *) → text *
    public static final UDF1<String, String> ttextMinValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer txtPtr = functions.ttext_min_value(ptr);
                if (txtPtr == null) return null;
                try {
                    return functions.text_out(txtPtr);
                } finally {
                    MeosMemory.free(txtPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ttextMaxValue(trip STRING) → STRING
    // MEOS: ttext_max_value(const Temporal *) → text *
    public static final UDF1<String, String> ttextMaxValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer txtPtr = functions.ttext_max_value(ptr);
                if (txtPtr == null) return null;
                try {
                    return functions.text_out(txtPtr);
                } finally {
                    MeosMemory.free(txtPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ttextValueN(trip STRING, n INT) → STRING  (n-th value, 1-based)
    // MEOS: ttext_value_n(const Temporal *, int) → text *
    public static final UDF2<String, Integer, String> ttextValueN =
        (trip, n) -> {
            if (trip == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer txtPtr = functions.ttext_value_n(ptr, n);
                if (txtPtr == null) return null;
                try {
                    return functions.text_out(txtPtr);
                } finally {
                    MeosMemory.free(txtPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // tpoint accessors
    // ------------------------------------------------------------------

    // tpointSrid(trip STRING) → INT
    // MEOS: tspatial_srid(const Temporal *) → int
    public static final UDF1<String, Integer> tpointSrid =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                return functions.tspatial_srid(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tpointValueN(trip STRING, n INT) → STRING  (WKT of n-th point, 1-based)
    // MEOS: tgeo_value_n(const Temporal *, int, GSERIALIZED **) → bool
    public static final UDF2<String, Integer, String> tpointValueN =
        (trip, n) -> {
            if (trip == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer gsPtr = functions.tgeo_value_n(ptr, n);
                if (gsPtr == null) return null;
                try {
                    return functions.geo_as_text(gsPtr, 6);
                } finally {
                    MeosMemory.free(gsPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tpointConvexHull(trip STRING) → STRING  (WKT of convex hull geometry)
    // MEOS: tgeo_convex_hull(const Temporal *) → GSERIALIZED *
    public static final UDF1<String, String> tpointConvexHull =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer gsPtr = functions.tgeo_convex_hull(ptr);
                if (gsPtr == null) return null;
                try {
                    return functions.geo_as_text(gsPtr, 6);
                } finally {
                    MeosMemory.free(gsPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // tint value accessor
    // ------------------------------------------------------------------

    // tintValueN(trip STRING, n INT) → INT  (n-th distinct value, 1-based)
    // MEOS: tint_value_n(const Temporal *, int n) → int * (JNR-allocated; do NOT MeosMemory.free)
    public static final UDF2<String, Integer, Integer> tintValueN =
        (trip, n) -> {
            if (trip == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer valPtr = functions.tint_value_n(ptr, n);
                if (valPtr == null) return null;
                return valPtr.getInt(0);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // value_at_timestamptz: retrieve value at a given instant
    //
    // MEOS: tbool_value_at_timestamptz / tint_value_at_timestamptz /
    //       tfloat_value_at_timestamptz (output-pointer pattern)
    // ------------------------------------------------------------------

    // tboolValueAtTimestamptz(tval STRING, ts TIMESTAMP) → BOOLEAN
    public static final UDF2<String, Timestamp, Boolean> tboolValueAtTimestamptz =
        (tval, ts) -> {
            if (tval == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(tval);
            if (ptr == null) return null;
            try {
                long pgEpochMicros = (ts.getTime() - PG_UNIX_EPOCH_OFFSET_MS) * 1000L;
                OffsetDateTime odt = OffsetDateTime.ofInstant(
                    Instant.ofEpochSecond(pgEpochMicros, 0), ZoneOffset.UTC);
                Pointer outVal = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(1);
                boolean found = functions.tbool_value_at_timestamptz(ptr, odt, false, outVal);
                if (!found) return null;
                return outVal.getByte(0) != 0;
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tintValueAtTimestamptz(tval STRING, ts TIMESTAMP) → INT
    public static final UDF2<String, Timestamp, Integer> tintValueAtTimestamptz =
        (tval, ts) -> {
            if (tval == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(tval);
            if (ptr == null) return null;
            try {
                long pgEpochMicros = (ts.getTime() - PG_UNIX_EPOCH_OFFSET_MS) * 1000L;
                OffsetDateTime odt = OffsetDateTime.ofInstant(
                    Instant.ofEpochSecond(pgEpochMicros, 0), ZoneOffset.UTC);
                Pointer outVal = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4);
                boolean found = functions.tint_value_at_timestamptz(ptr, odt, false, outVal);
                if (!found) return null;
                return outVal.getInt(0);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tfloatValueAtTimestamptz(tval STRING, ts TIMESTAMP) → DOUBLE
    public static final UDF2<String, Timestamp, Double> tfloatValueAtTimestamptz =
        (tval, ts) -> {
            if (tval == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(tval);
            if (ptr == null) return null;
            try {
                long pgEpochMicros = (ts.getTime() - PG_UNIX_EPOCH_OFFSET_MS) * 1000L;
                OffsetDateTime odt = OffsetDateTime.ofInstant(
                    Instant.ofEpochSecond(pgEpochMicros, 0), ZoneOffset.UTC);
                Pointer outVal = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                boolean found = functions.tfloat_value_at_timestamptz(ptr, odt, false, outVal);
                if (!found) return null;
                return outVal.getDouble(0);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ttextValueAtTimestamptz(tval STRING, ts TIMESTAMP) → STRING
    // MEOS: ttext_value_at_timestamptz(temp, t, strict, text **value) → bool
    //       outBuf holds a text* (MEOS-allocated) — use getPointer(0) then free.
    public static final UDF2<String, Timestamp, String> ttextValueAtTimestamptz =
        (tval, ts) -> {
            if (tval == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(tval);
            if (ptr == null) return null;
            try {
                long pgEpochMicros = (ts.getTime() - PG_UNIX_EPOCH_OFFSET_MS) * 1000L;
                OffsetDateTime odt = OffsetDateTime.ofInstant(
                    Instant.ofEpochSecond(pgEpochMicros, 0), ZoneOffset.UTC);
                Pointer outBuf = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                boolean found = functions.ttext_value_at_timestamptz(ptr, odt, false, outBuf);
                if (!found) return null;
                Pointer textPtr = outBuf.getPointer(0);
                if (textPtr == null) return null;
                try {
                    return functions.text_out(textPtr);
                } finally {
                    MeosMemory.free(textPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    public static void registerAll(SparkSession spark) {
        // Subtype
        spark.udf().register("temporalSubtype",  temporalSubtype,  DataTypes.StringType);
        // Instant navigation
        spark.udf().register("startInstant",     startInstant,     DataTypes.StringType);
        spark.udf().register("endInstant",       endInstant,       DataTypes.StringType);
        spark.udf().register("instantN",         instantN,         DataTypes.StringType);
        // Sequence navigation
        spark.udf().register("startSequence",    startSequence,    DataTypes.StringType);
        spark.udf().register("endSequence",      endSequence,      DataTypes.StringType);
        spark.udf().register("sequenceN",        sequenceN,        DataTypes.StringType);
        // Min/max instant
        spark.udf().register("minInstant",       minInstant,       DataTypes.StringType);
        spark.udf().register("maxInstant",       maxInstant,       DataTypes.StringType);
        // Timestamp accessors
        spark.udf().register("numTimestamps",    numTimestamps,    DataTypes.IntegerType);
        spark.udf().register("timestampN",       timestampN,       DataTypes.TimestampType);
        // Inclusivity flags
        spark.udf().register("lowerInc",         lowerInc,         DataTypes.BooleanType);
        spark.udf().register("upperInc",         upperInc,         DataTypes.BooleanType);
        // Duration
        spark.udf().register("duration",         duration,         DataTypes.StringType);
        // tbool value accessor
        spark.udf().register("tboolValueN",      tboolValueN,      DataTypes.BooleanType);
        // tfloat value accessor
        spark.udf().register("tfloatValueN",     tfloatValueN,     DataTypes.DoubleType);
        // ttext value accessors
        spark.udf().register("ttextMinValue",    ttextMinValue,    DataTypes.StringType);
        spark.udf().register("ttextMaxValue",    ttextMaxValue,    DataTypes.StringType);
        spark.udf().register("ttextValueN",      ttextValueN,      DataTypes.StringType);
        // tpoint accessors
        spark.udf().register("tpointSrid",                   tpointSrid,                   DataTypes.IntegerType);
        spark.udf().register("tpointValueN",                 tpointValueN,                 DataTypes.StringType);
        spark.udf().register("tpointConvexHull",             tpointConvexHull,             DataTypes.StringType);
        // value_at_timestamptz
        spark.udf().register("tintValueN",                   tintValueN,                   DataTypes.IntegerType);
        spark.udf().register("tboolValueAtTimestamptz",      tboolValueAtTimestamptz,      DataTypes.BooleanType);
        spark.udf().register("tintValueAtTimestamptz",       tintValueAtTimestamptz,       DataTypes.IntegerType);
        spark.udf().register("tfloatValueAtTimestamptz",     tfloatValueAtTimestamptz,     DataTypes.DoubleType);
        spark.udf().register("ttextValueAtTimestamptz",      ttextValueAtTimestamptz,      DataTypes.StringType);
        // Array-returning accessors
        spark.udf().register("temporalTimestamps",   temporalTimestamps,
            DataTypes.createArrayType(DataTypes.TimestampType));
        spark.udf().register("tboolValues",          tboolValues,
            DataTypes.createArrayType(DataTypes.BooleanType));
        spark.udf().register("tintValues",           tintValues,
            DataTypes.createArrayType(DataTypes.IntegerType));
        spark.udf().register("tfloatValues",         tfloatValues,
            DataTypes.createArrayType(DataTypes.DoubleType));
    }

    // ------------------------------------------------------------------
    // Array-returning accessors
    //
    // temporalTimestamps: temporal_timestamps(temp, sizeOut) → TimestampTz[]
    // tboolValues:        tbool_values(temp, sizeOut) → bool[]
    //
    // MEOS convention: sizeOut is int * (4 bytes); the returned C array is
    // palloc'd and must be freed after use.
    // ------------------------------------------------------------------

    private static final long PG_UNIX_OFFSET_MS = 946684800L * 1000L;

    // temporalTimestamps(hex STRING) → ARRAY<TIMESTAMP>
    // Returns the distinct timestamps at which the temporal has an instant.
    public static final UDF1<String, List<Timestamp>> temporalTimestamps =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            try {
                Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                Pointer arrPtr = functions.temporal_timestamps(ptr, sizeOut);
                if (arrPtr == null) return null;
                int count = sizeOut.getInt(0);
                List<Timestamp> result = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    long pgMicros = arrPtr.getLong((long) i * 8);
                    result.add(new Timestamp(pgMicros / 1000L + PG_UNIX_OFFSET_MS));
                }
                MeosMemory.free(arrPtr);
                return result;
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tboolValues(hex STRING) → ARRAY<BOOLEAN>
    // Returns the distinct boolean values present in a tbool.
    public static final UDF1<String, List<Boolean>> tboolValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            try {
                Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                Pointer arrPtr = functions.tbool_values(ptr, sizeOut);
                if (arrPtr == null) return null;
                int count = sizeOut.getInt(0);
                List<Boolean> result = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    result.add(arrPtr.getByte(i) != 0);
                }
                MeosMemory.free(arrPtr);
                return result;
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tintValues(hex STRING) → ARRAY<INTEGER>
    // Returns the distinct integer values present in a tint.
    public static final UDF1<String, List<Integer>> tintValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            try {
                Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                Pointer arrPtr = functions.tint_values(ptr, sizeOut);
                if (arrPtr == null) return null;
                int count = sizeOut.getInt(0);
                List<Integer> result = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    result.add(arrPtr.getInt((long) i * 4));
                }
                MeosMemory.free(arrPtr);
                return result;
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tfloatValues(hex STRING) → ARRAY<DOUBLE>
    // Returns the distinct float values present in a tfloat.
    public static final UDF1<String, List<Double>> tfloatValues =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            try {
                Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                Pointer arrPtr = functions.tfloat_values(ptr, sizeOut);
                if (arrPtr == null) return null;
                int count = sizeOut.getInt(0);
                List<Double> result = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    result.add(arrPtr.getDouble((long) i * 8));
                }
                MeosMemory.free(arrPtr);
                return result;
            } finally {
                MeosMemory.free(ptr);
            }
        };
}
