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
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.mobilitydb.spark.util.TimeUtil;

/**
 * Spark SQL UDFs for the temporal value/structure accessor and
 * transform functions not covered by AccessorUDFs.java,
 * MoreAccessorUDFs.java, or SpanAccessorUDFs.java.
 *
 * Each function exposes the TEMPORAL overload of the corresponding
 * MobilityDB SQL function. All temporal values are encoded as hex-WKB
 * Strings; geometry results are returned as WKT Strings.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
public final class AccessorGapUDFs {

    private AccessorGapUDFs() {}

    // PG/MEOS epoch origin (2000-01-01 00:00:00 UTC) used for the default
    // tprecision / tsample origin argument.
    private static final OffsetDateTime PG_EPOCH =
        OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    // ------------------------------------------------------------------
    // Value accessors returning a geometry (trgeometry overload)
    //
    // The generic temporal_*_value functions return a bare Datum (int),
    // whose interpretation depends on the temporal base type and is thus
    // ambiguous for a single typed UDF. The trgeometry overloads return a
    // GSERIALIZED *, which maps cleanly to a WKT String, so they are the
    // ones implemented here.
    // ------------------------------------------------------------------

    // startValue(trgeometry STRING) → STRING (WKT of the first geometry value)
    // MEOS: trgeometry_start_value(const Temporal *) → GSERIALIZED *
    public static final UDF1<String, String> startValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer gs = GeneratedFunctions.trgeometry_start_value(ptr);
                if (gs == null) return null;
                try {
                    return GeneratedFunctions.geo_as_text(gs, 15);
                } finally {
                    MeosMemory.free(gs);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // endValue(trgeometry STRING) → STRING (WKT of the last geometry value)
    // MEOS: trgeometry_end_value(const Temporal *) → GSERIALIZED *
    public static final UDF1<String, String> endValue =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer gs = GeneratedFunctions.trgeometry_end_value(ptr);
                if (gs == null) return null;
                try {
                    return GeneratedFunctions.geo_as_text(gs, 15);
                } finally {
                    MeosMemory.free(gs);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // valueN(trgeometry STRING, n INT) → STRING (WKT of the n-th geometry value, 1-based)
    // MEOS: trgeometry_value_n(const Temporal *, int) → GSERIALIZED *
    public static final UDF2<String, Integer, String> valueN =
        (trip, n) -> {
            if (trip == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer gs = GeneratedFunctions.trgeometry_value_n(ptr, n);
                if (gs == null) return null;
                try {
                    return GeneratedFunctions.geo_as_text(gs, 15);
                } finally {
                    MeosMemory.free(gs);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Restriction functions returning a temporal value
    // ------------------------------------------------------------------

    // minusValue(tcbuffer STRING, cbuffer STRING) → STRING (hex-WKB tcbuffer)
    // MEOS: tcbuffer_minus_cbuffer(const Temporal *, const Cbuffer *) → Temporal *
    public static final UDF2<String, String, String> minusValue =
        (trip, cbHex) -> {
            if (trip == null || cbHex == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer cb = GeneratedFunctions.cbuffer_from_hexwkb(cbHex);
                if (cb == null) return null;
                try {
                    Pointer r = GeneratedFunctions.tcbuffer_minus_cbuffer(ptr, cb);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(cb);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // minusValues(temporal STRING, set STRING) → STRING (hex-WKB temporal)
    // MEOS: temporal_minus_values(const Temporal *, const Set *) → Temporal *
    public static final UDF2<String, String, String> minusValues =
        (trip, setHex) -> {
            if (trip == null || setHex == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer set = GeneratedFunctions.set_from_hexwkb(setHex);
                if (set == null) return null;
                try {
                    Pointer r = GeneratedFunctions.temporal_minus_values(ptr, set);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(set);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // atValue(tcbuffer STRING, cbuffer STRING) → STRING (hex-WKB tcbuffer)
    // MEOS: tcbuffer_at_cbuffer(const Temporal *, const Cbuffer *) → Temporal *
    public static final UDF2<String, String, String> atValue =
        (trip, cbHex) -> {
            if (trip == null || cbHex == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer cb = GeneratedFunctions.cbuffer_from_hexwkb(cbHex);
                if (cb == null) return null;
                try {
                    Pointer r = GeneratedFunctions.tcbuffer_at_cbuffer(ptr, cb);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(cb);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Structure accessors returning an array of temporal fragments
    //
    // Each function returns a Temporal** (palloc'd outer array of view
    // pointers). The outer array must be freed; the elements are views into
    // the original temporal and must NOT be freed.
    // ------------------------------------------------------------------

    private static List<String> temporalPtrArray(String hex,
            java.util.function.BiFunction<Pointer, Pointer, Pointer> fn) {
        if (hex == null) return null;
        MeosThread.ensureReady();
        Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(hex);
        if (ptr == null) return null;
        try {
            Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
            Pointer arrPtr = fn.apply(ptr, sizeOut);
            if (arrPtr == null) return null;
            int count = sizeOut.getInt(0);
            List<String> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Pointer elem = arrPtr.getPointer((long) i * 8);
                if (elem != null) {
                    String h = GeneratedFunctions.temporal_as_hexwkb(elem, (byte) 0);
                    if (h != null) result.add(h);
                }
            }
            MeosMemory.free(arrPtr);
            return result;
        } finally {
            MeosMemory.free(ptr);
        }
    }

    // instants(temporal STRING) → ARRAY<STRING> (hex-WKB of each instant)
    // MEOS: temporal_instants(const Temporal *, int *count) → TInstant **
    public static final UDF1<String, List<String>> instants =
        (hex) -> temporalPtrArray(hex, GeneratedFunctions::temporal_instants);

    // sequences(temporal STRING) → ARRAY<STRING> (hex-WKB of each sequence)
    // MEOS: temporal_sequences(const Temporal *, int *count) → TSequence **
    public static final UDF1<String, List<String>> sequences =
        (hex) -> temporalPtrArray(hex, GeneratedFunctions::temporal_sequences);

    // segments(temporal STRING) → ARRAY<STRING> (hex-WKB of each segment)
    // MEOS: temporal_segments(const Temporal *, int *count) → TSequence **
    public static final UDF1<String, List<String>> segments =
        (hex) -> temporalPtrArray(hex, GeneratedFunctions::temporal_segments);

    // timestamps(temporal STRING) → ARRAY<TIMESTAMP> (distinct instant times)
    // MEOS: temporal_timestamps(const Temporal *, int *count) → TimestampTz *
    public static final UDF1<String, List<Timestamp>> timestamps =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            try {
                Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                Pointer arrPtr = GeneratedFunctions.temporal_timestamps(ptr, sizeOut);
                if (arrPtr == null) return null;
                int count = sizeOut.getInt(0);
                List<Timestamp> result = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    long pgMicros = arrPtr.getLong((long) i * 8);
                    result.add(new Timestamp(pgMicros / 1000L + TimeUtil.PG_UNIX_EPOCH_OFFSET_MS));
                }
                MeosMemory.free(arrPtr);
                return result;
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // traversedArea (geometry result)
    // ------------------------------------------------------------------

    // traversedArea(tgeometry STRING, unaryUnion BOOLEAN) → STRING (WKT geometry)
    // MEOS: tgeo_traversed_area(const Temporal *, bool unary_union) → GSERIALIZED *
    public static final UDF2<String, Boolean, String> traversedArea =
        (trip, unaryUnion) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer gs = GeneratedFunctions.tgeo_traversed_area(ptr,
                    unaryUnion != null && unaryUnion);
                if (gs == null) return null;
                try {
                    return GeneratedFunctions.geo_as_text(gs, 15);
                } finally {
                    MeosMemory.free(gs);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // getSpace (stbox result)
    // ------------------------------------------------------------------

    // getSpace(stbox STRING) → STRING (hex-WKB of the spatial-only stbox)
    // MEOS: stbox_get_space(const STBox *) → STBox *
    public static final UDF1<String, String> getSpace =
        (boxHex) -> {
            if (boxHex == null) return null;
            MeosThread.ensureReady();
            Pointer box = GeneratedFunctions.stbox_from_hexwkb(boxHex);
            if (box == null) return null;
            try {
                Pointer r = GeneratedFunctions.stbox_get_space(box);
                if (r == null) return null;
                try {
                    Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    return GeneratedFunctions.stbox_as_hexwkb(r, (byte) 0, sizeOut);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(box);
            }
        };

    // ------------------------------------------------------------------
    // tprecision / tsample (temporal result)
    // ------------------------------------------------------------------

    // tprecision(temporal STRING, durationStr STRING) → STRING (hex-WKB temporal)
    // origin is fixed at the PG/MEOS epoch (2000-01-01 00:00:00 UTC).
    // MEOS: temporal_tprecision(const Temporal *, const Interval *, TimestampTz) → Temporal *
    public static final UDF2<String, String, String> tprecision =
        (trip, durationStr) -> {
            if (trip == null || durationStr == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer ivPtr = GeneratedFunctions.pg_interval_in(durationStr, -1);
                if (ivPtr == null) return null;
                try {
                    Pointer r = GeneratedFunctions.temporal_tprecision(ptr, ivPtr, PG_EPOCH);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(ivPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tsample(temporal STRING, durationStr STRING, interpStr STRING) → STRING
    // origin is fixed at the PG/MEOS epoch (2000-01-01 00:00:00 UTC).
    // interp: "Discrete" | "Step" | "Linear" (interpType DISCRETE=1, STEP=2, LINEAR=3).
    // MEOS: temporal_tsample(const Temporal *, const Interval *, TimestampTz, interpType) → Temporal *
    public static final UDF3<String, String, String, String> tsample =
        (trip, durationStr, interpStr) -> {
            if (trip == null || durationStr == null || interpStr == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            try {
                Pointer ivPtr = GeneratedFunctions.pg_interval_in(durationStr, -1);
                if (ivPtr == null) return null;
                try {
                    int interp;
                    if ("Discrete".equalsIgnoreCase(interpStr)) interp = 1;
                    else if ("Step".equalsIgnoreCase(interpStr)) interp = 2;
                    else interp = 3;
                    Pointer r = GeneratedFunctions.temporal_tsample(ptr, ivPtr, PG_EPOCH, interp);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(ivPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    public static void registerAll(SparkSession spark) {
        // Value accessors (trgeometry → WKT)
        spark.udf().register("startValue",    startValue,    DataTypes.StringType);
        spark.udf().register("endValue",      endValue,      DataTypes.StringType);
        spark.udf().register("valueN",        valueN,        DataTypes.StringType);
        // Restriction functions
        spark.udf().register("minusValue",    minusValue,    DataTypes.StringType);
        spark.udf().register("minusValues",   minusValues,   DataTypes.StringType);
        spark.udf().register("atValue",       atValue,       DataTypes.StringType);
        // Structure accessors (arrays)
        spark.udf().register("instants",      instants,
            DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("sequences",     sequences,
            DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("segments",      segments,
            DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("timestamps",    timestamps,
            DataTypes.createArrayType(DataTypes.TimestampType));
        // Geometry / stbox / transform
        spark.udf().register("traversedArea", traversedArea, DataTypes.StringType);
        spark.udf().register("getSpace",      getSpace,      DataTypes.StringType);
        spark.udf().register("tprecision",    tprecision,    DataTypes.StringType);
        spark.udf().register("tsample",       tsample,       DataTypes.StringType);
    }
}
