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
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Spark SQL UDFs for converting and transforming temporal types.
 *
 * Covers: subtype conversion (to TInstant/TSequence/TSequenceSet),
 * interpolation change, type casting (tfloat↔tint), value-domain
 * shifting and scaling, time-domain shifting and scaling, SRID
 * assignment, coordinate rounding, and trajectory simplification.
 *
 * All temporal values are encoded as hex-WKB Strings. Interpolation
 * is expressed as a String: "Discrete", "Step", or "Linear".
 * Interval arguments use PostgreSQL interval literal syntax, e.g.
 * "1 day" or "01:00:00".
 *
 * Memory management: every native Pointer allocated by MEOS is freed
 * via MeosMemory.free() in a finally block to prevent native heap
 * leakage across UDF calls.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
public final class TransformUDFs {

    private TransformUDFs() {}

    // interpType constants from meos.h: DISCRETE=1, STEP=2, LINEAR=3
    private static int interpToInt(String interp) {
        if ("Discrete".equalsIgnoreCase(interp)) return 1;
        if ("Step".equalsIgnoreCase(interp))     return 2;
        if ("Linear".equalsIgnoreCase(interp))   return 3;
        throw new IllegalArgumentException("Unknown interpolation: " + interp);
    }

    // ------------------------------------------------------------------
    // Subtype conversion
    // ------------------------------------------------------------------

    // temporalToTInstant(s STRING) → STRING
    // MEOS: temporal_to_tinstant(const Temporal *) → TInstant *
    public static final UDF1<String, String> temporalToTInstant =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.temporal_to_tinstant(ptr);
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

    // temporalToTSequence(s STRING, interp STRING) → STRING
    // interp: "Discrete" | "Step" | "Linear"
    // MEOS: temporal_to_tsequence(const Temporal *, interpType interp) → TSequence *
    public static final UDF2<String, String, String> temporalToTSequence =
        (s, interp) -> {
            if (s == null || interp == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.temporal_to_tsequence(ptr, interp);
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

    // temporalToTSequenceSet(s STRING, interp STRING) → STRING
    // interp: "Discrete" | "Step" | "Linear"
    // MEOS: temporal_to_tsequenceset(const Temporal *, interpType interp) → TSequenceSet *
    public static final UDF2<String, String, String> temporalToTSequenceSet =
        (s, interp) -> {
            if (s == null || interp == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.temporal_to_tsequenceset(ptr, interp);
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
    // Interpolation change
    // ------------------------------------------------------------------

    // temporalSetInterp(s STRING, interpStr STRING) → STRING
    // interpStr: "Discrete" → 1, "Step" → 2, "Linear" → 3
    // MEOS: temporal_set_interp(const Temporal *, interpType interp) → Temporal *
    public static final UDF2<String, String, String> temporalSetInterp =
        (s, interpStr) -> {
            if (s == null || interpStr == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                int interpInt = interpToInt(interpStr);
                Pointer result = functions.temporal_set_interp(ptr, interpInt);
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
    // Type casting
    // ------------------------------------------------------------------

    // tfloatToTint(s STRING) → STRING
    // MEOS: tfloat_to_tint(const Temporal *) → Temporal *
    public static final UDF1<String, String> tfloatToTint =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.tfloat_to_tint(ptr);
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
    // Value-domain shifting and scaling (tfloat)
    // ------------------------------------------------------------------

    // tfloatShiftValue(s STRING, shift DOUBLE) → STRING
    // MEOS: tfloat_shift_value(const Temporal *, double) → Temporal *
    public static final UDF2<String, Double, String> tfloatShiftValue =
        (s, shift) -> {
            if (s == null || shift == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.tfloat_shift_value(ptr, shift);
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

    // tfloatScaleValue(s STRING, width DOUBLE) → STRING
    // MEOS: tfloat_scale_value(const Temporal *, double) → Temporal *
    public static final UDF2<String, Double, String> tfloatScaleValue =
        (s, width) -> {
            if (s == null || width == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.tfloat_scale_value(ptr, width);
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

    // tfloatShiftScaleValue(s STRING, shift DOUBLE, width DOUBLE) → STRING
    // MEOS: tfloat_shift_scale_value(const Temporal *, double shift, double width) → Temporal *
    public static final UDF3<String, Double, Double, String> tfloatShiftScaleValue =
        (s, shift, width) -> {
            if (s == null || shift == null || width == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.tfloat_shift_scale_value(ptr, shift, width);
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
    // Time-domain shifting and scaling
    // ------------------------------------------------------------------

    // temporalShiftTime(s STRING, shiftStr STRING) → STRING
    // MEOS: temporal_shift_time(const Temporal *, const Interval *) → Temporal *
    public static final UDF2<String, String, String> temporalShiftTime =
        (s, shiftStr) -> {
            if (s == null || shiftStr == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer shiftPtr = functions.pg_interval_in(shiftStr, -1);
                if (shiftPtr == null) return null;
                try {
                    Pointer result = functions.temporal_shift_time(tptr, shiftPtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(shiftPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // temporalScaleTime(s STRING, scaleStr STRING) → STRING
    // MEOS: temporal_scale_time(const Temporal *, const Interval *) → Temporal *
    public static final UDF2<String, String, String> temporalScaleTime =
        (s, scaleStr) -> {
            if (s == null || scaleStr == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer scalePtr = functions.pg_interval_in(scaleStr, -1);
                if (scalePtr == null) return null;
                try {
                    Pointer result = functions.temporal_scale_time(tptr, scalePtr);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(scalePtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // temporalShiftScaleTime(s STRING, shiftStr STRING, scaleStr STRING) → STRING
    // shiftStr and scaleStr are PostgreSQL interval literals, e.g. "1 day".
    // MEOS: temporal_shift_scale_time(const Temporal *, const Interval *, const Interval *) → Temporal *
    public static final UDF3<String, String, String, String> temporalShiftScaleTime =
        (s, shiftStr, scaleStr) -> {
            if (s == null || shiftStr == null || scaleStr == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(s);
            if (tptr == null) return null;
            try {
                Pointer shiftPtr = functions.pg_interval_in(shiftStr, -1);
                if (shiftPtr == null) return null;
                try {
                    Pointer scalePtr = functions.pg_interval_in(scaleStr, -1);
                    if (scalePtr == null) return null;
                    try {
                        Pointer result = functions.temporal_shift_scale_time(tptr, shiftPtr, scalePtr);
                        if (result == null) return null;
                        try {
                            return functions.temporal_as_hexwkb(result, (byte) 0);
                        } finally {
                            MeosMemory.free(result);
                        }
                    } finally {
                        MeosMemory.free(scalePtr);
                    }
                } finally {
                    MeosMemory.free(shiftPtr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Spatial transformations (tpoint)
    // ------------------------------------------------------------------

    // tpointSetSrid(s STRING, srid INT) → STRING
    // MEOS: tspatial_set_srid(const Temporal *, int32_t srid) → Temporal *
    public static final UDF2<String, Integer, String> tpointSetSrid =
        (s, srid) -> {
            if (s == null || srid == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.tspatial_set_srid(ptr, srid);
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

    // tpointRound(s STRING, maxdd INT) → STRING
    // MEOS: temporal_round(const Temporal *, int maxdd) → Temporal *
    public static final UDF2<String, Integer, String> tpointRound =
        (s, maxdd) -> {
            if (s == null || maxdd == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.temporal_round(ptr, maxdd);
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
    // Trajectory simplification
    // ------------------------------------------------------------------

    // temporalSimplifyDp(s STRING, dist DOUBLE) → STRING
    // Uses the Douglas-Peucker algorithm with synchronized=false.
    // MEOS: temporal_simplify_dp(const Temporal *, double eps_dist, bool synchronized) → Temporal *
    public static final UDF2<String, Double, String> temporalSimplifyDp =
        (s, dist) -> {
            if (s == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.temporal_simplify_dp(ptr, dist, false);
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

    // temporalSimplifyMaxDist(s STRING, dist DOUBLE) → STRING
    // Uses maximum-distance simplification with synchronized=false.
    // MEOS: temporal_simplify_max_dist(const Temporal *, double eps_dist, bool synchronized) → Temporal *
    public static final UDF2<String, Double, String> temporalSimplifyMaxDist =
        (s, dist) -> {
            if (s == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.temporal_simplify_max_dist(ptr, dist, false);
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

    // temporalSimplifyMinDist(s STRING, dist DOUBLE) → STRING
    // Removes consecutive instants whose distance is below the threshold.
    // MEOS: temporal_simplify_min_dist(const Temporal *, double dist) → Temporal *
    public static final UDF2<String, Double, String> temporalSimplifyMinDist =
        (s, dist) -> {
            if (s == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer result = functions.temporal_simplify_min_dist(ptr, dist);
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

    // temporalTSample(s STRING, durationStr STRING, interpStr STRING) → STRING
    // Re-samples a temporal value at regular time intervals.
    // origin is fixed at 2000-01-01 00:00:00 UTC (MEOS/PG epoch).
    // MEOS: temporal_tsample(const Temporal *, const Interval *, TimestampTz, interpType) → Temporal *
    private static final OffsetDateTime PG_EPOCH = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    public static final UDF3<String, String, String, String> temporalTSample =
        (s, durationStr, interpStr) -> {
            if (s == null || durationStr == null || interpStr == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer ivPtr = functions.pg_interval_in(durationStr, -1);
                if (ivPtr == null) return null;
                try {
                    int interp;
                    switch (interpStr) {
                        case "Discrete": interp = 1; break;
                        case "Step":     interp = 2; break;
                        default:         interp = 3; break;
                    }
                    Pointer result = functions.temporal_tsample(ptr, ivPtr, PG_EPOCH, interp);
                    if (result == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(result, (byte) 0);
                    } finally {
                        MeosMemory.free(result);
                    }
                } finally {
                    MeosMemory.free(ivPtr);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // tpointTrajectory(s STRING) → STRING  (WKT of the trajectory geometry)
    // For a tgeompoint sequence, this returns a LINESTRING or POINT geometry.
    // MEOS: tpoint_trajectory(const Temporal *, bool unary_union) → GSERIALIZED *
    public static final UDF1<String, String> tpointTrajectory =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer gsPtr = functions.tpoint_trajectory(ptr, false);
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

    // tpointMakeSimple(s STRING) → STRING
    // Splits a self-intersecting temporal point into non-self-intersecting parts.
    // Returns the first non-self-intersecting piece (for simplicity in SQL context).
    // MEOS: tpoint_make_simple(const Temporal *, int *count) → Temporal **
    // (omitted: tpoint_make_simple returns an array; complex to expose as single UDF)

    // ------------------------------------------------------------------
    // Registration
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        // Subtype conversion
        spark.udf().register("temporalToTInstant",    temporalToTInstant,    DataTypes.StringType);
        spark.udf().register("temporalToTSequence",   temporalToTSequence,   DataTypes.StringType);
        spark.udf().register("temporalToTSequenceSet", temporalToTSequenceSet, DataTypes.StringType);
        // Interpolation change
        spark.udf().register("temporalSetInterp",     temporalSetInterp,     DataTypes.StringType);
        // Type casting
        spark.udf().register("tfloatToTint",          tfloatToTint,          DataTypes.StringType);
        // Value-domain shifting and scaling
        spark.udf().register("tfloatShiftValue",      tfloatShiftValue,      DataTypes.StringType);
        spark.udf().register("tfloatScaleValue",      tfloatScaleValue,      DataTypes.StringType);
        spark.udf().register("tfloatShiftScaleValue", tfloatShiftScaleValue, DataTypes.StringType);
        // Time-domain shifting and scaling
        spark.udf().register("temporalShiftTime",       temporalShiftTime,       DataTypes.StringType);
        spark.udf().register("temporalScaleTime",       temporalScaleTime,       DataTypes.StringType);
        spark.udf().register("temporalShiftScaleTime", temporalShiftScaleTime, DataTypes.StringType);
        // Spatial transformations
        spark.udf().register("tpointSetSrid",         tpointSetSrid,         DataTypes.StringType);
        spark.udf().register("tpointRound",           tpointRound,           DataTypes.StringType);
        // Trajectory simplification
        spark.udf().register("temporalSimplifyDp",      temporalSimplifyDp,      DataTypes.StringType);
        spark.udf().register("temporalSimplifyMaxDist", temporalSimplifyMaxDist, DataTypes.StringType);
        spark.udf().register("temporalSimplifyMinDist", temporalSimplifyMinDist, DataTypes.StringType);
        // Temporal sampling
        spark.udf().register("temporalTSample", temporalTSample, DataTypes.StringType);
        // Trajectory extraction
        spark.udf().register("tpointTrajectory", tpointTrajectory, DataTypes.StringType);
    }
}
