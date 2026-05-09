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
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for temporal analytics: numeric math and spatial aggregates.
 *
 * All temporal inputs use hex-WKB string encoding. Scalar outputs (length,
 * integral, twavg) are returned as Java primitive types.
 *
 * Memory management: every Pointer returned by MEOS must be freed via
 * MeosMemory.free() — see GeoUDFs for the rationale.
 *
 * MEOS function authority: meos/include/meos.h and meos/include/meos_geo.h
 */
public final class AnalyticsUDFs {

    private AnalyticsUDFs() {}

    // ------------------------------------------------------------------
    // tfloat math  (hex-WKB in, hex-WKB out)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tfloatDerivative =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_derivative(ptr);
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

    public static final UDF2<String, Integer, String> tfloatRound =
        (s, maxdd) -> {
            if (s == null || maxdd == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.temporal_round(ptr, maxdd);
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

    public static final UDF1<String, String> tfloatFloor =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.tfloat_floor(ptr);
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

    public static final UDF1<String, String> tfloatCeil =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.tfloat_ceil(ptr);
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

    public static final UDF2<String, Boolean, String> tfloatDegrees =
        (s, normalize) -> {
            if (s == null || normalize == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.tfloat_degrees(ptr, normalize);
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

    public static final UDF1<String, String> tfloatRadians =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.tfloat_radians(ptr);
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
    // tnumber scalar aggregates  (hex-WKB in, scalar out)
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> tnumberIntegral =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return functions.tnumber_integral(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    public static final UDF1<String, Double> tnumberTwavg =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return functions.tnumber_twavg(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // tpoint spatial analytics  (hex-WKB in, scalar/hex out)
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> tpointLength =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return functions.tpoint_length(ptr);
            } finally {
                MeosMemory.free(ptr);
            }
        };

    public static final UDF1<String, String> tpointSpeed =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.tpoint_speed(ptr);
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

    public static final UDF1<String, String> tpointAzimuth =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.tpoint_azimuth(ptr);
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

    public static final UDF1<String, String> tpointDirection =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = functions.tpoint_direction(ptr);
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

    public static void registerAll(SparkSession spark) {
        spark.udf().register("tfloatDerivative", tfloatDerivative, DataTypes.StringType);
        spark.udf().register("tfloatRound",       tfloatRound,      DataTypes.StringType);
        spark.udf().register("tfloatFloor",       tfloatFloor,      DataTypes.StringType);
        spark.udf().register("tfloatCeil",        tfloatCeil,       DataTypes.StringType);
        spark.udf().register("tfloatDegrees",     tfloatDegrees,    DataTypes.StringType);
        spark.udf().register("tfloatRadians",     tfloatRadians,    DataTypes.StringType);
        spark.udf().register("tnumberIntegral",   tnumberIntegral,  DataTypes.DoubleType);
        spark.udf().register("tnumberTwavg",      tnumberTwavg,     DataTypes.DoubleType);
        spark.udf().register("tpointLength",      tpointLength,     DataTypes.DoubleType);
        spark.udf().register("tpointSpeed",       tpointSpeed,      DataTypes.StringType);
        spark.udf().register("tpointAzimuth",     tpointAzimuth,    DataTypes.StringType);
        spark.udf().register("tpointDirection",   tpointDirection,  DataTypes.StringType);
    }
}
