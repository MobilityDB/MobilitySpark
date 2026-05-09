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
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for temporal analytics: numeric math and spatial aggregates.
 *
 * All temporal inputs use hex-WKB string encoding. Scalar outputs (length,
 * integral, twavg) are returned as Java primitive types.
 *
 * MEOS function authority: meos/include/meos.h and meos/include/meos_geo.h
 */
public final class AnalyticsUDFs {

    private AnalyticsUDFs() {}

    private static Pointer tempPtr(String hex) {
        return hex == null ? null : functions.temporal_from_hexwkb(hex);
    }

    // ------------------------------------------------------------------
    // tfloat math  (hex-WKB in, hex-WKB out)
    //
    // MEOS: tfloat_derivative / tfloat_round / tfloat_floor / tfloat_ceil
    //       tfloat_degrees / tfloat_radians
    // ------------------------------------------------------------------

    // tfloatDerivative("[1.0@2020-01-01, 3.0@2020-01-02]") → instantaneous rate of change
    // MEOS: tfloat_derivative(Temporal *) → Temporal *
    public static final UDF1<String, String> tfloatDerivative =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer r = functions.temporal_derivative(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tfloatRound("[1.23456@2020-01-01]", 3) → "[1.235@2020-01-01]"
    // MEOS: tfloat_round(Temporal *, int maxdd) → Temporal *
    public static final UDF2<String, Integer, String> tfloatRound =
        (s, maxdd) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || maxdd == null) return null;
            Pointer r = functions.temporal_round(ptr, maxdd);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tfloatFloor("[1.7@2020-01-01]") → "[1.0@2020-01-01]"
    // MEOS: tfloat_floor(Temporal *) → Temporal *
    public static final UDF1<String, String> tfloatFloor =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer r = functions.tfloat_floor(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tfloatCeil("[1.2@2020-01-01]") → "[2.0@2020-01-01]"
    // MEOS: tfloat_ceil(Temporal *) → Temporal *
    public static final UDF1<String, String> tfloatCeil =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer r = functions.tfloat_ceil(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tfloatDegrees("[1.5707963@2020-01-01]", false) → "[90.0@2020-01-01]"
    // MEOS: tfloat_degrees(Temporal *, bool normalize) → Temporal *
    public static final UDF2<String, Boolean, String> tfloatDegrees =
        (s, normalize) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null || normalize == null) return null;
            Pointer r = functions.tfloat_degrees(ptr, normalize);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tfloatRadians("[90.0@2020-01-01]") → "[1.5707963...@2020-01-01]"
    // MEOS: tfloat_radians(Temporal *) → Temporal *
    public static final UDF1<String, String> tfloatRadians =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer r = functions.tfloat_radians(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // tnumber scalar aggregates  (hex-WKB in, scalar out)
    //
    // MEOS: tnumber_integral / tnumber_twavg
    // ------------------------------------------------------------------

    // tnumberIntegral("[1.0@2020-01-01, 3.0@2020-01-03]") → 172800.0 (area under curve in seconds)
    // MEOS: tnumber_integral(Temporal *) → double
    public static final UDF1<String, Double> tnumberIntegral =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            return functions.tnumber_integral(ptr);
        };

    // tnumberTwavg("[1.0@2020-01-01, 3.0@2020-01-03]") → time-weighted average
    // MEOS: tnumber_twavg(Temporal *) → double
    public static final UDF1<String, Double> tnumberTwavg =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            return functions.tnumber_twavg(ptr);
        };

    // ------------------------------------------------------------------
    // tpoint spatial analytics  (hex-WKB in, scalar/hex out)
    //
    // MEOS: tpoint_length / tpoint_speed / tpoint_azimuth / tpoint_direction
    // ------------------------------------------------------------------

    // tpointLength(trip) → total path length in metres (geodetic) or projection units
    // MEOS: tpoint_length(Temporal *) → double
    public static final UDF1<String, Double> tpointLength =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            return functions.tpoint_length(ptr);
        };

    // tpointSpeed(trip) → instantaneous speed as tfloat hex-WKB
    // MEOS: tpoint_speed(Temporal *) → Temporal *
    public static final UDF1<String, String> tpointSpeed =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer r = functions.tpoint_speed(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tpointAzimuth(trip) → direction of motion as tfloat hex-WKB (radians)
    // MEOS: tpoint_azimuth(Temporal *) → Temporal *
    public static final UDF1<String, String> tpointAzimuth =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer r = functions.tpoint_azimuth(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tpointDirection(trip) → overall direction as tfloat hex-WKB (radians)
    // MEOS: tpoint_direction(Temporal *) → Temporal *
    public static final UDF1<String, String> tpointDirection =
        (s) -> {
            MeosThread.ensureReady();
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer r = functions.tpoint_direction(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static void registerAll(SparkSession spark) {
        // tfloat math
        spark.udf().register("tfloatDerivative", tfloatDerivative, DataTypes.StringType);
        spark.udf().register("tfloatRound",       tfloatRound,      DataTypes.StringType);
        spark.udf().register("tfloatFloor",       tfloatFloor,      DataTypes.StringType);
        spark.udf().register("tfloatCeil",        tfloatCeil,       DataTypes.StringType);
        spark.udf().register("tfloatDegrees",     tfloatDegrees,    DataTypes.StringType);
        spark.udf().register("tfloatRadians",     tfloatRadians,    DataTypes.StringType);
        // tnumber aggregates
        spark.udf().register("tnumberIntegral",   tnumberIntegral,  DataTypes.DoubleType);
        spark.udf().register("tnumberTwavg",      tnumberTwavg,     DataTypes.DoubleType);
        // tpoint analytics
        spark.udf().register("tpointLength",      tpointLength,     DataTypes.DoubleType);
        spark.udf().register("tpointSpeed",       tpointSpeed,      DataTypes.StringType);
        spark.udf().register("tpointAzimuth",     tpointAzimuth,    DataTypes.StringType);
        spark.udf().register("tpointDirection",   tpointDirection,  DataTypes.StringType);
    }
}
