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
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for the temporal math / analytics functions not covered by
 * MathUDFs.java, registered under their canonical MobilityDB SQL names.
 *
 * Covers (temporal overloads): abs, ceil, floor, exp, ln, log10, degrees,
 * radians, round, derivative, deltavalue, integral, twavg, trend, and the
 * tnumber arithmetic operators (tnumber_add / tnumber_sub / tnumber_mul /
 * tnumber_div).
 *
 * All temporal inputs and outputs use hex-WKB string encoding.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class MathGapUDFs {

    private MathGapUDFs() {}

    private static String hexOut(Pointer r) {
        if (r == null) return null;
        try {
            return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
        } finally {
            MeosMemory.free(r);
        }
    }

    // ------------------------------------------------------------------
    // Unary temporal -> temporal (hex-WKB in, hex-WKB out)
    // ------------------------------------------------------------------

    // abs(tnumber) -> tnumber     MEOS: tnumber_abs
    public static final UDF1<String, String> abs =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tnumber_abs(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // ceil(tfloat) -> tfloat      MEOS: tfloat_ceil
    public static final UDF1<String, String> ceil =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_ceil(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // floor(tfloat) -> tfloat     MEOS: tfloat_floor
    public static final UDF1<String, String> floor =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_floor(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // exp(tfloat) -> tfloat       MEOS: tfloat_exp
    public static final UDF1<String, String> exp =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_exp(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // ln(tfloat) -> tfloat        MEOS: tfloat_ln
    public static final UDF1<String, String> ln =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_ln(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // log10(tfloat) -> tfloat     MEOS: tfloat_log10
    public static final UDF1<String, String> log10 =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_log10(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // radians(tfloat) -> tfloat   MEOS: tfloat_radians
    public static final UDF1<String, String> radians =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_radians(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // derivative(tfloat) -> tfloat   MEOS: temporal_derivative
    public static final UDF1<String, String> derivative =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.temporal_derivative(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // deltavalue(tnumber) -> tnumber   MEOS: tnumber_delta_value
    public static final UDF1<String, String> deltavalue =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tnumber_delta_value(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // trend(tnumber) -> tint           MEOS: tnumber_trend
    public static final UDF1<String, String> trend =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tnumber_trend(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // ------------------------------------------------------------------
    // degrees(tfloat, bool normalize) -> tfloat   MEOS: tfloat_degrees
    // round(temporal, int maxdd) -> temporal      MEOS: temporal_round
    // ------------------------------------------------------------------

    public static final UDF2<String, Boolean, String> degrees =
        (s, normalize) -> {
            if (s == null || normalize == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_degrees(ptr, normalize));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF2<String, Integer, String> round =
        (s, maxdd) -> {
            if (s == null || maxdd == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.temporal_round(ptr, maxdd));
            } finally { MeosMemory.free(ptr); }
        };

    // ------------------------------------------------------------------
    // Unary temporal -> double
    // ------------------------------------------------------------------

    // integral(tnumber) -> float   MEOS: tnumber_integral
    public static final UDF1<String, Double> integral =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return GeneratedFunctions.tnumber_integral(ptr);
            } finally { MeosMemory.free(ptr); }
        };

    // twavg(tnumber) -> float      MEOS: tnumber_twavg
    public static final UDF1<String, Double> twavg =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return GeneratedFunctions.tnumber_twavg(ptr);
            } finally { MeosMemory.free(ptr); }
        };

    // ------------------------------------------------------------------
    // tnumber OP tnumber  (hex-WKB in, hex-WKB out)
    //
    // MEOS: add_tnumber_tnumber / sub_tnumber_tnumber /
    //       mul_tnumber_tnumber / div_tnumber_tnumber
    // ------------------------------------------------------------------

    public static final UDF2<String, String, String> tnumber_add =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    return hexOut(GeneratedFunctions.add_tnumber_tnumber(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, String> tnumber_sub =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    return hexOut(GeneratedFunctions.sub_tnumber_tnumber(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, String> tnumber_mul =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    return hexOut(GeneratedFunctions.mul_tnumber_tnumber(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, String> tnumber_div =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    return hexOut(GeneratedFunctions.div_tnumber_tnumber(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // REGISTRATION
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        // unary temporal -> temporal
        spark.udf().register("abs",        abs,        DataTypes.StringType);
        spark.udf().register("ceil",       ceil,       DataTypes.StringType);
        spark.udf().register("floor",      floor,      DataTypes.StringType);
        spark.udf().register("exp",        exp,        DataTypes.StringType);
        spark.udf().register("ln",         ln,         DataTypes.StringType);
        spark.udf().register("log10",      log10,      DataTypes.StringType);
        spark.udf().register("radians",    radians,    DataTypes.StringType);
        spark.udf().register("derivative", derivative, DataTypes.StringType);
        spark.udf().register("deltavalue", deltavalue, DataTypes.StringType);
        spark.udf().register("trend",      trend,      DataTypes.StringType);
        // unary temporal + param -> temporal
        spark.udf().register("degrees",    degrees,    DataTypes.StringType);
        spark.udf().register("round",      round,      DataTypes.StringType);
        // unary temporal -> double
        spark.udf().register("integral",   integral,   DataTypes.DoubleType);
        spark.udf().register("twavg",      twavg,      DataTypes.DoubleType);
        // tnumber OP tnumber
        spark.udf().register("tnumber_add", tnumber_add, DataTypes.StringType);
        spark.udf().register("tnumber_sub", tnumber_sub, DataTypes.StringType);
        spark.udf().register("tnumber_mul", tnumber_mul, DataTypes.StringType);
        spark.udf().register("tnumber_div", tnumber_div, DataTypes.StringType);
    }
}
