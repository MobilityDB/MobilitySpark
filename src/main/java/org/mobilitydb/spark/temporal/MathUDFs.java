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
 * Spark SQL UDFs for arithmetic on tnumber (tint / tfloat).
 *
 * Three groups:
 *   1. Unary analytics: abs, deltaValue, angularDifference (on tnumber),
 *      angularDifference (on tpoint → tfloat).
 *   2. Scalar arithmetic: add/sub/mult/div of tnumber with a Java scalar
 *      (tint+int, tfloat+double).
 *   3. Temporal arithmetic: add/sub/mult/div of two tnumbers.
 *
 * All temporal inputs and outputs use hex-WKB string encoding.
 *
 * MEOS function authority: meos/include/meos.h (026_tnumber_mathfuncs)
 */
public final class MathUDFs {

    private MathUDFs() {}

    private static String hexOut(Pointer r) {
        if (r == null) return null;
        try {
            return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
        } finally {
            MeosMemory.free(r);
        }
    }

    // ------------------------------------------------------------------
    // Unary analytics (hex-WKB in, hex-WKB out)
    //
    // MEOS: tnumber_abs, tnumber_delta_value, tnumber_angular_difference,
    //       tpoint_angular_difference  (→ tfloat hex-WKB)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tnumberAbs =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tnumber_abs(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF1<String, String> tnumberDeltaValue =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tnumber_delta_value(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF1<String, String> tnumberAngularDifference =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tnumber_angular_difference(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF1<String, String> tpointAngularDifference =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tpoint_angular_difference(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // ------------------------------------------------------------------
    // Transcendental functions (tfloat → tfloat)
    //
    // MEOS: tfloat_exp / tfloat_ln / tfloat_log10
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tfloatExp =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_exp(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF1<String, String> tfloatLn =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_ln(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF1<String, String> tfloatLog10 =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tfloat_log10(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // ------------------------------------------------------------------
    // Scalar arithmetic: tint OP int  (hex-WKB in, int scalar, hex-WKB out)
    //
    // MEOS: add_tint_int / sub_tint_int / mult_tint_int / div_tint_int
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> addTintInt =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.add_tint_int(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF2<String, Integer, String> subTintInt =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.sub_tint_int(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF2<String, Integer, String> multTintInt =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.mul_tint_int(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF2<String, Integer, String> divTintInt =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.div_tint_int(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    // ------------------------------------------------------------------
    // Scalar arithmetic: tfloat OP double  (hex-WKB in, double scalar, hex-WKB out)
    //
    // MEOS: add_tfloat_float / sub_tfloat_float / mult_tfloat_float / div_tfloat_float
    // ------------------------------------------------------------------

    public static final UDF2<String, Double, String> addTfloatFloat =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.add_tfloat_float(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF2<String, Double, String> subTfloatFloat =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.sub_tfloat_float(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF2<String, Double, String> multTfloatFloat =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.mul_tfloat_float(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    public static final UDF2<String, Double, String> divTfloatFloat =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.div_tfloat_float(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    // ------------------------------------------------------------------
    // Temporal arithmetic: tnumber OP tnumber  (hex-WKB in, hex-WKB out)
    //
    // MEOS: add_tnumber_tnumber / sub_tnumber_tnumber /
    //       mult_tnumber_tnumber / div_tnumber_tnumber
    //
    // Both tnumbers must have the same value type (tint+tint or tfloat+tfloat).
    // ------------------------------------------------------------------

    public static final UDF2<String, String, String> addTnumberTnumber =
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

    public static final UDF2<String, String, String> subTnumberTnumber =
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

    public static final UDF2<String, String, String> multTnumberTnumber =
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

    public static final UDF2<String, String, String> divTnumberTnumber =
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
        // unary analytics
        spark.udf().register("tnumberAbs",               tnumberAbs,               DataTypes.StringType);
        spark.udf().register("tnumberDeltaValue",        tnumberDeltaValue,        DataTypes.StringType);
        spark.udf().register("tnumberAngularDifference", tnumberAngularDifference, DataTypes.StringType);
        spark.udf().register("tpointAngularDifference",  tpointAngularDifference,  DataTypes.StringType);
        // transcendental
        spark.udf().register("tfloatExp",                tfloatExp,                DataTypes.StringType);
        spark.udf().register("tfloatLn",                 tfloatLn,                 DataTypes.StringType);
        spark.udf().register("tfloatLog10",              tfloatLog10,              DataTypes.StringType);
        // tint + scalar
        spark.udf().register("addTintInt",               addTintInt,               DataTypes.StringType);
        spark.udf().register("subTintInt",               subTintInt,               DataTypes.StringType);
        spark.udf().register("multTintInt",              multTintInt,              DataTypes.StringType);
        spark.udf().register("divTintInt",               divTintInt,               DataTypes.StringType);
        // tfloat + scalar
        spark.udf().register("addTfloatFloat",           addTfloatFloat,           DataTypes.StringType);
        spark.udf().register("subTfloatFloat",           subTfloatFloat,           DataTypes.StringType);
        spark.udf().register("multTfloatFloat",          multTfloatFloat,          DataTypes.StringType);
        spark.udf().register("divTfloatFloat",           divTfloatFloat,           DataTypes.StringType);
        // tnumber + tnumber
        spark.udf().register("addTnumberTnumber",        addTnumberTnumber,        DataTypes.StringType);
        spark.udf().register("subTnumberTnumber",        subTnumberTnumber,        DataTypes.StringType);
        spark.udf().register("multTnumberTnumber",       multTnumberTnumber,       DataTypes.StringType);
        spark.udf().register("divTnumberTnumber",        divTnumberTnumber,        DataTypes.StringType);
    }
}
