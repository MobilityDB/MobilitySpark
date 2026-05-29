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
 * Spark SQL UDFs for temporal boolean AND/OR operations on tbool.
 *
 * Spark SQL cannot register two UDFs with the same name but different
 * argument types, so the three arities of tand/tor are registered with
 * type-qualified names:
 *
 *   tandBool(tbool, bool)      → tand_tbool_bool
 *   tandBoolTbool(bool, tbool) → tand_bool_tbool
 *   tandTboolTbool(tbool,tbool)→ tand_tbool_tbool
 *
 * (Likewise for tor.)
 *
 * All temporal inputs and outputs use hex-WKB string encoding.
 *
 * MEOS function authority: meos/include/meos.h (028_tbool_boolops)
 */
public final class BoolOpsUDFs {

    private BoolOpsUDFs() {}

    private static String hexOut(Pointer r) {
        if (r == null) return null;
        try {
            return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
        } finally {
            MeosMemory.free(r);
        }
    }

    // ------------------------------------------------------------------
    // tnot: temporal NOT
    // MEOS: tnot_tbool(const Temporal *) → Temporal *
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tnotTbool =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tnot_tbool(ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // ------------------------------------------------------------------
    // tand: temporal AND
    // MEOS: tand_tbool_bool / tand_bool_tbool / tand_tbool_tbool
    // ------------------------------------------------------------------

    // tandBool(tbool_hex, bool) → tbool hex-WKB
    public static final UDF2<String, Boolean, String> tandBool =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tand_tbool_bool(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    // tandBoolTbool(bool, tbool_hex) → tbool hex-WKB
    public static final UDF2<Boolean, String, String> tandBoolTbool =
        (v, s) -> {
            if (v == null || s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tand_bool_tbool(v, ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // tandTboolTbool(tbool1_hex, tbool2_hex) → tbool hex-WKB
    public static final UDF2<String, String, String> tandTboolTbool =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    return hexOut(GeneratedFunctions.tand_tbool_tbool(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // tor: temporal OR
    // MEOS: tor_tbool_bool / tor_bool_tbool / tor_tbool_tbool
    // ------------------------------------------------------------------

    // torBool(tbool_hex, bool) → tbool hex-WKB
    public static final UDF2<String, Boolean, String> torBool =
        (s, v) -> {
            if (s == null || v == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tor_tbool_bool(ptr, v));
            } finally { MeosMemory.free(ptr); }
        };

    // torBoolTbool(bool, tbool_hex) → tbool hex-WKB
    public static final UDF2<Boolean, String, String> torBoolTbool =
        (v, s) -> {
            if (v == null || s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                return hexOut(GeneratedFunctions.tor_bool_tbool(v, ptr));
            } finally { MeosMemory.free(ptr); }
        };

    // torTboolTbool(tbool1_hex, tbool2_hex) → tbool hex-WKB
    public static final UDF2<String, String, String> torTboolTbool =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    return hexOut(GeneratedFunctions.tor_tbool_tbool(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // Temporal boolean accessor
    // ------------------------------------------------------------------

    // tboolWhenTrue(tbool_hex) → tstzspanset hex-WKB
    // Returns the periods when the tbool is true.
    // MEOS: tbool_when_true(const Temporal *) → SpanSet *
    public static final UDF1<String, String> tboolWhenTrue =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(s);
            if (ptr == null) return null;
            try {
                Pointer r = GeneratedFunctions.tbool_when_true(ptr);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.spanset_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Temporal comparison operators  (temporal × temporal → tbool hex-WKB)
    //
    // MEOS: teq/tne/tlt/tle/tgt/tge_temporal_temporal  meos.h
    // ------------------------------------------------------------------

    public static final UDF2<String, String, String> teqTemporalTemporal =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    Pointer r = GeneratedFunctions.teq_temporal_temporal(p1, p2);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    public static final UDF2<String, String, String> tneTemporalTemporal =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    Pointer r = GeneratedFunctions.tne_temporal_temporal(p1, p2);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    public static final UDF2<String, String, String> tltTemporalTemporal =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    Pointer r = GeneratedFunctions.tlt_temporal_temporal(p1, p2);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    public static final UDF2<String, String, String> tleTemporalTemporal =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    Pointer r = GeneratedFunctions.tle_temporal_temporal(p1, p2);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    public static final UDF2<String, String, String> tgtTemporalTemporal =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    Pointer r = GeneratedFunctions.tgt_temporal_temporal(p1, p2);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    public static final UDF2<String, String, String> tgeTemporalTemporal =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(s2);
                if (p2 == null) return null;
                try {
                    Pointer r = GeneratedFunctions.tge_temporal_temporal(p1, p2);
                    if (r == null) return null;
                    try {
                        return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    // ------------------------------------------------------------------
    // REGISTRATION
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        // tnot
        spark.udf().register("tnotTbool",            tnotTbool,            DataTypes.StringType);
        // tand
        spark.udf().register("tandBool",             tandBool,             DataTypes.StringType);
        spark.udf().register("tandBoolTbool",        tandBoolTbool,        DataTypes.StringType);
        spark.udf().register("tandTboolTbool",       tandTboolTbool,       DataTypes.StringType);
        // tor
        spark.udf().register("torBool",              torBool,              DataTypes.StringType);
        spark.udf().register("torBoolTbool",         torBoolTbool,         DataTypes.StringType);
        spark.udf().register("torTboolTbool",        torTboolTbool,        DataTypes.StringType);
        // tbool accessor
        spark.udf().register("tboolWhenTrue",        tboolWhenTrue,        DataTypes.StringType);
        // temporal comparison operators
        spark.udf().register("teqTemporalTemporal",  teqTemporalTemporal,  DataTypes.StringType);
        spark.udf().register("tneTemporalTemporal",  tneTemporalTemporal,  DataTypes.StringType);
        spark.udf().register("tltTemporalTemporal",  tltTemporalTemporal,  DataTypes.StringType);
        spark.udf().register("tleTemporalTemporal",  tleTemporalTemporal,  DataTypes.StringType);
        spark.udf().register("tgtTemporalTemporal",  tgtTemporalTemporal,  DataTypes.StringType);
        spark.udf().register("tgeTemporalTemporal",  tgeTemporalTemporal,  DataTypes.StringType);

        // MobilityDB SQL bare-name aliases for tbool boolops
        spark.udf().register("tboolNot", tnotTbool,        DataTypes.StringType);
        spark.udf().register("tboolAnd", tandTboolTbool,   DataTypes.StringType);
        spark.udf().register("tboolOr",  torTboolTbool,    DataTypes.StringType);
    }
}
