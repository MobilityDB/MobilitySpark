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
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for temporal and spatial positional operators.
 *
 * Three families of operators:
 *   1. Time-direction (before/after/overbefore/overafter) on any temporal value.
 *   2. Value-direction (left/right/overleft/overright) on tnumber (tint/tfloat).
 *   3. Spatial-direction (left/right/overleft/overright/below/above/overbelow/
 *      overabove/front/back/overfront/overback) on tpoint (tgeompoint/tgeogpoint).
 *
 * All inputs are hex-WKB strings; tstzspan inputs also use hex-WKB (span_from_hexwkb).
 * All outputs are Boolean.
 *
 * MEOS function authority: meos/include/meos.h (temporal), meos/include/meos_geo.h (tpoint)
 */
public final class PosOpsUDFs {

    private PosOpsUDFs() {}

    private static Pointer tempPtr(String hex) {
        return hex == null ? null : functions.temporal_from_hexwkb(hex);
    }

    private static Pointer spanPtr(String hex) {
        return hex == null ? null : functions.span_from_hexwkb(hex);
    }

    // ------------------------------------------------------------------
    // Time-direction: temporal ↔ temporal
    // MEOS: before/after/overbefore/overafter_temporal_temporal → boolean
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> temporalBefore =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.before_temporal_temporal(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> temporalAfter =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.after_temporal_temporal(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> temporalOverbefore =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overbefore_temporal_temporal(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> temporalOverafter =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overafter_temporal_temporal(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // Time-direction: temporal ↔ tstzspan (hex-WKB span as second arg)
    // MEOS: before/after/overbefore/overafter_temporal_tstzspan → boolean
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> temporalBeforeSpan =
        (tHex, spanHex) -> {
            if (tHex == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tempPtr(tHex);
            if (p == null) return null;
            try {
                Pointer sp = spanPtr(spanHex);
                if (sp == null) return null;
                try {
                    return functions.before_temporal_tstzspan(p, sp);
                } finally { MeosMemory.free(sp); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF2<String, String, Boolean> temporalAfterSpan =
        (tHex, spanHex) -> {
            if (tHex == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tempPtr(tHex);
            if (p == null) return null;
            try {
                Pointer sp = spanPtr(spanHex);
                if (sp == null) return null;
                try {
                    return functions.after_temporal_tstzspan(p, sp);
                } finally { MeosMemory.free(sp); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF2<String, String, Boolean> temporalOverbeforeSpan =
        (tHex, spanHex) -> {
            if (tHex == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tempPtr(tHex);
            if (p == null) return null;
            try {
                Pointer sp = spanPtr(spanHex);
                if (sp == null) return null;
                try {
                    return functions.overbefore_temporal_tstzspan(p, sp);
                } finally { MeosMemory.free(sp); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF2<String, String, Boolean> temporalOverafterSpan =
        (tHex, spanHex) -> {
            if (tHex == null || spanHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = tempPtr(tHex);
            if (p == null) return null;
            try {
                Pointer sp = spanPtr(spanHex);
                if (sp == null) return null;
                try {
                    return functions.overafter_temporal_tstzspan(p, sp);
                } finally { MeosMemory.free(sp); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // Value-direction: tnumber ↔ tnumber
    // MEOS: left/right/overleft/overright_tnumber_tnumber → boolean
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tnumberLeft =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.left_tnumber_tnumber(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tnumberRight =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.right_tnumber_tnumber(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tnumberOverleft =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overleft_tnumber_tnumber(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tnumberOverright =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overright_tnumber_tnumber(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // Spatial-direction x-axis: tpoint ↔ tpoint
    // MEOS: left/right/overleft/overright_tspatial_tspatial → boolean
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tpointLeft =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.left_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tpointRight =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.right_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tpointOverleft =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overleft_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tpointOverright =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overright_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // Spatial-direction y-axis: tpoint ↔ tpoint
    // MEOS: below/above/overbelow/overabove_tspatial_tspatial → boolean
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tpointBelow =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.below_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tpointAbove =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.above_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tpointOverbelow =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overbelow_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tpointOverabove =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overabove_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // Spatial-direction z-axis (3D): tpoint ↔ tpoint
    // MEOS: front/back/overfront/overback_tspatial_tspatial → boolean
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tpointFront =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.front_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tpointBack =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.back_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tpointOverfront =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overfront_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, Boolean> tpointOverback =
        (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = tempPtr(s1);
            if (p1 == null) return null;
            try {
                Pointer p2 = tempPtr(s2);
                if (p2 == null) return null;
                try {
                    return functions.overback_tspatial_tspatial(p1, p2);
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // REGISTRATION
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        // time-direction: temporal ↔ temporal
        spark.udf().register("temporalBefore",        temporalBefore,        DataTypes.BooleanType);
        spark.udf().register("temporalAfter",         temporalAfter,         DataTypes.BooleanType);
        spark.udf().register("temporalOverbefore",    temporalOverbefore,    DataTypes.BooleanType);
        spark.udf().register("temporalOverafter",     temporalOverafter,     DataTypes.BooleanType);
        // time-direction: temporal ↔ tstzspan
        spark.udf().register("temporalBeforeSpan",    temporalBeforeSpan,    DataTypes.BooleanType);
        spark.udf().register("temporalAfterSpan",     temporalAfterSpan,     DataTypes.BooleanType);
        spark.udf().register("temporalOverbeforeSpan",temporalOverbeforeSpan,DataTypes.BooleanType);
        spark.udf().register("temporalOverafterSpan", temporalOverafterSpan, DataTypes.BooleanType);
        // value-direction: tnumber ↔ tnumber
        spark.udf().register("tnumberLeft",           tnumberLeft,           DataTypes.BooleanType);
        spark.udf().register("tnumberRight",          tnumberRight,          DataTypes.BooleanType);
        spark.udf().register("tnumberOverleft",       tnumberOverleft,       DataTypes.BooleanType);
        spark.udf().register("tnumberOverright",      tnumberOverright,      DataTypes.BooleanType);
        // spatial x-axis: tpoint ↔ tpoint
        spark.udf().register("tpointLeft",            tpointLeft,            DataTypes.BooleanType);
        spark.udf().register("tpointRight",           tpointRight,           DataTypes.BooleanType);
        spark.udf().register("tpointOverleft",        tpointOverleft,        DataTypes.BooleanType);
        spark.udf().register("tpointOverright",       tpointOverright,       DataTypes.BooleanType);
        // spatial y-axis: tpoint ↔ tpoint
        spark.udf().register("tpointBelow",           tpointBelow,           DataTypes.BooleanType);
        spark.udf().register("tpointAbove",           tpointAbove,           DataTypes.BooleanType);
        spark.udf().register("tpointOverbelow",       tpointOverbelow,       DataTypes.BooleanType);
        spark.udf().register("tpointOverabove",       tpointOverabove,       DataTypes.BooleanType);
        // spatial z-axis (3D): tpoint ↔ tpoint
        spark.udf().register("tpointFront",           tpointFront,           DataTypes.BooleanType);
        spark.udf().register("tpointBack",            tpointBack,            DataTypes.BooleanType);
        spark.udf().register("tpointOverfront",       tpointOverfront,       DataTypes.BooleanType);
        spark.udf().register("tpointOverback",        tpointOverback,        DataTypes.BooleanType);
    }
}
