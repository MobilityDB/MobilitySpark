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

package org.mobilitydb.spark.geo;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosNative;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for cross-type positional and topological predicates between
 * STBox and tgeopoint (tspatial).
 *
 * MEOS function authority: meos/include/meos_geo.h
 *
 * Naming: stbox = hex-WKB STBox, tpoint = hex-WKB tgeopoint.
 * All predicates return Boolean (null if either input is null or invalid).
 */
public final class TPointSTBoxOpsUDFs {

    private TPointSTBoxOpsUDFs() {}

    // ------------------------------------------------------------------
    // Helper: deserialize STBox from hex-WKB, check null
    // ------------------------------------------------------------------

    private static Pointer stboxPtr(String hex) {
        return hex == null ? null : GeneratedFunctions.stbox_from_hexwkb(hex);
    }

    private static Pointer tpointPtr(String hex) {
        return hex == null ? null : GeneratedFunctions.temporal_from_hexwkb(hex);
    }

    // ------------------------------------------------------------------
    // STBox × TPoint — spatial direction
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> stboxLeftTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.left_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxOverleftTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.overleft_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxRightTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.right_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxOverrightTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.overright_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxBelowTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.below_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxOverbelowTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.overbelow_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxAboveTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.above_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxOveraboveTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.overabove_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxFrontTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.front_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxOverfrontTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.overfront_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxBackTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.back_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxOverbackTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.overback_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    // ------------------------------------------------------------------
    // STBox × TPoint — temporal direction
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> stboxBeforeTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.before_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxOverbeforeTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.overbefore_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxAfterTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.after_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxOverafterTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.overafter_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    // ------------------------------------------------------------------
    // STBox × TPoint — topological
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> stboxAdjacentTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.adjacent_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxContainsTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.contains_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxContainedTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.contained_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxOverlapsTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.overlaps_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, Boolean> stboxSameTpoint =
        (sb, tp) -> {
            MeosThread.ensureReady();
            Pointer s = stboxPtr(sb); if (s == null) return null;
            Pointer t = tpointPtr(tp); if (t == null) { MeosMemory.free(s); return null; }
            try { return MeosNative.INSTANCE.same_stbox_tspatial(s, t); }
            finally { MeosMemory.free(s, t); }
        };

    // ------------------------------------------------------------------
    // TPoint × STBox — spatial direction
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tpointLeftStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.left_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointOverleftStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.overleft_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointRightStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.right_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointOverrightStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.overright_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointBelowStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.below_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointOverbelowStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.overbelow_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointAboveStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.above_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointOveraboveStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.overabove_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointFrontStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.front_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointOverfrontStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.overfront_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointBackStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.back_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointOverbackStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.overback_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    // ------------------------------------------------------------------
    // TPoint × STBox — temporal direction
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tpointBeforeStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.before_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointOverbeforeStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.overbefore_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointAfterStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.after_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointOverafterStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.overafter_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    // ------------------------------------------------------------------
    // TPoint × STBox — topological
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tpointAdjacentStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.adjacent_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointContainsStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.contains_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointContainedStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.contained_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointOverlapsStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.overlaps_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, Boolean> tpointSameStbox =
        (tp, sb) -> {
            MeosThread.ensureReady();
            Pointer t = tpointPtr(tp); if (t == null) return null;
            Pointer s = stboxPtr(sb); if (s == null) { MeosMemory.free(t); return null; }
            try { return MeosNative.INSTANCE.same_tspatial_stbox(t, s); }
            finally { MeosMemory.free(t, s); }
        };

    // ------------------------------------------------------------------
    // Register all
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        // STBox × TPoint — spatial direction
        spark.udf().register("stboxLeftTpoint",       stboxLeftTpoint,       DataTypes.BooleanType);
        spark.udf().register("stboxOverleftTpoint",   stboxOverleftTpoint,   DataTypes.BooleanType);
        spark.udf().register("stboxRightTpoint",      stboxRightTpoint,      DataTypes.BooleanType);
        spark.udf().register("stboxOverrightTpoint",  stboxOverrightTpoint,  DataTypes.BooleanType);
        spark.udf().register("stboxBelowTpoint",      stboxBelowTpoint,      DataTypes.BooleanType);
        spark.udf().register("stboxOverbelowTpoint",  stboxOverbelowTpoint,  DataTypes.BooleanType);
        spark.udf().register("stboxAboveTpoint",      stboxAboveTpoint,      DataTypes.BooleanType);
        spark.udf().register("stboxOveraboveTpoint",  stboxOveraboveTpoint,  DataTypes.BooleanType);
        spark.udf().register("stboxFrontTpoint",      stboxFrontTpoint,      DataTypes.BooleanType);
        spark.udf().register("stboxOverfrontTpoint",  stboxOverfrontTpoint,  DataTypes.BooleanType);
        spark.udf().register("stboxBackTpoint",       stboxBackTpoint,       DataTypes.BooleanType);
        spark.udf().register("stboxOverbackTpoint",   stboxOverbackTpoint,   DataTypes.BooleanType);
        // STBox × TPoint — temporal direction
        spark.udf().register("stboxBeforeTpoint",     stboxBeforeTpoint,     DataTypes.BooleanType);
        spark.udf().register("stboxOverbeforeTpoint", stboxOverbeforeTpoint, DataTypes.BooleanType);
        spark.udf().register("stboxAfterTpoint",      stboxAfterTpoint,      DataTypes.BooleanType);
        spark.udf().register("stboxOverafterTpoint",  stboxOverafterTpoint,  DataTypes.BooleanType);
        // STBox × TPoint — topological
        spark.udf().register("stboxAdjacentTpoint",   stboxAdjacentTpoint,   DataTypes.BooleanType);
        spark.udf().register("stboxContainsTpoint",   stboxContainsTpoint,   DataTypes.BooleanType);
        spark.udf().register("stboxContainedTpoint",  stboxContainedTpoint,  DataTypes.BooleanType);
        spark.udf().register("stboxOverlapsTpoint",   stboxOverlapsTpoint,   DataTypes.BooleanType);
        spark.udf().register("stboxSameTpoint",       stboxSameTpoint,       DataTypes.BooleanType);

        // TPoint × STBox — spatial direction
        spark.udf().register("tpointLeftStbox",       tpointLeftStbox,       DataTypes.BooleanType);
        spark.udf().register("tpointOverleftStbox",   tpointOverleftStbox,   DataTypes.BooleanType);
        spark.udf().register("tpointRightStbox",      tpointRightStbox,      DataTypes.BooleanType);
        spark.udf().register("tpointOverrightStbox",  tpointOverrightStbox,  DataTypes.BooleanType);
        spark.udf().register("tpointBelowStbox",      tpointBelowStbox,      DataTypes.BooleanType);
        spark.udf().register("tpointOverbelowStbox",  tpointOverbelowStbox,  DataTypes.BooleanType);
        spark.udf().register("tpointAboveStbox",      tpointAboveStbox,      DataTypes.BooleanType);
        spark.udf().register("tpointOveraboveStbox",  tpointOveraboveStbox,  DataTypes.BooleanType);
        spark.udf().register("tpointFrontStbox",      tpointFrontStbox,      DataTypes.BooleanType);
        spark.udf().register("tpointOverfrontStbox",  tpointOverfrontStbox,  DataTypes.BooleanType);
        spark.udf().register("tpointBackStbox",       tpointBackStbox,       DataTypes.BooleanType);
        spark.udf().register("tpointOverbackStbox",   tpointOverbackStbox,   DataTypes.BooleanType);
        // TPoint × STBox — temporal direction
        spark.udf().register("tpointBeforeStbox",     tpointBeforeStbox,     DataTypes.BooleanType);
        spark.udf().register("tpointOverbeforeStbox", tpointOverbeforeStbox, DataTypes.BooleanType);
        spark.udf().register("tpointAfterStbox",      tpointAfterStbox,      DataTypes.BooleanType);
        spark.udf().register("tpointOverafterStbox",  tpointOverafterStbox,  DataTypes.BooleanType);
        // TPoint × STBox — topological
        spark.udf().register("tpointAdjacentStbox",   tpointAdjacentStbox,   DataTypes.BooleanType);
        spark.udf().register("tpointContainsStbox",   tpointContainsStbox,   DataTypes.BooleanType);
        spark.udf().register("tpointContainedStbox",  tpointContainedStbox,  DataTypes.BooleanType);
        spark.udf().register("tpointOverlapsStbox",   tpointOverlapsStbox,   DataTypes.BooleanType);
        spark.udf().register("tpointSameStbox",       tpointSameStbox,       DataTypes.BooleanType);
    }
}
