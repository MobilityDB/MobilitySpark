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

import java.util.function.BiFunction;

/**
 * Spark SQL UDFs for cross-type positional, temporal, and topological
 * predicates between TBox and TNumber types.
 *
 * Coverage: tbox×tbox, tbox×tnumber, tnumber×tbox — 13 predicates each = 39 UDFs
 * Predicates: left, right, overleft, overright (X axis);
 *             before, after, overbefore, overafter (time axis);
 *             adjacent, contains, contained, overlaps, same (topological)
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class TBoxOpsUDFs {

    private TBoxOpsUDFs() {}

    // ------------------------------------------------------------------
    // Helpers — reduce 39× boilerplate to one factory per cross-type
    // ------------------------------------------------------------------

    private static UDF2<String, String, Boolean> tboxTbox(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.tbox_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = functions.tbox_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    private static UDF2<String, String, Boolean> tboxTnumber(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.tbox_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = functions.temporal_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    private static UDF2<String, String, Boolean> tnumberTbox(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = functions.tbox_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    // ------------------------------------------------------------------
    // tbox × tbox
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tboxLeftTbox       = tboxTbox(functions::left_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverleftTbox   = tboxTbox(functions::overleft_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxRightTbox      = tboxTbox(functions::right_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverrightTbox  = tboxTbox(functions::overright_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxBeforeTbox     = tboxTbox(functions::before_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverbeforeTbox = tboxTbox(functions::overbefore_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxAfterTbox      = tboxTbox(functions::after_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverafterTbox  = tboxTbox(functions::overafter_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxAdjacentTbox   = tboxTbox(functions::adjacent_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxContainsTbox   = tboxTbox(functions::contains_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxContainedTbox  = tboxTbox(functions::contained_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxOverlapsTbox   = tboxTbox(functions::overlaps_tbox_tbox);
    public static final UDF2<String, String, Boolean> tboxSameTbox       = tboxTbox(functions::same_tbox_tbox);

    // ------------------------------------------------------------------
    // tbox × tnumber
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tboxLeftTnumber       = tboxTnumber(functions::left_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxOverleftTnumber   = tboxTnumber(functions::overleft_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxRightTnumber      = tboxTnumber(functions::right_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxOverrightTnumber  = tboxTnumber(functions::overright_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxBeforeTnumber     = tboxTnumber(functions::before_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxOverbeforeTnumber = tboxTnumber(functions::overbefore_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxAfterTnumber      = tboxTnumber(functions::after_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxOverafterTnumber  = tboxTnumber(functions::overafter_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxAdjacentTnumber   = tboxTnumber(functions::adjacent_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxContainsTnumber   = tboxTnumber(functions::contains_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxContainedTnumber  = tboxTnumber(functions::contained_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxOverlapsTnumber   = tboxTnumber(functions::overlaps_tbox_tnumber);
    public static final UDF2<String, String, Boolean> tboxSameTnumber       = tboxTnumber(functions::same_tbox_tnumber);

    // ------------------------------------------------------------------
    // tnumber × tbox
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tnumberLeftTbox       = tnumberTbox(functions::left_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberOverleftTbox   = tnumberTbox(functions::overleft_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberRightTbox      = tnumberTbox(functions::right_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberOverrightTbox  = tnumberTbox(functions::overright_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberBeforeTbox     = tnumberTbox(functions::before_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberOverbeforeTbox = tnumberTbox(functions::overbefore_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberAfterTbox      = tnumberTbox(functions::after_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberOverafterTbox  = tnumberTbox(functions::overafter_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberAdjacentTbox   = tnumberTbox(functions::adjacent_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberContainsTbox   = tnumberTbox(functions::contains_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberContainedTbox  = tnumberTbox(functions::contained_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberOverlapsTbox   = tnumberTbox(functions::overlaps_tnumber_tbox);
    public static final UDF2<String, String, Boolean> tnumberSameTbox       = tnumberTbox(functions::same_tnumber_tbox);

    public static void registerAll(SparkSession spark) {
        // tbox × tbox
        spark.udf().register("tboxLeftTbox",       tboxLeftTbox,       DataTypes.BooleanType);
        spark.udf().register("tboxOverleftTbox",   tboxOverleftTbox,   DataTypes.BooleanType);
        spark.udf().register("tboxRightTbox",      tboxRightTbox,      DataTypes.BooleanType);
        spark.udf().register("tboxOverrightTbox",  tboxOverrightTbox,  DataTypes.BooleanType);
        spark.udf().register("tboxBeforeTbox",     tboxBeforeTbox,     DataTypes.BooleanType);
        spark.udf().register("tboxOverbeforeTbox", tboxOverbeforeTbox, DataTypes.BooleanType);
        spark.udf().register("tboxAfterTbox",      tboxAfterTbox,      DataTypes.BooleanType);
        spark.udf().register("tboxOverafterTbox",  tboxOverafterTbox,  DataTypes.BooleanType);
        spark.udf().register("tboxAdjacentTbox",   tboxAdjacentTbox,   DataTypes.BooleanType);
        spark.udf().register("tboxContainsTbox",   tboxContainsTbox,   DataTypes.BooleanType);
        spark.udf().register("tboxContainedTbox",  tboxContainedTbox,  DataTypes.BooleanType);
        spark.udf().register("tboxOverlapsTbox",   tboxOverlapsTbox,   DataTypes.BooleanType);
        spark.udf().register("tboxSameTbox",       tboxSameTbox,       DataTypes.BooleanType);

        // tbox × tnumber
        spark.udf().register("tboxLeftTnumber",       tboxLeftTnumber,       DataTypes.BooleanType);
        spark.udf().register("tboxOverleftTnumber",   tboxOverleftTnumber,   DataTypes.BooleanType);
        spark.udf().register("tboxRightTnumber",      tboxRightTnumber,      DataTypes.BooleanType);
        spark.udf().register("tboxOverrightTnumber",  tboxOverrightTnumber,  DataTypes.BooleanType);
        spark.udf().register("tboxBeforeTnumber",     tboxBeforeTnumber,     DataTypes.BooleanType);
        spark.udf().register("tboxOverbeforeTnumber", tboxOverbeforeTnumber, DataTypes.BooleanType);
        spark.udf().register("tboxAfterTnumber",      tboxAfterTnumber,      DataTypes.BooleanType);
        spark.udf().register("tboxOverafterTnumber",  tboxOverafterTnumber,  DataTypes.BooleanType);
        spark.udf().register("tboxAdjacentTnumber",   tboxAdjacentTnumber,   DataTypes.BooleanType);
        spark.udf().register("tboxContainsTnumber",   tboxContainsTnumber,   DataTypes.BooleanType);
        spark.udf().register("tboxContainedTnumber",  tboxContainedTnumber,  DataTypes.BooleanType);
        spark.udf().register("tboxOverlapsTnumber",   tboxOverlapsTnumber,   DataTypes.BooleanType);
        spark.udf().register("tboxSameTnumber",       tboxSameTnumber,       DataTypes.BooleanType);

        // tnumber × tbox
        spark.udf().register("tnumberLeftTbox",       tnumberLeftTbox,       DataTypes.BooleanType);
        spark.udf().register("tnumberOverleftTbox",   tnumberOverleftTbox,   DataTypes.BooleanType);
        spark.udf().register("tnumberRightTbox",      tnumberRightTbox,      DataTypes.BooleanType);
        spark.udf().register("tnumberOverrightTbox",  tnumberOverrightTbox,  DataTypes.BooleanType);
        spark.udf().register("tnumberBeforeTbox",     tnumberBeforeTbox,     DataTypes.BooleanType);
        spark.udf().register("tnumberOverbeforeTbox", tnumberOverbeforeTbox, DataTypes.BooleanType);
        spark.udf().register("tnumberAfterTbox",      tnumberAfterTbox,      DataTypes.BooleanType);
        spark.udf().register("tnumberOverafterTbox",  tnumberOverafterTbox,  DataTypes.BooleanType);
        spark.udf().register("tnumberAdjacentTbox",   tnumberAdjacentTbox,   DataTypes.BooleanType);
        spark.udf().register("tnumberContainsTbox",   tnumberContainsTbox,   DataTypes.BooleanType);
        spark.udf().register("tnumberContainedTbox",  tnumberContainedTbox,  DataTypes.BooleanType);
        spark.udf().register("tnumberOverlapsTbox",   tnumberOverlapsTbox,   DataTypes.BooleanType);
        spark.udf().register("tnumberSameTbox",       tnumberSameTbox,       DataTypes.BooleanType);
    }
}
