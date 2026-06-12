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
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.function.BiFunction;

/**
 * Spark SQL UDFs for cross-type box-overlap predicates on temporal types.
 *
 * Coverage: 30 UDFs across 5 predicates (adjacent, contained, contains,
 * overlaps, same) × 6 cross-types:
 *   tnumber × tnumber     — value+time bounding-box compare
 *   numspan × tnumber     — value-span × tnumber bbox
 *   tnumber × numspan     — tnumber × value-span bbox
 *   tstzspan × temporal   — time-span × temporal bbox
 *   temporal × tstzspan   — temporal × time-span bbox
 *   temporal × temporal   — temporal-vs-temporal bbox
 *
 * tbox×tnumber, tnumber×tbox, tbox×tbox already covered by TBoxOpsUDFs.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class TemporalBoxOpsUDFs {

    private TemporalBoxOpsUDFs() {}

    private static UDF2<String, String, Boolean> spanTemporal(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.span_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    private static UDF2<String, String, Boolean> temporalSpan(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.span_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    private static UDF2<String, String, Boolean> temporalTemporal(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    // ------------------------------------------------------------------
    // tnumber × tnumber  (5)
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tnumberAdjacentTnumber  = temporalTemporal(GeneratedFunctions::adjacent_tnumber_tnumber);
    public static final UDF2<String, String, Boolean> tnumberContainsTnumber  = temporalTemporal(GeneratedFunctions::contains_tnumber_tnumber);
    public static final UDF2<String, String, Boolean> tnumberContainedTnumber = temporalTemporal(GeneratedFunctions::contained_tnumber_tnumber);
    public static final UDF2<String, String, Boolean> tnumberOverlapsTnumber  = temporalTemporal(GeneratedFunctions::overlaps_tnumber_tnumber);
    public static final UDF2<String, String, Boolean> tnumberSameTnumber      = temporalTemporal(GeneratedFunctions::same_tnumber_tnumber);

    // ------------------------------------------------------------------
    // numspan × tnumber  (5)
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> numspanAdjacentTnumber  = spanTemporal(GeneratedFunctions::adjacent_numspan_tnumber);
    public static final UDF2<String, String, Boolean> numspanContainsTnumber  = spanTemporal(GeneratedFunctions::contains_numspan_tnumber);
    public static final UDF2<String, String, Boolean> numspanContainedTnumber = spanTemporal(GeneratedFunctions::contained_numspan_tnumber);
    public static final UDF2<String, String, Boolean> numspanOverlapsTnumber  = spanTemporal(GeneratedFunctions::overlaps_numspan_tnumber);
    public static final UDF2<String, String, Boolean> numspanSameTnumber      = spanTemporal(GeneratedFunctions::same_numspan_tnumber);

    // ------------------------------------------------------------------
    // tnumber × numspan  (5)
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tnumberAdjacentNumspan  = temporalSpan(GeneratedFunctions::adjacent_tnumber_numspan);
    public static final UDF2<String, String, Boolean> tnumberContainsNumspan  = temporalSpan(GeneratedFunctions::contains_tnumber_numspan);
    public static final UDF2<String, String, Boolean> tnumberContainedNumspan = temporalSpan(GeneratedFunctions::contained_tnumber_numspan);
    public static final UDF2<String, String, Boolean> tnumberOverlapsNumspan  = temporalSpan(GeneratedFunctions::overlaps_tnumber_numspan);
    public static final UDF2<String, String, Boolean> tnumberSameNumspan      = temporalSpan(GeneratedFunctions::same_tnumber_numspan);

    // ------------------------------------------------------------------
    // tstzspan × temporal  (5)
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> tstzspanAdjacentTemporal  = spanTemporal(GeneratedFunctions::adjacent_tstzspan_temporal);
    public static final UDF2<String, String, Boolean> tstzspanContainsTemporal  = spanTemporal(GeneratedFunctions::contains_tstzspan_temporal);
    public static final UDF2<String, String, Boolean> tstzspanContainedTemporal = spanTemporal(GeneratedFunctions::contained_tstzspan_temporal);
    public static final UDF2<String, String, Boolean> tstzspanOverlapsTemporal  = spanTemporal(GeneratedFunctions::overlaps_tstzspan_temporal);
    public static final UDF2<String, String, Boolean> tstzspanSameTemporal      = spanTemporal(GeneratedFunctions::same_tstzspan_temporal);

    // ------------------------------------------------------------------
    // temporal × tstzspan  (5)
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> temporalAdjacentTstzspan  = temporalSpan(GeneratedFunctions::adjacent_temporal_tstzspan);
    public static final UDF2<String, String, Boolean> temporalContainsTstzspan  = temporalSpan(GeneratedFunctions::contains_temporal_tstzspan);
    public static final UDF2<String, String, Boolean> temporalContainedTstzspan = temporalSpan(GeneratedFunctions::contained_temporal_tstzspan);
    public static final UDF2<String, String, Boolean> temporalOverlapsTstzspan  = temporalSpan(GeneratedFunctions::overlaps_temporal_tstzspan);
    public static final UDF2<String, String, Boolean> temporalSameTstzspan      = temporalSpan(GeneratedFunctions::same_temporal_tstzspan);

    // ------------------------------------------------------------------
    // temporal × temporal  (5)
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> temporalAdjacentTemporal  = temporalTemporal(GeneratedFunctions::adjacent_temporal_temporal);
    public static final UDF2<String, String, Boolean> temporalContainsTemporal  = temporalTemporal(GeneratedFunctions::contains_temporal_temporal);
    public static final UDF2<String, String, Boolean> temporalContainedTemporal = temporalTemporal(GeneratedFunctions::contained_temporal_temporal);
    public static final UDF2<String, String, Boolean> temporalOverlapsTemporal  = temporalTemporal(GeneratedFunctions::overlaps_temporal_temporal);
    public static final UDF2<String, String, Boolean> temporalSameTemporal      = temporalTemporal(GeneratedFunctions::same_temporal_temporal);

    public static void registerAll(SparkSession spark) {
        // tnumber × tnumber
        spark.udf().register("tnumberAdjacentTnumber",  tnumberAdjacentTnumber,  DataTypes.BooleanType);
        spark.udf().register("tnumberContainsTnumber",  tnumberContainsTnumber,  DataTypes.BooleanType);
        spark.udf().register("tnumberContainedTnumber", tnumberContainedTnumber, DataTypes.BooleanType);
        spark.udf().register("tnumberOverlapsTnumber",  tnumberOverlapsTnumber,  DataTypes.BooleanType);
        spark.udf().register("tnumberSameTnumber",      tnumberSameTnumber,      DataTypes.BooleanType);
        // numspan × tnumber
        spark.udf().register("numspanAdjacentTnumber",  numspanAdjacentTnumber,  DataTypes.BooleanType);
        spark.udf().register("numspanContainsTnumber",  numspanContainsTnumber,  DataTypes.BooleanType);
        spark.udf().register("numspanContainedTnumber", numspanContainedTnumber, DataTypes.BooleanType);
        spark.udf().register("numspanOverlapsTnumber",  numspanOverlapsTnumber,  DataTypes.BooleanType);
        spark.udf().register("numspanSameTnumber",      numspanSameTnumber,      DataTypes.BooleanType);
        // tnumber × numspan
        spark.udf().register("tnumberAdjacentNumspan",  tnumberAdjacentNumspan,  DataTypes.BooleanType);
        spark.udf().register("tnumberContainsNumspan",  tnumberContainsNumspan,  DataTypes.BooleanType);
        spark.udf().register("tnumberContainedNumspan", tnumberContainedNumspan, DataTypes.BooleanType);
        spark.udf().register("tnumberOverlapsNumspan",  tnumberOverlapsNumspan,  DataTypes.BooleanType);
        spark.udf().register("tnumberSameNumspan",      tnumberSameNumspan,      DataTypes.BooleanType);
        // tstzspan × temporal
        spark.udf().register("tstzspanAdjacentTemporal",  tstzspanAdjacentTemporal,  DataTypes.BooleanType);
        spark.udf().register("tstzspanContainsTemporal",  tstzspanContainsTemporal,  DataTypes.BooleanType);
        spark.udf().register("tstzspanContainedTemporal", tstzspanContainedTemporal, DataTypes.BooleanType);
        spark.udf().register("tstzspanOverlapsTemporal",  tstzspanOverlapsTemporal,  DataTypes.BooleanType);
        spark.udf().register("tstzspanSameTemporal",      tstzspanSameTemporal,      DataTypes.BooleanType);
        // temporal × tstzspan
        spark.udf().register("temporalAdjacentTstzspan",  temporalAdjacentTstzspan,  DataTypes.BooleanType);
        spark.udf().register("temporalContainsTstzspan",  temporalContainsTstzspan,  DataTypes.BooleanType);
        spark.udf().register("temporalContainedTstzspan", temporalContainedTstzspan, DataTypes.BooleanType);
        spark.udf().register("temporalOverlapsTstzspan",  temporalOverlapsTstzspan,  DataTypes.BooleanType);
        spark.udf().register("temporalSameTstzspan",      temporalSameTstzspan,      DataTypes.BooleanType);
        // temporal × temporal — superseded 1:1 by the portable bare names
        // adjacent/contains/contained/overlaps/same, registered by
        // org.mobilitydb.spark.portable.PortableOperatorAliasUDFs reusing
        // these very backing fields (one bare name, all six families).
    }
}
