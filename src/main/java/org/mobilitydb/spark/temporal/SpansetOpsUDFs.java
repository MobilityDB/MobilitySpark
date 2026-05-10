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
 * Spark SQL UDFs for cross-type positional and topological predicates between
 * Span and Spanset types.
 *
 * Coverage: span×spanset, spanset×span, spanset×spanset — 8 predicates each = 24 UDFs
 * Predicates: left, right, overleft, overright, adjacent, contains, contained, overlaps
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class SpansetOpsUDFs {

    private SpansetOpsUDFs() {}

    private static UDF2<String, String, Boolean> spanSpanset(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.span_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = functions.spanset_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    private static UDF2<String, String, Boolean> spansetSpan(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.spanset_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = functions.span_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    private static UDF2<String, String, Boolean> spansetSpanset(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.spanset_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = functions.spanset_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    // ------------------------------------------------------------------
    // span × spanset
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> spanLeftSpanset       = spanSpanset(functions::left_span_spanset);
    public static final UDF2<String, String, Boolean> spanOverleftSpanset   = spanSpanset(functions::overleft_span_spanset);
    public static final UDF2<String, String, Boolean> spanRightSpanset      = spanSpanset(functions::right_span_spanset);
    public static final UDF2<String, String, Boolean> spanOverrightSpanset  = spanSpanset(functions::overright_span_spanset);
    public static final UDF2<String, String, Boolean> spanAdjacentSpanset   = spanSpanset(functions::adjacent_span_spanset);
    public static final UDF2<String, String, Boolean> spanContainsSpanset   = spanSpanset(functions::contains_span_spanset);
    public static final UDF2<String, String, Boolean> spanContainedSpanset  = spanSpanset(functions::contained_span_spanset);
    public static final UDF2<String, String, Boolean> spanOverlapsSpanset   = spanSpanset(functions::overlaps_span_spanset);

    // ------------------------------------------------------------------
    // spanset × span
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> spansetLeftSpan       = spansetSpan(functions::left_spanset_span);
    public static final UDF2<String, String, Boolean> spansetOverleftSpan   = spansetSpan(functions::overleft_spanset_span);
    public static final UDF2<String, String, Boolean> spansetRightSpan      = spansetSpan(functions::right_spanset_span);
    public static final UDF2<String, String, Boolean> spansetOverrightSpan  = spansetSpan(functions::overright_spanset_span);
    public static final UDF2<String, String, Boolean> spansetAdjacentSpan   = spansetSpan(functions::adjacent_spanset_span);
    public static final UDF2<String, String, Boolean> spansetContainedSpan  = spansetSpan(functions::contained_spanset_span);
    public static final UDF2<String, String, Boolean> spansetOverlapsSpan   = spansetSpan(functions::overlaps_spanset_span);
    // spansetContainsSpan already registered by SpanAlgebraUDFs; omitted here to avoid redundant duplicate.

    // ------------------------------------------------------------------
    // spanset × spanset
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> spansetLeftSpanset       = spansetSpanset(functions::left_spanset_spanset);
    public static final UDF2<String, String, Boolean> spansetOverleftSpanset   = spansetSpanset(functions::overleft_spanset_spanset);
    public static final UDF2<String, String, Boolean> spansetRightSpanset      = spansetSpanset(functions::right_spanset_spanset);
    public static final UDF2<String, String, Boolean> spansetOverrightSpanset  = spansetSpanset(functions::overright_spanset_spanset);
    public static final UDF2<String, String, Boolean> spansetAdjacentSpanset   = spansetSpanset(functions::adjacent_spanset_spanset);
    public static final UDF2<String, String, Boolean> spansetContainsSpanset  = spansetSpanset(functions::contains_spanset_spanset);
    public static final UDF2<String, String, Boolean> spansetContainedSpanset = spansetSpanset(functions::contained_spanset_spanset);
    // spansetOverlaps(spanset, spanset) already registered by SpanAlgebraUDFs.

    public static void registerAll(SparkSession spark) {
        // span × spanset
        spark.udf().register("spanLeftSpanset",      spanLeftSpanset,      DataTypes.BooleanType);
        spark.udf().register("spanOverleftSpanset",  spanOverleftSpanset,  DataTypes.BooleanType);
        spark.udf().register("spanRightSpanset",     spanRightSpanset,     DataTypes.BooleanType);
        spark.udf().register("spanOverrightSpanset", spanOverrightSpanset, DataTypes.BooleanType);
        spark.udf().register("spanAdjacentSpanset",  spanAdjacentSpanset,  DataTypes.BooleanType);
        spark.udf().register("spanContainsSpanset",  spanContainsSpanset,  DataTypes.BooleanType);
        spark.udf().register("spanContainedSpanset", spanContainedSpanset, DataTypes.BooleanType);
        spark.udf().register("spanOverlapsSpanset",  spanOverlapsSpanset,  DataTypes.BooleanType);

        // spanset × span
        spark.udf().register("spansetLeftSpan",      spansetLeftSpan,      DataTypes.BooleanType);
        spark.udf().register("spansetOverleftSpan",  spansetOverleftSpan,  DataTypes.BooleanType);
        spark.udf().register("spansetRightSpan",     spansetRightSpan,     DataTypes.BooleanType);
        spark.udf().register("spansetOverrightSpan", spansetOverrightSpan, DataTypes.BooleanType);
        spark.udf().register("spansetAdjacentSpan",  spansetAdjacentSpan,  DataTypes.BooleanType);
        spark.udf().register("spansetContainedSpan", spansetContainedSpan, DataTypes.BooleanType);
        spark.udf().register("spansetOverlapsSpan",  spansetOverlapsSpan,  DataTypes.BooleanType);

        // spanset × spanset
        spark.udf().register("spansetLeftSpanset",       spansetLeftSpanset,       DataTypes.BooleanType);
        spark.udf().register("spansetOverleftSpanset",   spansetOverleftSpanset,   DataTypes.BooleanType);
        spark.udf().register("spansetRightSpanset",      spansetRightSpanset,      DataTypes.BooleanType);
        spark.udf().register("spansetOverrightSpanset",  spansetOverrightSpanset,  DataTypes.BooleanType);
        spark.udf().register("spansetAdjacentSpanset",   spansetAdjacentSpanset,   DataTypes.BooleanType);
        spark.udf().register("spansetContainsSpanset",   spansetContainsSpanset,   DataTypes.BooleanType);
        spark.udf().register("spansetContainedSpanset",  spansetContainedSpanset,  DataTypes.BooleanType);
    }
}
