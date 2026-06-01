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
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for set / span / spanset algebra and tbox/stbox box algebra.
 *
 * Storage convention: set, span, spanset, tbox and stbox values are all stored
 * as hex-WKB strings. Each type round-trips through its matching
 * {@code <type>_from_hexwkb} / {@code <type>_as_hexwkb} pair. (tbox_as_hexwkb and
 * stbox_as_hexwkb require a non-null size_out scratch Pointer.)
 *
 * Only the operand-symmetric (set/set, span/span, spanset/spanset, box/box)
 * overloads are exposed here; the value-operand overloads (e.g.
 * union_value_set, union_span_value) take a base-type Datum pointer that has no
 * portable hex representation from Spark and are therefore not implemented.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
public final class SetSpanGapUDFs {

    private SetSpanGapUDFs() {}

    // ------------------------------------------------------------------
    // hex <-> pointer helpers
    // ------------------------------------------------------------------

    private static String spanHex(Pointer p) {
        if (p == null) return null;
        return GeneratedFunctions.span_as_hexwkb(p, (byte) 0);
    }

    private static String spansetHex(Pointer p) {
        if (p == null) return null;
        return GeneratedFunctions.spanset_as_hexwkb(p, (byte) 0);
    }

    private static String setHex(Pointer p) {
        if (p == null) return null;
        return GeneratedFunctions.set_as_hexwkb(p, (byte) 0);
    }

    private static String tboxHex(Pointer p) {
        if (p == null) return null;
        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        return GeneratedFunctions.tbox_as_hexwkb(p, (byte) 0, sizeOut);
    }

    private static String stboxHex(Pointer p) {
        if (p == null) return null;
        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        return GeneratedFunctions.stbox_as_hexwkb(p, (byte) 0, sizeOut);
    }

    // ------------------------------------------------------------------
    // span_union / span_minus / span_intersection
    //   span/span        -> span (intersection may yield NULL)
    //   spanset/spanset   -> spanset
    // The result element type (span vs spanset) is deduced from the inputs:
    // we serialise a span result with span_as_hexwkb and a spanset result with
    // spanset_as_hexwkb. Distinct UDF names per operand kind keep this explicit.
    // ------------------------------------------------------------------

    // span_union(span, span) -> span
    public static final UDF2<String, String, String> spanUnion =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.span_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.span_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            Pointer r = GeneratedFunctions.super_union_span_span(s1, s2, false);
            if (r == null) return null;
            try { return spanHex(r); } finally { MeosMemory.free(r); }
        };

    // span_minus(span, span) -> spanset (a minus b may split into 2 spans)
    public static final UDF2<String, String, String> spanMinus =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.span_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.span_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            Pointer r = GeneratedFunctions.minus_span_span(s1, s2);
            if (r == null) return null;
            try { return spansetHex(r); } finally { MeosMemory.free(r); }
        };

    // span_intersection(span, span) -> span (NULL when disjoint)
    public static final UDF2<String, String, String> spanIntersection =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.span_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.span_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            Pointer r = GeneratedFunctions.intersection_span_span(s1, s2);
            if (r == null) return null;
            try { return spanHex(r); } finally { MeosMemory.free(r); }
        };

    // spanset_union(spanset, spanset) -> spanset
    public static final UDF2<String, String, String> spansetUnion =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.spanset_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.spanset_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            Pointer r = GeneratedFunctions.union_spanset_spanset(s1, s2);
            if (r == null) return null;
            try { return spansetHex(r); } finally { MeosMemory.free(r); }
        };

    // spanset_minus(spanset, spanset) -> spanset
    public static final UDF2<String, String, String> spansetMinus =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.spanset_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.spanset_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            Pointer r = GeneratedFunctions.minus_spanset_spanset(s1, s2);
            if (r == null) return null;
            try { return spansetHex(r); } finally { MeosMemory.free(r); }
        };

    // spanset_intersection(spanset, spanset) -> spanset (NULL when disjoint)
    public static final UDF2<String, String, String> spansetIntersection =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.spanset_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.spanset_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            Pointer r = GeneratedFunctions.intersection_spanset_spanset(s1, s2);
            if (r == null) return null;
            try { return spansetHex(r); } finally { MeosMemory.free(r); }
        };

    // ------------------------------------------------------------------
    // span / spanset accessors
    // ------------------------------------------------------------------

    // spann(spanset, n) -> span  (1-based; view, do NOT free the result)
    public static final UDF2<String, Integer, String> spann =
        (hex, n) -> {
            if (hex == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer ss = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (ss == null) return null;
            try {
                Pointer span = GeneratedFunctions.spanset_span_n(ss, n);
                return spanHex(span);
            } finally { MeosMemory.free(ss); }
        };

    // startSpan(spanset) -> span
    public static final UDF1<String, String> startSpan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer ss = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (ss == null) return null;
            try {
                Pointer span = GeneratedFunctions.spanset_start_span(ss);
                if (span == null) return null;
                try { return spanHex(span); } finally { MeosMemory.free(span); }
            } finally { MeosMemory.free(ss); }
        };

    // endSpan(spanset) -> span
    public static final UDF1<String, String> endSpan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer ss = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (ss == null) return null;
            try {
                Pointer span = GeneratedFunctions.spanset_end_span(ss);
                if (span == null) return null;
                try { return spanHex(span); } finally { MeosMemory.free(span); }
            } finally { MeosMemory.free(ss); }
        };

    // numSpans(spanset) -> integer
    public static final UDF1<String, Integer> numSpans =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer ss = GeneratedFunctions.spanset_from_hexwkb(hex);
            if (ss == null) return null;
            try { return GeneratedFunctions.spanset_num_spans(ss); }
            finally { MeosMemory.free(ss); }
        };

    // ------------------------------------------------------------------
    // set_union / set_minus / set_intersection  (set/set -> set)
    // ------------------------------------------------------------------

    public static final UDF2<String, String, String> setUnion =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.set_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.set_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            Pointer r = GeneratedFunctions.union_set_set(s1, s2);
            if (r == null) return null;
            try { return setHex(r); } finally { MeosMemory.free(r); }
        };

    public static final UDF2<String, String, String> setMinus =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.set_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.set_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            Pointer r = GeneratedFunctions.minus_set_set(s1, s2);
            if (r == null) return null;
            try { return setHex(r); } finally { MeosMemory.free(r); }
        };

    public static final UDF2<String, String, String> setIntersection =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.set_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.set_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            Pointer r = GeneratedFunctions.intersection_set_set(s1, s2);
            if (r == null) return null;
            try { return setHex(r); } finally { MeosMemory.free(r); }
        };

    // ------------------------------------------------------------------
    // tbox / stbox box algebra
    // ------------------------------------------------------------------

    // tbox_union(tbox, tbox) -> tbox  (strict=false: always returns a box)
    public static final UDF2<String, String, String> tboxUnion =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer b1 = GeneratedFunctions.tbox_from_hexwkb(a);
            Pointer b2 = GeneratedFunctions.tbox_from_hexwkb(b);
            if (b1 == null || b2 == null) return null;
            Pointer r = GeneratedFunctions.union_tbox_tbox(b1, b2, false);
            if (r == null) return null;
            try { return tboxHex(r); } finally { MeosMemory.free(r); }
        };

    // tbox_intersection(tbox, tbox) -> tbox (NULL when disjoint)
    public static final UDF2<String, String, String> tboxIntersection =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer b1 = GeneratedFunctions.tbox_from_hexwkb(a);
            Pointer b2 = GeneratedFunctions.tbox_from_hexwkb(b);
            if (b1 == null || b2 == null) return null;
            Pointer r = GeneratedFunctions.intersection_tbox_tbox(b1, b2);
            if (r == null) return null;
            try { return tboxHex(r); } finally { MeosMemory.free(r); }
        };

    // stbox_union(stbox, stbox) -> stbox  (strict=false)
    public static final UDF2<String, String, String> stboxUnion =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer b1 = GeneratedFunctions.stbox_from_hexwkb(a);
            Pointer b2 = GeneratedFunctions.stbox_from_hexwkb(b);
            if (b1 == null || b2 == null) return null;
            Pointer r = GeneratedFunctions.union_stbox_stbox(b1, b2, false);
            if (r == null) return null;
            try { return stboxHex(r); } finally { MeosMemory.free(r); }
        };

    // stbox_intersection(stbox, stbox) -> stbox (NULL when disjoint)
    public static final UDF2<String, String, String> stboxIntersection =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer b1 = GeneratedFunctions.stbox_from_hexwkb(a);
            Pointer b2 = GeneratedFunctions.stbox_from_hexwkb(b);
            if (b1 == null || b2 == null) return null;
            Pointer r = GeneratedFunctions.intersection_stbox_stbox(b1, b2);
            if (r == null) return null;
            try { return stboxHex(r); } finally { MeosMemory.free(r); }
        };

    // bigintspan(tbox) -> bigintspan
    public static final UDF1<String, String> bigintSpan =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer box = GeneratedFunctions.tbox_from_hexwkb(hex);
            if (box == null) return null;
            try {
                Pointer span = GeneratedFunctions.tbox_to_bigintspan(box);
                if (span == null) return null;
                try { return spanHex(span); } finally { MeosMemory.free(span); }
            } finally { MeosMemory.free(box); }
        };

    // ------------------------------------------------------------------
    // tnumber x tbox restriction
    //   atTbox(tnumber, tbox)    -> tnumber
    //   minusTbox(tnumber, tbox) -> tnumber
    // tnumber values round-trip through temporal hex-WKB.
    // ------------------------------------------------------------------

    // atTbox(tnumber, tbox) -> tnumber
    public static final UDF2<String, String, String> atTbox =
        (tempHex, boxHex) -> {
            if (tempHex == null || boxHex == null) return null;
            MeosThread.ensureReady();
            Pointer temp = GeneratedFunctions.temporal_from_hexwkb(tempHex);
            Pointer box = GeneratedFunctions.tbox_from_hexwkb(boxHex);
            if (temp == null || box == null) return null;
            try {
                Pointer r = GeneratedFunctions.tnumber_at_tbox(temp, box);
                if (r == null) return null;
                try { return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(temp); }
        };

    // minusTbox(tnumber, tbox) -> tnumber
    public static final UDF2<String, String, String> minusTbox =
        (tempHex, boxHex) -> {
            if (tempHex == null || boxHex == null) return null;
            MeosThread.ensureReady();
            Pointer temp = GeneratedFunctions.temporal_from_hexwkb(tempHex);
            Pointer box = GeneratedFunctions.tbox_from_hexwkb(boxHex);
            if (temp == null || box == null) return null;
            try {
                Pointer r = GeneratedFunctions.tnumber_minus_tbox(temp, box);
                if (r == null) return null;
                try { return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(temp); }
        };

    // ------------------------------------------------------------------
    // Distance (float-typed operands only)
    //   spanDistance(floatspan, floatspan)        -> double
    //   spanDistance(floatspanset, floatspanset)  -> double
    //   setDistance(floatset, floatset)           -> double
    //   timeDistance(tstzspan, tstzspan)          -> double (seconds)
    //   timeDistance(tstzspanset, tstzspanset)    -> double (seconds)
    // The generic distance_*_* backings return an int Datum whose meaning
    // depends on the base type, so the float-typed wrappers are used to keep
    // results exact for the common numeric/time cases.
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Double> spanDistance =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.span_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.span_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            return GeneratedFunctions.distance_floatspan_floatspan(s1, s2);
        };

    public static final UDF2<String, String, Double> spansetDistance =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.spanset_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.spanset_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            return GeneratedFunctions.distance_floatspanset_floatspanset(s1, s2);
        };

    public static final UDF2<String, String, Double> setDistance =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.set_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.set_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            return GeneratedFunctions.distance_floatset_floatset(s1, s2);
        };

    public static final UDF2<String, String, Double> timeDistanceSpan =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.span_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.span_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            return GeneratedFunctions.distance_tstzspan_tstzspan(s1, s2);
        };

    public static final UDF2<String, String, Double> timeDistanceSpanset =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer s1 = GeneratedFunctions.spanset_from_hexwkb(a);
            Pointer s2 = GeneratedFunctions.spanset_from_hexwkb(b);
            if (s1 == null || s2 == null) return null;
            return GeneratedFunctions.distance_tstzspanset_tstzspanset(s1, s2);
        };

    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        // span / spanset set algebra
        spark.udf().register("span_union",          spanUnion,          DataTypes.StringType);
        spark.udf().register("span_minus",          spanMinus,          DataTypes.StringType);
        spark.udf().register("span_intersection",   spanIntersection,   DataTypes.StringType);
        spark.udf().register("spanset_union",        spansetUnion,        DataTypes.StringType);
        spark.udf().register("spanset_minus",        spansetMinus,        DataTypes.StringType);
        spark.udf().register("spanset_intersection", spansetIntersection, DataTypes.StringType);

        // span / spanset accessors
        spark.udf().register("spann",     spann,     DataTypes.StringType);
        spark.udf().register("startSpan", startSpan, DataTypes.StringType);
        spark.udf().register("endSpan",   endSpan,   DataTypes.StringType);
        spark.udf().register("numSpans",  numSpans,  DataTypes.IntegerType);

        // set algebra
        spark.udf().register("setUnion",        setUnion,        DataTypes.StringType);
        spark.udf().register("setMinus",        setMinus,        DataTypes.StringType);
        spark.udf().register("setIntersection", setIntersection, DataTypes.StringType);

        // tbox / stbox algebra
        spark.udf().register("tbox_union",         tboxUnion,         DataTypes.StringType);
        spark.udf().register("tbox_intersection",  tboxIntersection,  DataTypes.StringType);
        spark.udf().register("stbox_union",        stboxUnion,        DataTypes.StringType);
        spark.udf().register("stbox_intersection", stboxIntersection, DataTypes.StringType);
        spark.udf().register("bigintSpan",         bigintSpan,        DataTypes.StringType);

        // tnumber x tbox restriction
        spark.udf().register("atTbox",    atTbox,    DataTypes.StringType);
        spark.udf().register("minusTbox", minusTbox, DataTypes.StringType);

        // distance (float / time typed)
        spark.udf().register("span_distance",        spanDistance,        DataTypes.DoubleType);
        spark.udf().register("spanset_distance",     spansetDistance,     DataTypes.DoubleType);
        spark.udf().register("set_distance",         setDistance,         DataTypes.DoubleType);
        spark.udf().register("time_distance",        timeDistanceSpan,    DataTypes.DoubleType);
        spark.udf().register("time_distance_spanset", timeDistanceSpanset, DataTypes.DoubleType);
    }
}
