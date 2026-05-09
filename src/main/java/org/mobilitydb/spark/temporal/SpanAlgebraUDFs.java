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
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for span and set topology predicates and algebraic operations.
 *
 * All inputs and outputs use hex-WKB string encoding (the internal MobilitySpark
 * storage format). Set-returning operations (union, minus) produce a SpanSet
 * hex-WKB; intersection returns a Span hex-WKB (null when disjoint).
 *
 * Naming convention: camelCase to avoid conflicts with Spark built-ins.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class SpanAlgebraUDFs {

    private SpanAlgebraUDFs() {}

    // ------------------------------------------------------------------
    // Helper: parse hex-WKB string → span Pointer
    // ------------------------------------------------------------------
    private static Pointer spanPtr(String hex) {
        return hex == null ? null : functions.span_from_hexwkb(hex);
    }

    private static Pointer spansetPtr(String hex) {
        return hex == null ? null : functions.spanset_from_hexwkb(hex);
    }

    private static Pointer setPtr(String hex) {
        return hex == null ? null : functions.set_from_hexwkb(hex);
    }

    // ------------------------------------------------------------------
    // Span topology predicates  (span, span) → Boolean
    //
    // MEOS: contains_span_span / contained_span_span / overlaps_span_span
    //       adjacent_span_span / left_span_span / right_span_span
    //       overleft_span_span / overright_span_span
    // ------------------------------------------------------------------

    // spanContains("[1,10)", "[2,5)") → true
    public static final UDF2<String, String, Boolean> spanContains =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.contains_span_span(p1, p2);
        };

    // spanContainedIn("[2,5)", "[1,10)") → true
    public static final UDF2<String, String, Boolean> spanContainedIn =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.contained_span_span(p1, p2);
        };

    // spanOverlaps("[1,5)", "[3,10)") → true
    public static final UDF2<String, String, Boolean> spanOverlaps =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.overlaps_span_span(p1, p2);
        };

    // spanAdjacent("[1,5)", "[5,10)") → true
    public static final UDF2<String, String, Boolean> spanAdjacent =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.adjacent_span_span(p1, p2);
        };

    // spanLeft("[1,5)", "[6,10)") → true
    public static final UDF2<String, String, Boolean> spanLeft =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.left_span_span(p1, p2);
        };

    // spanRight("[6,10)", "[1,5)") → true
    public static final UDF2<String, String, Boolean> spanRight =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.right_span_span(p1, p2);
        };

    // spanOverleft("[1,5)", "[3,10)") → true (s1 does not extend right of s2)
    public static final UDF2<String, String, Boolean> spanOverleft =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.overleft_span_span(p1, p2);
        };

    // spanOverright("[3,10)", "[1,5)") → true (s1 does not extend left of s2)
    public static final UDF2<String, String, Boolean> spanOverright =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.overright_span_span(p1, p2);
        };

    // ------------------------------------------------------------------
    // Span algebraic operations  (span, span) → hex-WKB STRING
    //
    // MEOS: union_span_span → SpanSet *
    //       intersection_span_span → Span * (null when disjoint)
    //       minus_span_span → SpanSet *
    // ------------------------------------------------------------------

    // spanUnion("[1,5)", "[3,10)") → "{[1,10)}"  (SpanSet hex-WKB)
    public static final UDF2<String, String, String> spanUnion =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer ss = functions.union_span_span(p1, p2);
            if (ss == null) return null;
            return functions.spanset_as_hexwkb(ss, (byte) 0);
        };

    // spanIntersection("[1,10)", "[3,7)") → "[3,7)"  (Span hex-WKB; null if disjoint)
    public static final UDF2<String, String, String> spanIntersection =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer s = functions.intersection_span_span(p1, p2);
            if (s == null) return null;
            return functions.span_as_hexwkb(s, (byte) 0);
        };

    // spanMinus("[1,10)", "[3,7)") → "{[1,3),[7,10)}"  (SpanSet hex-WKB)
    public static final UDF2<String, String, String> spanMinus =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer ss = functions.minus_span_span(p1, p2);
            if (ss == null) return null;
            return functions.spanset_as_hexwkb(ss, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Tstzspan distance (tstzspan, tstzspan) → Double (seconds)
    //
    // MEOS: distance_tstzspan_tstzspan → double
    // ------------------------------------------------------------------

    // tstzspanDistance("[2020-01-01, 2020-01-05)", "[2020-01-10, 2020-01-15)") → 432000.0
    public static final UDF2<String, String, Double> tstzspanDistance =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spanPtr(s1), p2 = spanPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.distance_tstzspan_tstzspan(p1, p2);
        };

    // ------------------------------------------------------------------
    // Spanset topology predicates  (spanset, span) → Boolean
    //
    // MEOS: contains_spanset_span / contained_spanset_span
    //       overlaps_spanset_spanset
    // ------------------------------------------------------------------

    // spansetContainsSpan("{[1,5),[7,10)}", "[2,4)") → true
    public static final UDF2<String, String, Boolean> spansetContainsSpan =
        (ss, s) -> {
            MeosThread.ensureReady();
            Pointer pss = spansetPtr(ss), ps = spanPtr(s);
            if (pss == null || ps == null) return null;
            return functions.contains_spanset_span(pss, ps);
        };

    // spanContainedInSpanset("[2,4)", "{[1,5),[7,10)}") → true
    public static final UDF2<String, String, Boolean> spanContainedInSpanset =
        (s, ss) -> {
            MeosThread.ensureReady();
            Pointer ps = spanPtr(s), pss = spansetPtr(ss);
            if (ps == null || pss == null) return null;
            return functions.contained_span_spanset(ps, pss);
        };

    // spansetOverlaps("{[1,5)}", "{[3,10)}") → true
    public static final UDF2<String, String, Boolean> spansetOverlaps =
        (ss1, ss2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spansetPtr(ss1), p2 = spansetPtr(ss2);
            if (p1 == null || p2 == null) return null;
            return functions.overlaps_spanset_spanset(p1, p2);
        };

    // ------------------------------------------------------------------
    // Spanset algebraic operations  (spanset, spanset) → hex-WKB STRING
    //
    // MEOS: union_spanset_spanset / intersection_spanset_spanset
    //       minus_spanset_spanset
    // ------------------------------------------------------------------

    // spansetUnion("{[1,5)}", "{[7,10)}") → "{[1,5),[7,10)}"
    public static final UDF2<String, String, String> spansetUnion =
        (ss1, ss2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spansetPtr(ss1), p2 = spansetPtr(ss2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.union_spanset_spanset(p1, p2);
            if (r == null) return null;
            return functions.spanset_as_hexwkb(r, (byte) 0);
        };

    // spansetIntersection("{[1,10)}", "{[3,7)}") → "{[3,7)}"
    public static final UDF2<String, String, String> spansetIntersection =
        (ss1, ss2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spansetPtr(ss1), p2 = spansetPtr(ss2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.intersection_spanset_spanset(p1, p2);
            if (r == null) return null;
            return functions.spanset_as_hexwkb(r, (byte) 0);
        };

    // spansetMinus("{[1,10)}", "{[3,7)}") → "{[1,3),[7,10)}"
    public static final UDF2<String, String, String> spansetMinus =
        (ss1, ss2) -> {
            MeosThread.ensureReady();
            Pointer p1 = spansetPtr(ss1), p2 = spansetPtr(ss2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.minus_spanset_spanset(p1, p2);
            if (r == null) return null;
            return functions.spanset_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Set topology predicates  (set, set) → Boolean
    //
    // MEOS: contains_set_set / overlaps_set_set
    // ------------------------------------------------------------------

    // setContains("{1,2,3,4}", "{2,3}") → true
    public static final UDF2<String, String, Boolean> setContains =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = setPtr(s1), p2 = setPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.contains_set_set(p1, p2);
        };

    // setOverlaps("{1,2,3}", "{3,4,5}") → true
    public static final UDF2<String, String, Boolean> setOverlaps =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = setPtr(s1), p2 = setPtr(s2);
            if (p1 == null || p2 == null) return null;
            return functions.overlaps_set_set(p1, p2);
        };

    // ------------------------------------------------------------------
    // Set algebraic operations  (set, set) → hex-WKB STRING
    //
    // MEOS: union_set_set / intersection_set_set / minus_set_set → Set *
    // ------------------------------------------------------------------

    // setUnion("{1,2,3}", "{4,5}") → "{1,2,3,4,5}"
    public static final UDF2<String, String, String> setUnion =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = setPtr(s1), p2 = setPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.union_set_set(p1, p2);
            if (r == null) return null;
            return functions.set_as_hexwkb(r, (byte) 0);
        };

    // setIntersection("{1,2,3,4}", "{3,4,5}") → "{3,4}"
    public static final UDF2<String, String, String> setIntersection =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = setPtr(s1), p2 = setPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.intersection_set_set(p1, p2);
            if (r == null) return null;
            return functions.set_as_hexwkb(r, (byte) 0);
        };

    // setMinus("{1,2,3,4}", "{3,4,5}") → "{1,2}"
    public static final UDF2<String, String, String> setMinus =
        (s1, s2) -> {
            MeosThread.ensureReady();
            Pointer p1 = setPtr(s1), p2 = setPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.minus_set_set(p1, p2);
            if (r == null) return null;
            return functions.set_as_hexwkb(r, (byte) 0);
        };

    public static void registerAll(SparkSession spark) {
        // Span topology predicates
        spark.udf().register("spanContains",             spanContains,             DataTypes.BooleanType);
        spark.udf().register("spanContainedIn",          spanContainedIn,          DataTypes.BooleanType);
        spark.udf().register("spanOverlaps",             spanOverlaps,             DataTypes.BooleanType);
        spark.udf().register("spanAdjacent",             spanAdjacent,             DataTypes.BooleanType);
        spark.udf().register("spanLeft",                 spanLeft,                 DataTypes.BooleanType);
        spark.udf().register("spanRight",                spanRight,                DataTypes.BooleanType);
        spark.udf().register("spanOverleft",             spanOverleft,             DataTypes.BooleanType);
        spark.udf().register("spanOverright",            spanOverright,            DataTypes.BooleanType);
        // Span algebra
        spark.udf().register("spanUnion",                spanUnion,                DataTypes.StringType);
        spark.udf().register("spanIntersection",         spanIntersection,         DataTypes.StringType);
        spark.udf().register("spanMinus",                spanMinus,                DataTypes.StringType);
        spark.udf().register("tstzspanDistance",         tstzspanDistance,         DataTypes.DoubleType);
        // Spanset predicates
        spark.udf().register("spansetContainsSpan",      spansetContainsSpan,      DataTypes.BooleanType);
        spark.udf().register("spanContainedInSpanset",   spanContainedInSpanset,   DataTypes.BooleanType);
        spark.udf().register("spansetOverlaps",          spansetOverlaps,          DataTypes.BooleanType);
        // Spanset algebra
        spark.udf().register("spansetUnion",             spansetUnion,             DataTypes.StringType);
        spark.udf().register("spansetIntersection",      spansetIntersection,      DataTypes.StringType);
        spark.udf().register("spansetMinus",             spansetMinus,             DataTypes.StringType);
        // Set predicates
        spark.udf().register("setContains",              setContains,              DataTypes.BooleanType);
        spark.udf().register("setOverlaps",              setOverlaps,              DataTypes.BooleanType);
        // Set algebra
        spark.udf().register("setUnion",                 setUnion,                 DataTypes.StringType);
        spark.udf().register("setIntersection",          setIntersection,          DataTypes.StringType);
        spark.udf().register("setMinus",                 setMinus,                 DataTypes.StringType);
    }
}
