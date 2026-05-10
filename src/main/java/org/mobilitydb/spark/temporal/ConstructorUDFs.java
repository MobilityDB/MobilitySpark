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
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for constructing temporal and span types from text literals.
 *
 * All UDFs accept a WKT/text literal and return the internal hex-WKB
 * representation used throughout MobilitySpark. This matches the MobilityDuck
 * constructor surface (tstzspan, intspan, tint, tfloat, …).
 *
 * Storage convention: temporal values and span/set values are stored as
 * hex-WKB strings produced by temporal_as_hexwkb / span_as_hexwkb.
 *
 * MEOS function authority: meos/include/meos.h and meos/include/meos_geo.h
 */
public final class ConstructorUDFs {

    private ConstructorUDFs() {}

    // ------------------------------------------------------------------
    // Temporal type constructors: text literal → hex-WKB STRING
    // ------------------------------------------------------------------

    // tint("[1@2020-01-01, 2@2020-01-02]") → hex-WKB
    // MEOS: tint_in(const char *) → Temporal *
    public static final UDF1<String, String> tint =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.tint_in(s);
            if (ptr == null) return null;
            return functions.temporal_as_hexwkb(ptr, (byte) 0);
        };

    // tfloat("1.5@2020-01-01") → hex-WKB
    // MEOS: tfloat_in(const char *) → Temporal *
    public static final UDF1<String, String> tfloat =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.tfloat_in(s);
            if (ptr == null) return null;
            return functions.temporal_as_hexwkb(ptr, (byte) 0);
        };

    // tbool("true@2020-01-01") → hex-WKB
    // MEOS: tbool_in(const char *) → Temporal *
    public static final UDF1<String, String> tbool =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.tbool_in(s);
            if (ptr == null) return null;
            return functions.temporal_as_hexwkb(ptr, (byte) 0);
        };

    // ttext("hello@2020-01-01") → hex-WKB
    // MEOS: ttext_in(const char *) → Temporal *
    public static final UDF1<String, String> ttext =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.ttext_in(s);
            if (ptr == null) return null;
            return functions.temporal_as_hexwkb(ptr, (byte) 0);
        };

    // tgeogpoint("POINT(4.35 50.85)@2020-01-01") → hex-WKB
    // MEOS: tgeogpoint_in(const char *) → Temporal *
    public static final UDF1<String, String> tgeogpoint =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.tgeogpoint_in(s);
            if (ptr == null) return null;
            return functions.temporal_as_hexwkb(ptr, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Span type constructors: text literal → hex-WKB STRING
    // ------------------------------------------------------------------

    // tstzspan("[2020-01-01, 2020-01-02)") → hex-WKB
    // MEOS: tstzspan_in(const char *) → Span *
    public static final UDF1<String, String> tstzspan =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.tstzspan_in(s);
            if (ptr == null) return null;
            return functions.span_as_hexwkb(ptr, (byte) 0);
        };

    // tstzspanset("{[2020-01-01, 2020-01-02), [2020-03-01, 2020-04-01)}") → hex-WKB
    // MEOS: tstzspanset_in(const char *) → SpanSet *
    public static final UDF1<String, String> tstzspanset =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.tstzspanset_in(s);
            if (ptr == null) return null;
            return functions.spanset_as_hexwkb(ptr, (byte) 0);
        };

    // intspan("[1, 10)") → hex-WKB
    // MEOS: intspan_in(const char *) → Span *
    public static final UDF1<String, String> intspan =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.intspan_in(s);
            if (ptr == null) return null;
            return functions.span_as_hexwkb(ptr, (byte) 0);
        };

    // floatspan("[1.0, 10.0)") → hex-WKB
    // MEOS: floatspan_in(const char *) → Span *
    public static final UDF1<String, String> floatspan =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.floatspan_in(s);
            if (ptr == null) return null;
            return functions.span_as_hexwkb(ptr, (byte) 0);
        };

    // datespan("[2020-01-01, 2020-01-31)") → hex-WKB
    // MEOS: datespan_in(const char *) → Span *
    public static final UDF1<String, String> datespan =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.datespan_in(s);
            if (ptr == null) return null;
            return functions.span_as_hexwkb(ptr, (byte) 0);
        };

    // datespanset("{[2020-01-01, 2020-01-31), [2020-06-01, 2020-06-30)}") → hex-WKB
    // MEOS: datespanset_in(const char *) → SpanSet *
    public static final UDF1<String, String> datespanset =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.datespanset_in(s);
            if (ptr == null) return null;
            return functions.spanset_as_hexwkb(ptr, (byte) 0);
        };

    // intset("{1, 2, 3, 4}") → hex-WKB
    // MEOS: intset_in(const char *) → Set *
    public static final UDF1<String, String> intset =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.intset_in(s);
            if (ptr == null) return null;
            return functions.set_as_hexwkb(ptr, (byte) 0);
        };

    // floatset("{1.1, 2.2, 3.3}") → hex-WKB
    // MEOS: floatset_in(const char *) → Set *
    public static final UDF1<String, String> floatset =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.floatset_in(s);
            if (ptr == null) return null;
            return functions.set_as_hexwkb(ptr, (byte) 0);
        };

    // tstzset("{2020-01-01, 2020-02-01, 2020-03-01}") → hex-WKB
    // MEOS: tstzset_in(const char *) → Set *
    public static final UDF1<String, String> tstzset =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.tstzset_in(s);
            if (ptr == null) return null;
            return functions.set_as_hexwkb(ptr, (byte) 0);
        };

    // textset("{hello, world}") → hex-WKB
    // MEOS: textset_in(const char *) → Set *
    public static final UDF1<String, String> textset =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.textset_in(s);
            if (ptr == null) return null;
            return functions.set_as_hexwkb(ptr, (byte) 0);
        };

    // bigintset("{1000, 2000, 3000}") → hex-WKB
    // MEOS: bigintset_in(const char *) → Set *
    public static final UDF1<String, String> bigintset =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.bigintset_in(s);
            if (ptr == null) return null;
            return functions.set_as_hexwkb(ptr, (byte) 0);
        };

    // stbox("STBOX X((1,2),(3,4))") → hex-WKB
    // MEOS: stbox_in(const char *) → STBox *
    public static final UDF1<String, String> stbox =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.stbox_in(s);
            if (ptr == null) return null;
            Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
            return functions.stbox_as_hexwkb(ptr, (byte) 0, sizeOut);
        };

    // tbox("TBOX T([2020-01-01,2020-01-02))") → hex-WKB
    // MEOS: tbox_in(const char *) → TBox *
    public static final UDF1<String, String> tbox =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.tbox_in(s);
            if (ptr == null) return null;
            Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
            return functions.tbox_as_hexwkb(ptr, (byte) 0, sizeOut);
        };

    // ------------------------------------------------------------------
    // MFJSON constructors  (JSON string in → hex-WKB out)
    //
    // MEOS: tbool_from_mfjson, tint_from_mfjson, tfloat_from_mfjson,
    //       ttext_from_mfjson  (meos.h)
    //       tgeompoint_from_mfjson, tgeogpoint_from_mfjson  (meos_geo.h)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tboolFromMfjson =
        (json) -> {
            if (json == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tbool_from_mfjson(json);
            if (p == null) return null;
            return functions.temporal_as_hexwkb(p, (byte) 0);
        };

    public static final UDF1<String, String> tintFromMfjson =
        (json) -> {
            if (json == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tint_from_mfjson(json);
            if (p == null) return null;
            return functions.temporal_as_hexwkb(p, (byte) 0);
        };

    public static final UDF1<String, String> tfloatFromMfjson =
        (json) -> {
            if (json == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tfloat_from_mfjson(json);
            if (p == null) return null;
            return functions.temporal_as_hexwkb(p, (byte) 0);
        };

    public static final UDF1<String, String> ttextFromMfjson =
        (json) -> {
            if (json == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.ttext_from_mfjson(json);
            if (p == null) return null;
            return functions.temporal_as_hexwkb(p, (byte) 0);
        };

    public static final UDF1<String, String> tgeompointFromMfjson =
        (json) -> {
            if (json == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tgeompoint_from_mfjson(json);
            if (p == null) return null;
            return functions.temporal_as_hexwkb(p, (byte) 0);
        };

    public static final UDF1<String, String> tgeogpointFromMfjson =
        (json) -> {
            if (json == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tgeogpoint_from_mfjson(json);
            if (p == null) return null;
            return functions.temporal_as_hexwkb(p, (byte) 0);
        };

    public static void registerAll(SparkSession spark) {
        spark.udf().register("tint",                tint,                DataTypes.StringType);
        spark.udf().register("tfloat",              tfloat,              DataTypes.StringType);
        spark.udf().register("tbool",               tbool,               DataTypes.StringType);
        spark.udf().register("ttext",               ttext,               DataTypes.StringType);
        spark.udf().register("tgeogpoint",          tgeogpoint,          DataTypes.StringType);
        spark.udf().register("tstzspan",            tstzspan,            DataTypes.StringType);
        spark.udf().register("tstzspanset",         tstzspanset,         DataTypes.StringType);
        spark.udf().register("intspan",             intspan,             DataTypes.StringType);
        spark.udf().register("floatspan",           floatspan,           DataTypes.StringType);
        spark.udf().register("datespan",            datespan,            DataTypes.StringType);
        spark.udf().register("datespanset",         datespanset,         DataTypes.StringType);
        spark.udf().register("intset",              intset,              DataTypes.StringType);
        spark.udf().register("floatset",            floatset,            DataTypes.StringType);
        spark.udf().register("tstzset",             tstzset,             DataTypes.StringType);
        spark.udf().register("textset",             textset,             DataTypes.StringType);
        spark.udf().register("bigintset",           bigintset,           DataTypes.StringType);
        spark.udf().register("stbox",               stbox,               DataTypes.StringType);
        spark.udf().register("tbox",                tbox,                DataTypes.StringType);
        spark.udf().register("tboolFromMfjson",     tboolFromMfjson,     DataTypes.StringType);
        spark.udf().register("tintFromMfjson",      tintFromMfjson,      DataTypes.StringType);
        spark.udf().register("tfloatFromMfjson",    tfloatFromMfjson,    DataTypes.StringType);
        spark.udf().register("ttextFromMfjson",     ttextFromMfjson,     DataTypes.StringType);
        spark.udf().register("tgeompointFromMfjson",tgeompointFromMfjson,DataTypes.StringType);
        spark.udf().register("tgeogpointFromMfjson",tgeogpointFromMfjson,DataTypes.StringType);
    }
}
