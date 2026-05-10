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
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for ttext case-conversion operations.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class TTextUDFs {

    private TTextUDFs() {}

    // ttext_upper(ttext hex-WKB) → ttext hex-WKB
    public static final UDF1<String, String> ttextUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            Pointer r = functions.ttext_upper(p);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ttext_lower(ttext hex-WKB) → ttext hex-WKB
    public static final UDF1<String, String> ttextLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            Pointer r = functions.ttext_lower(p);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ttext_initcap(ttext hex-WKB) → ttext hex-WKB
    public static final UDF1<String, String> ttextInitcap =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            Pointer r = functions.ttext_initcap(p);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // ttext comparison operators  (scalar text vs ttext)
    //
    // MEOS: teq/tne/tlt/tle/tgt/tge_text_ttext(text *, Temporal *) → Temporal *
    //       teq/tne/tlt/tle/tgt/tge_ttext_text(Temporal *, text *) → Temporal *
    //
    // text * is created from a String via a dummy single-instant ttext and
    // ttext_value_n, since text_in is not exposed by JMEOS-1.4.
    //
    // Both operators return a tbool hex-WKB (temporal boolean).
    // ------------------------------------------------------------------

    // Helper: allocate a MEOS text* from a Java String.
    // Returns {textPtr, dummyTtext}; caller must MeosMemory.free() both.
    private static Pointer[] makeTextPtr(String val) {
        Pointer dummy = functions.ttext_in(val + "@2000-01-01 00:00:00+00");
        if (dummy == null) return null;
        Pointer textPtr = functions.ttext_value_n(dummy, 1);
        if (textPtr == null) { MeosMemory.free(dummy); return null; }
        return new Pointer[]{textPtr, dummy};
    }

    private static String ttextCompare(String textVal, String ttextHex, java.util.function.BiFunction<Pointer, Pointer, Pointer> fn) {
        if (textVal == null || ttextHex == null) return null;
        MeosThread.ensureReady();
        Pointer[] tp = makeTextPtr(textVal);
        if (tp == null) return null;
        Pointer tptr = functions.temporal_from_hexwkb(ttextHex);
        if (tptr == null) { MeosMemory.free(tp[0]); MeosMemory.free(tp[1]); return null; }
        try {
            Pointer result = fn.apply(tp[0], tptr);
            if (result == null) return null;
            try { return functions.temporal_as_hexwkb(result, (byte) 0); }
            finally { MeosMemory.free(result); }
        } finally {
            MeosMemory.free(tptr);
            MeosMemory.free(tp[0]);
            MeosMemory.free(tp[1]);
        }
    }

    private static String ttextCompareRev(String ttextHex, String textVal, java.util.function.BiFunction<Pointer, Pointer, Pointer> fn) {
        if (ttextHex == null || textVal == null) return null;
        MeosThread.ensureReady();
        Pointer tptr = functions.temporal_from_hexwkb(ttextHex);
        if (tptr == null) return null;
        Pointer[] tp = makeTextPtr(textVal);
        if (tp == null) { MeosMemory.free(tptr); return null; }
        try {
            Pointer result = fn.apply(tptr, tp[0]);
            if (result == null) return null;
            try { return functions.temporal_as_hexwkb(result, (byte) 0); }
            finally { MeosMemory.free(result); }
        } finally {
            MeosMemory.free(tptr);
            MeosMemory.free(tp[0]);
            MeosMemory.free(tp[1]);
        }
    }

    // text op ttext
    public static final UDF2<String, String, String> teqTextTtext =
        (textVal, ttextHex) -> ttextCompare(textVal, ttextHex, functions::teq_text_ttext);
    public static final UDF2<String, String, String> tneTextTtext =
        (textVal, ttextHex) -> ttextCompare(textVal, ttextHex, functions::tne_text_ttext);
    public static final UDF2<String, String, String> tltTextTtext =
        (textVal, ttextHex) -> ttextCompare(textVal, ttextHex, functions::tlt_text_ttext);
    public static final UDF2<String, String, String> tleTextTtext =
        (textVal, ttextHex) -> ttextCompare(textVal, ttextHex, functions::tle_text_ttext);
    public static final UDF2<String, String, String> tgtTextTtext =
        (textVal, ttextHex) -> ttextCompare(textVal, ttextHex, functions::tgt_text_ttext);
    public static final UDF2<String, String, String> tgeTextTtext =
        (textVal, ttextHex) -> ttextCompare(textVal, ttextHex, functions::tge_text_ttext);

    // ttext op text
    public static final UDF2<String, String, String> teqTtextText =
        (ttextHex, textVal) -> ttextCompareRev(ttextHex, textVal, functions::teq_ttext_text);
    public static final UDF2<String, String, String> tneTtextText =
        (ttextHex, textVal) -> ttextCompareRev(ttextHex, textVal, functions::tne_ttext_text);
    public static final UDF2<String, String, String> tltTtextText =
        (ttextHex, textVal) -> ttextCompareRev(ttextHex, textVal, functions::tlt_ttext_text);
    public static final UDF2<String, String, String> tleTtextText =
        (ttextHex, textVal) -> ttextCompareRev(ttextHex, textVal, functions::tle_ttext_text);
    public static final UDF2<String, String, String> tgtTtextText =
        (ttextHex, textVal) -> ttextCompareRev(ttextHex, textVal, functions::tgt_ttext_text);
    public static final UDF2<String, String, String> tgeTtextText =
        (ttextHex, textVal) -> ttextCompareRev(ttextHex, textVal, functions::tge_ttext_text);

    public static void registerAll(SparkSession spark) {
        spark.udf().register("ttextUpper",   ttextUpper,   DataTypes.StringType);
        spark.udf().register("ttextLower",   ttextLower,   DataTypes.StringType);
        spark.udf().register("ttextInitcap", ttextInitcap, DataTypes.StringType);
        // text op ttext comparison operators
        spark.udf().register("teqTextTtext", teqTextTtext, DataTypes.StringType);
        spark.udf().register("tneTextTtext", tneTextTtext, DataTypes.StringType);
        spark.udf().register("tltTextTtext", tltTextTtext, DataTypes.StringType);
        spark.udf().register("tleTextTtext", tleTextTtext, DataTypes.StringType);
        spark.udf().register("tgtTextTtext", tgtTextTtext, DataTypes.StringType);
        spark.udf().register("tgeTextTtext", tgeTextTtext, DataTypes.StringType);
        // ttext op text comparison operators
        spark.udf().register("teqTtextText", teqTtextText, DataTypes.StringType);
        spark.udf().register("tneTtextText", tneTtextText, DataTypes.StringType);
        spark.udf().register("tltTtextText", tltTtextText, DataTypes.StringType);
        spark.udf().register("tleTtextText", tleTtextText, DataTypes.StringType);
        spark.udf().register("tgtTtextText", tgtTtextText, DataTypes.StringType);
        spark.udf().register("tgeTtextText", tgeTtextText, DataTypes.StringType);

        // ttext concatenation (MEOS textcat_ttext_*)
        spark.udf().register("ttextCatTtextText", ttextCatTtextText, DataTypes.StringType);
        spark.udf().register("ttextCatTextTtext", ttextCatTextTtext, DataTypes.StringType);
        spark.udf().register("ttextCatTtextTtext", ttextCatTtextTtext, DataTypes.StringType);
        // MobilityDB SQL bare-name alias
        spark.udf().register("ttextCat", ttextCatTtextTtext, DataTypes.StringType);
        // textset concatenation
        spark.udf().register("textsetCatTextsetText", textsetCatTextsetText, DataTypes.StringType);
        spark.udf().register("textsetCatTextTextset", textsetCatTextTextset, DataTypes.StringType);
        spark.udf().register("textsetCat",            textsetCatTextsetText, DataTypes.StringType);
    }

    public static final UDF2<String, String, String> textsetCatTextsetText =
        (setHex, txt) -> {
            if (setHex == null || txt == null) return null;
            MeosThread.ensureReady();
            Pointer s = functions.set_from_hexwkb(setHex);
            if (s == null) return null;
            Pointer t = functions.cstring2text(txt);
            if (t == null) { MeosMemory.free(s); return null; }
            try {
                Pointer r = functions.textcat_textset_text(s, t);
                if (r == null) return null;
                try { return functions.set_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(s, t); }
        };

    public static final UDF2<String, String, String> textsetCatTextTextset =
        (txt, setHex) -> {
            if (txt == null || setHex == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.cstring2text(txt);
            if (t == null) return null;
            Pointer s = functions.set_from_hexwkb(setHex);
            if (s == null) { MeosMemory.free(t); return null; }
            try {
                Pointer r = functions.textcat_text_textset(t, s);
                if (r == null) return null;
                try { return functions.set_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(t, s); }
        };

    public static final UDF2<String, String, String> ttextCatTtextText =
        (ttextHex, txt) -> {
            if (ttextHex == null || txt == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(ttextHex);
            if (p == null) return null;
            Pointer t = functions.cstring2text(txt);
            if (t == null) { MeosMemory.free(p); return null; }
            try {
                Pointer r = functions.textcat_ttext_text(p, t);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p, t); }
        };

    public static final UDF2<String, String, String> ttextCatTextTtext =
        (txt, ttextHex) -> {
            if (txt == null || ttextHex == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.cstring2text(txt);
            if (t == null) return null;
            Pointer p = functions.temporal_from_hexwkb(ttextHex);
            if (p == null) { MeosMemory.free(t); return null; }
            try {
                Pointer r = functions.textcat_text_ttext(t, p);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(t, p); }
        };

    public static final UDF2<String, String, String> ttextCatTtextTtext =
        (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(h1);
            if (p1 == null) return null;
            Pointer p2 = functions.temporal_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = functions.textcat_ttext_ttext(p1, p2);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p1, p2); }
        };
}
