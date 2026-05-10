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
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import java.util.HexFormat;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.function.ToDoubleFunction;

/**
 * Spark SQL UDFs for typed accessor aliases bridging MobilityDB SQL bare
 * names (e.g. `intspan_width`, `dates`, `valueSpans`, `tnumberToSpan`)
 * to existing JMEOS bindings.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class AccessorAliasUDFs {

    private AccessorAliasUDFs() {}

    // ------------------------------------------------------------------
    // Per-type span/spanset width accessors
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> intspanWidth =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.intspan_width(p); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, Long> bigintspanWidth =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.bigintspan_width(p); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, Double> floatspanWidth =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.floatspan_width(p); }
            finally { MeosMemory.free(p); }
        };

    // boundspan param: TRUE = use the span of the union of all components,
    // FALSE = sum of individual span widths.
    public static final UDF2<String, Boolean, Integer> intspansetWidth =
        (hex, boundspan) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.intspanset_width(p, boundspan != null && boundspan); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF2<String, Boolean, Long> bigintspansetWidth =
        (hex, boundspan) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.bigintspanset_width(p, boundspan != null && boundspan); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF2<String, Boolean, Double> floatspansetWidth =
        (hex, boundspan) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.floatspanset_width(p, boundspan != null && boundspan); }
            finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // Date span/spanset accessors (date represented as int days)
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer> startDate =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.datespanset_start_date(p); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, Integer> endDate =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.datespanset_end_date(p); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, Integer> numDates =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.datespanset_num_dates(p); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF2<String, Integer, Integer> dateN =
        (hex, n) -> {
            if (hex == null || n == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.datespanset_date_n(p, n);
                if (r == null) return null;
                // Pointer has the date as 4 bytes — read as int
                int date = r.getInt(0);
                return date;
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String> dates =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.datespanset_dates(p);
                if (r == null) return null;
                try { return functions.set_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // tnumber valueSpans (already exists as tnumberValuespans, add alias)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> valueSpan =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.tnumber_to_span(p);
                if (r == null) return null;
                try { return functions.span_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // Typed set values — return Java arrays of primitive boxes
    // ------------------------------------------------------------------

    public static final UDF1<String, Integer[]> intsetValues =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = functions.set_num_values(p);
                Integer[] out = new Integer[n];
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer outBuf = rt.getMemoryManager().allocateDirect(8);
                for (int i = 0; i < n; i++) {
                    if (functions.MeosLibrary.meos.intset_value_n(p, i + 1, outBuf)) {
                        out[i] = outBuf.getInt(0);
                    }
                }
                return out;
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, Long[]> bigintsetValues =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = functions.set_num_values(p);
                Long[] out = new Long[n];
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer outBuf = rt.getMemoryManager().allocateDirect(8);
                for (int i = 0; i < n; i++) {
                    if (functions.MeosLibrary.meos.bigintset_value_n(p, i + 1, outBuf)) {
                        out[i] = outBuf.getLong(0);
                    }
                }
                return out;
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // Array-returning bbox accessors (Spark ArrayType<String>)
    //
    // Note on TBox/STBox struct sizes: the returned pointer is a flat array
    // of contiguous structs, so we need sizeof to advance per element.
    // Both struct sizes are stable across MEOS releases since adding a new
    // field would break ABI; if MEOS changes them, regenerate JMEOS and
    // adjust here.
    // ------------------------------------------------------------------

    private static final int TBOX_SIZE  = 56;  // Span period(24) + Span span(24) + int16 flags(2) + 6 bytes padding
    private static final int STBOX_SIZE = 80;  // Span period(24) + 6×double(48) + int32 srid(4) + int16 flags(2) + 2 bytes padding

    public static final UDF1<String, String[]> tboxes =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.tnumber_tboxes(p, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++) {
                        Pointer elem = arr.slice(i * TBOX_SIZE);
                        out[i] = functions.tbox_as_hexwkb(elem, (byte) 0, sizeOut);
                    }
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    private static final int SPAN_SIZE = 24;  // 4 byte header + 4 padding + 2 Datum (8 each)

    public static final UDF3<String, Integer, Integer, String[]> intspanBins =
        (hex, vsize, vorigin) -> {
            if (hex == null || vsize == null || vorigin == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.intspan_bins(p, vsize, vorigin, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) out[i] = functions.span_as_hexwkb(arr.slice(i * SPAN_SIZE), (byte) 0);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF3<String, Long, Long, String[]> bigintspanBins =
        (hex, vsize, vorigin) -> {
            if (hex == null || vsize == null || vorigin == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.bigintspan_bins(p, vsize, vorigin, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) out[i] = functions.span_as_hexwkb(arr.slice(i * SPAN_SIZE), (byte) 0);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // Split-by-N functions (return arrays of Span/TBox/STBox hex-WKB)
    // ------------------------------------------------------------------

    private interface SplitFn { Pointer apply(Pointer t, int n, Pointer count); }

    private static String[] splitArrSpan(String hex, Integer n, SplitFn fn) {
        if (hex == null || n == null) return null;
        MeosThread.ensureReady();
        Pointer t = functions.temporal_from_hexwkb(hex);
        if (t == null) return null;
        try {
            jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
            Pointer countOut = rt.getMemoryManager().allocateDirect(4);
            Pointer arr = fn.apply(t, n, countOut);
            if (arr == null) return null;
            try {
                int cnt = countOut.getInt(0);
                String[] out = new String[cnt];
                for (int i = 0; i < cnt; i++) out[i] = functions.span_as_hexwkb(arr.slice(i * SPAN_SIZE), (byte) 0);
                return out;
            } finally { MeosMemory.free(arr); }
        } finally { MeosMemory.free(t); }
    }

    private static String[] splitArrTbox(String hex, Integer n, SplitFn fn) {
        if (hex == null || n == null) return null;
        MeosThread.ensureReady();
        Pointer t = functions.temporal_from_hexwkb(hex);
        if (t == null) return null;
        try {
            jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
            Pointer countOut = rt.getMemoryManager().allocateDirect(4);
            Pointer arr = fn.apply(t, n, countOut);
            if (arr == null) return null;
            try {
                int cnt = countOut.getInt(0);
                String[] out = new String[cnt];
                Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                for (int i = 0; i < cnt; i++) out[i] = functions.tbox_as_hexwkb(arr.slice(i * TBOX_SIZE), (byte) 0, sizeOut);
                return out;
            } finally { MeosMemory.free(arr); }
        } finally { MeosMemory.free(t); }
    }

    private static String[] splitArrStbox(String hex, Integer n, SplitFn fn) {
        if (hex == null || n == null) return null;
        MeosThread.ensureReady();
        Pointer t = functions.temporal_from_hexwkb(hex);
        if (t == null) return null;
        try {
            jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
            Pointer countOut = rt.getMemoryManager().allocateDirect(4);
            Pointer arr = fn.apply(t, n, countOut);
            if (arr == null) return null;
            try {
                int cnt = countOut.getInt(0);
                String[] out = new String[cnt];
                Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                for (int i = 0; i < cnt; i++) out[i] = functions.stbox_as_hexwkb(arr.slice(i * STBOX_SIZE), (byte) 0, sizeOut);
                return out;
            } finally { MeosMemory.free(arr); }
        } finally { MeosMemory.free(t); }
    }

    public static final UDF2<String, Integer, String[]> splitNSpans =
        (h, n) -> splitArrSpan(h, n, functions::temporal_split_n_spans);

    public static final UDF2<String, Integer, String[]> splitEachNSpans =
        (h, n) -> splitArrSpan(h, n, functions::temporal_split_each_n_spans);

    public static final UDF2<String, Integer, String[]> splitNTboxes =
        (h, n) -> splitArrTbox(h, n, functions::tnumber_split_n_tboxes);

    public static final UDF2<String, Integer, String[]> splitEachNTboxes =
        (h, n) -> splitArrTbox(h, n, functions::tnumber_split_each_n_tboxes);

    public static final UDF2<String, Integer, String[]> splitNStboxes =
        (h, n) -> splitArrStbox(h, n, functions::tgeo_split_n_stboxes);

    public static final UDF2<String, Integer, String[]> splitEachNStboxes =
        (h, n) -> splitArrStbox(h, n, functions::tgeo_split_each_n_stboxes);

    // ------------------------------------------------------------------
    // Temporal time bins / tstzspan bins (interval input as ISO 8601 string)
    // ------------------------------------------------------------------

    private static long tsToPgEpochMicros(java.sql.Timestamp ts) {
        return (ts.getTime() - 946684800L * 1000L) * 1000L;
    }

    public static final org.apache.spark.sql.api.java.UDF3<String, String, java.sql.Timestamp, String[]>
        timeBins = (hex, intervalStr, origin) -> {
            if (hex == null || intervalStr == null || origin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(hex);
            if (t == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(t); return null; }
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.MeosLibrary.meos.temporal_time_bins(t, iv, tsToPgEpochMicros(origin), countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) out[i] = functions.span_as_hexwkb(arr.slice(i * SPAN_SIZE), (byte) 0);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(t, iv); }
        };

    // ------------------------------------------------------------------
    // stbox_quad_split + getBin scalar
    // ------------------------------------------------------------------

    public static final UDF1<String, String[]> quadSplit =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer s = functions.stbox_from_hexwkb(hex);
            if (s == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.stbox_quad_split(s, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++) out[i] = functions.stbox_as_hexwkb(arr.slice(i * STBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(s); }
        };

    // getBin(timestamptz, interval, origin) → timestamptz (start of bin)
    public static final org.apache.spark.sql.api.java.UDF3<java.sql.Timestamp, String, java.sql.Timestamp, java.sql.Timestamp>
        timestamptzGetBin = (ts, intervalStr, origin) -> {
            if (ts == null || intervalStr == null || origin == null) return null;
            MeosThread.ensureReady();
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) return null;
            try {
                long binStart = functions.MeosLibrary.meos.timestamptz_get_bin(tsToPgEpochMicros(ts), iv, tsToPgEpochMicros(origin));
                // Convert PG-epoch micros → java Timestamp
                long unixMicros = binStart + 946684800L * 1000000L;
                return new java.sql.Timestamp(unixMicros / 1000);
            } finally { MeosMemory.free(iv); }
        };

    public static final UDF3<String, Integer, Integer, String[]> tintValueBins =
        (hex, vsize, vorigin) -> {
            if (hex == null || vsize == null || vorigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(hex);
            if (t == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.tint_value_bins(t, vsize, vorigin, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) out[i] = functions.span_as_hexwkb(arr.slice(i * SPAN_SIZE), (byte) 0);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(t); }
        };

    public static final UDF3<String, Double, Double, String[]> tfloatValueBins =
        (hex, vsize, vorigin) -> {
            if (hex == null || vsize == null || vorigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(hex);
            if (t == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.tfloat_value_bins(t, vsize, vorigin, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) out[i] = functions.span_as_hexwkb(arr.slice(i * SPAN_SIZE), (byte) 0);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(t); }
        };

    public static final org.apache.spark.sql.api.java.UDF3<String, String, java.sql.Timestamp, String[]>
        tstzspanBins = (hex, intervalStr, origin) -> {
            if (hex == null || intervalStr == null || origin == null) return null;
            MeosThread.ensureReady();
            Pointer s = functions.span_from_hexwkb(hex);
            if (s == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(s); return null; }
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.MeosLibrary.meos.tstzspan_bins(s, iv, tsToPgEpochMicros(origin), countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) out[i] = functions.span_as_hexwkb(arr.slice(i * SPAN_SIZE), (byte) 0);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(s, iv); }
        };

    public static final UDF3<String, Double, Double, String[]> floatspanBins =
        (hex, vsize, vorigin) -> {
            if (hex == null || vsize == null || vorigin == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.floatspan_bins(p, vsize, vorigin, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) out[i] = functions.span_as_hexwkb(arr.slice(i * SPAN_SIZE), (byte) 0);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String[]> stboxes =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.tgeo_stboxes(p, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++) {
                        Pointer elem = arr.slice(i * STBOX_SIZE);
                        out[i] = functions.stbox_as_hexwkb(elem, (byte) 0, sizeOut);
                    }
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // Time-weighted average value of tnumber
    // ------------------------------------------------------------------

    public static final UDF1<String, Double> avgValue =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.tnumber_avg_value(p); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, Double[]> floatsetValues =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = functions.set_num_values(p);
                Double[] out = new Double[n];
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer outBuf = rt.getMemoryManager().allocateDirect(8);
                for (int i = 0; i < n; i++) {
                    if (functions.MeosLibrary.meos.floatset_value_n(p, i + 1, outBuf)) {
                        out[i] = outBuf.getDouble(0);
                    }
                }
                return out;
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // Per-type setFromBinary aliases — generic byte[]→hex→set_from_hexwkb
    // ------------------------------------------------------------------

    private static UDF1<byte[], String> setFromBinaryFn() {
        return bytes -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    // ------------------------------------------------------------------
    // Array-returning accessors (return Spark ArrayType<String>)
    // ------------------------------------------------------------------

    // spans(spanset) → array of span hex-WKB strings
    public static final UDF1<String, String[]> spans =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try {
                int n = functions.spanset_num_spans(p);
                String[] out = new String[n];
                for (int i = 0; i < n; i++) {
                    // 1-based per MEOS convention
                    Pointer span = functions.spanset_span_n(p, i + 1);
                    if (span == null) { out[i] = null; continue; }
                    out[i] = functions.span_as_hexwkb(span, (byte) 0);
                    // span_n returns a fresh copy in newer MEOS; free it
                    MeosMemory.free(span);
                }
                return out;
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<byte[], String> intsetFromBinary    = setFromBinaryFn();
    public static final UDF1<byte[], String> bigintsetFromBinary = setFromBinaryFn();
    public static final UDF1<byte[], String> floatsetFromBinary  = setFromBinaryFn();
    public static final UDF1<byte[], String> textsetFromBinary   = setFromBinaryFn();
    public static final UDF1<byte[], String> tstzsetFromBinary   = setFromBinaryFn();
    public static final UDF1<byte[], String> datesetFromBinary   = setFromBinaryFn();

    public static void registerAll(SparkSession spark) {
        spark.udf().register("intspanWidth",       intspanWidth,       DataTypes.IntegerType);
        spark.udf().register("bigintspanWidth",    bigintspanWidth,    DataTypes.LongType);
        spark.udf().register("floatspanWidth",     floatspanWidth,     DataTypes.DoubleType);
        spark.udf().register("intspansetWidth",    intspansetWidth,    DataTypes.IntegerType);
        spark.udf().register("bigintspansetWidth", bigintspansetWidth, DataTypes.LongType);
        spark.udf().register("floatspansetWidth",  floatspansetWidth,  DataTypes.DoubleType);
        // single-arg width aliases (boundspan defaults to false in tests)
        spark.udf().register("width", floatspanWidth, DataTypes.DoubleType);

        spark.udf().register("startDate", startDate, DataTypes.IntegerType);
        spark.udf().register("endDate",   endDate,   DataTypes.IntegerType);
        spark.udf().register("numDates",  numDates,  DataTypes.IntegerType);
        spark.udf().register("dateN",     dateN,     DataTypes.IntegerType);
        spark.udf().register("dates",     dates,     DataTypes.StringType);

        spark.udf().register("valueSpan", valueSpan, DataTypes.StringType);

        spark.udf().register("intsetFromBinary",    intsetFromBinary,    DataTypes.StringType);
        spark.udf().register("bigintsetFromBinary", bigintsetFromBinary, DataTypes.StringType);
        spark.udf().register("floatsetFromBinary",  floatsetFromBinary,  DataTypes.StringType);
        spark.udf().register("textsetFromBinary",   textsetFromBinary,   DataTypes.StringType);
        spark.udf().register("tstzsetFromBinary",   tstzsetFromBinary,   DataTypes.StringType);
        spark.udf().register("datesetFromBinary",   datesetFromBinary,   DataTypes.StringType);

        // Array-returning accessors
        spark.udf().register("spans", spans, DataTypes.createArrayType(DataTypes.StringType));

        // tgeo type conversions
        spark.udf().register("tgeometry",  tgeometry,  DataTypes.StringType);
        spark.udf().register("tgeography", tgeography, DataTypes.StringType);
        // MobilityDB SQL bare-name aliases
        spark.udf().register("geometry",   tgeometry,  DataTypes.StringType);
        spark.udf().register("geography",  tgeography, DataTypes.StringType);
        // Introspection
        spark.udf().register("mobilitydbVersion",     mobilitydbVersion,     DataTypes.StringType);
        spark.udf().register("mobilitydbFullVersion", mobilitydbFullVersion, DataTypes.StringType);
        // valueSet (Datum-array → typed Set), segmentMin/MaxDuration, box3d
        spark.udf().register("valueSet",            valueSet,            DataTypes.StringType);
        spark.udf().register("segmentMinDuration",  segmentMinDuration,  DataTypes.StringType);
        spark.udf().register("segmentMaxDuration",  segmentMaxDuration,  DataTypes.StringType);
        spark.udf().register("box2d",               box2d,               DataTypes.StringType);
        spark.udf().register("box3d",               box3d,               DataTypes.StringType);
        // Typed set values (array-returning)
        spark.udf().register("intsetValues",    intsetValues,    DataTypes.createArrayType(DataTypes.IntegerType));
        spark.udf().register("bigintsetValues", bigintsetValues, DataTypes.createArrayType(DataTypes.LongType));
        spark.udf().register("floatsetValues",  floatsetValues,  DataTypes.createArrayType(DataTypes.DoubleType));
        // MobilityDB SQL bare-name aliases (typed dispatchers — picks float as default)
        spark.udf().register("getValues", floatsetValues, DataTypes.createArrayType(DataTypes.DoubleType));
        spark.udf().register("unnest",    floatsetValues, DataTypes.createArrayType(DataTypes.DoubleType));
        // Time tiling
        spark.udf().register("timeBins", timeBins, DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tstzspanBins", tstzspanBins, DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tintValueBins",   tintValueBins,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tfloatValueBins", tfloatValueBins, DataTypes.createArrayType(DataTypes.StringType));
        // Bare-name alias (defaults to float)
        spark.udf().register("valueBins", tfloatValueBins, DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("quadSplit", quadSplit, DataTypes.createArrayType(DataTypes.StringType));
        // Scalar getBin for timestamptz; numeric variants are floatBucket/intBucket
        spark.udf().register("timestamptzGetBin", timestamptzGetBin, DataTypes.TimestampType);
        spark.udf().register("getBin", timestamptzGetBin, DataTypes.TimestampType);
        spark.udf().register("avgValue", avgValue, DataTypes.DoubleType);
        spark.udf().register("tboxes",  tboxes,  DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("stboxes", stboxes, DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("intspanBins",    intspanBins,    DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("bigintspanBins", bigintspanBins, DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("floatspanBins",  floatspanBins,  DataTypes.createArrayType(DataTypes.StringType));
        // MobilityDB SQL bare-name alias
        spark.udf().register("bins",  floatspanBins,  DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("splitNSpans",       splitNSpans,       DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("splitEachNSpans",   splitEachNSpans,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("splitNTboxes",      splitNTboxes,      DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("splitEachNTboxes",  splitEachNTboxes,  DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("splitNStboxes",     splitNStboxes,     DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("splitEachNStboxes", splitEachNStboxes, DataTypes.createArrayType(DataTypes.StringType));
    }

    // ------------------------------------------------------------------
    // Introspection
    // ------------------------------------------------------------------

    public static final org.apache.spark.sql.api.java.UDF0<String> mobilitydbVersion =
        () -> {
            MeosThread.ensureReady();
            return org.mobilitydb.spark.MeosNative.INSTANCE.mobilitydb_version();
        };

    public static final org.apache.spark.sql.api.java.UDF0<String> mobilitydbFullVersion =
        () -> {
            MeosThread.ensureReady();
            return org.mobilitydb.spark.MeosNative.INSTANCE.mobilitydb_full_version();
        };

    // ------------------------------------------------------------------
    // tgeo type conversions
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // valueSet, segmentMin/MaxDuration, box3d (PostGIS embedded in MEOS)
    // ------------------------------------------------------------------

    // valueSet(temporal) → set hex containing distinct values.
    // Mirrors MobilityDB's PG-side Temporal_valueset:
    //   1. temporal_values_p → Datum* + count
    //   2. temptype_basetype(temptype) → MeosType basetype
    //   3. set_make_free → Set* (consumes the Datum* array)
    public static final UDF1<String, String> valueSet =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(hex);
            if (t == null) return null;
            try {
                int temptype = t.getByte(4) & 0xff;
                int basetype = org.mobilitydb.spark.MeosNative.INSTANCE.temptype_basetype(temptype);
                jnr.ffi.Runtime rt = jnr.ffi.Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer values = org.mobilitydb.spark.MeosNative.INSTANCE.temporal_values_p(t, countOut);
                if (values == null) return null;
                int count = countOut.getInt(0);
                Pointer s = org.mobilitydb.spark.MeosNative.INSTANCE
                    .set_make_free(values, count, basetype, false);
                if (s == null) return null;
                try { return functions.set_as_hexwkb(s, (byte) 0); }
                finally { MeosMemory.free(s); }
            } finally { MeosMemory.free(t); }
        };

    // segmentMinDuration(temporal, intervalStr, strict) → temporal sequence-set
    public static final org.apache.spark.sql.api.java.UDF3<String, String, Boolean, String>
        segmentMinDuration = (hex, intervalStr, strict) -> segmDuration(hex, intervalStr, strict, true);

    public static final org.apache.spark.sql.api.java.UDF3<String, String, Boolean, String>
        segmentMaxDuration = (hex, intervalStr, strict) -> segmDuration(hex, intervalStr, strict, false);

    private static String segmDuration(String hex, String intervalStr, Boolean strict, boolean atleast) {
        if (hex == null || intervalStr == null) return null;
        MeosThread.ensureReady();
        Pointer t = functions.temporal_from_hexwkb(hex);
        if (t == null) return null;
        Pointer iv = functions.pg_interval_in(intervalStr, -1);
        if (iv == null) { MeosMemory.free(t); return null; }
        try {
            Pointer r = functions.MeosLibrary.meos.temporal_segm_duration(t, iv, atleast, strict != null && strict);
            if (r == null) return null;
            try { return functions.temporal_as_hexwkb(r, (byte) 0); }
            finally { MeosMemory.free(r); }
        } finally { MeosMemory.free(t, iv); }
    }

    // box2d(stbox) → text representation "BOX(xmin ymin,xmax ymax)"
    // PostGIS BOX2D / GBOX type (embedded in MEOS).
    public static final UDF1<String, String> box2d =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer s = functions.stbox_from_hexwkb(hex);
            if (s == null) return null;
            try {
                Pointer b = functions.stbox_to_gbox(s);
                if (b == null) return null;
                try { return functions.gbox_out(b, 15); }
                finally { MeosMemory.free(b); }
            } finally { MeosMemory.free(s); }
        };

    // box3d(stbox) → text representation "BOX3D(xmin ymin zmin,xmax ymax zmax)"
    // PostGIS BOX3D type (embedded in MEOS).
    public static final UDF1<String, String> box3d =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer s = functions.stbox_from_hexwkb(hex);
            if (s == null) return null;
            try {
                Pointer b = functions.stbox_to_box3d(s);
                if (b == null) return null;
                try { return functions.box3d_out(b, 15); }
                finally { MeosMemory.free(b); }
            } finally { MeosMemory.free(s); }
        };

    public static final UDF1<String, String> tgeometry =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                // Try tgeompoint → tgeometry first; if that fails, try tgeography → tgeometry
                Pointer r = functions.tgeompoint_to_tgeometry(p);
                if (r == null) r = functions.tgeography_to_tgeometry(p);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String> tgeography =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.tgeogpoint_to_tgeography(p);
                if (r == null) r = functions.tgeometry_to_tgeography(p);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };
}
