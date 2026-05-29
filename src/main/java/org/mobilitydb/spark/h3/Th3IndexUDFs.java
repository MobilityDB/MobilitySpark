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

package org.mobilitydb.spark.h3;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Spark SQL UDFs for the temporal H3 index type (th3index) and its supporting
 * static h3index / h3indexset surfaces.  Mirrors the public C API at
 *
 *   meos/include/meos_h3.h         — th3index temporal type (66 fns)
 *   meos/include/h3/h3index.h      — static h3index scalar ops (10 fns)
 *   meos/include/h3/h3index_sets.h — h3indexset (Set of cells) ops (9 fns)
 *
 * The class targets 100% parity with the public h3 API.  Sections below
 * mirror the layout of meos_h3.h for traceability.
 *
 * Storage convention:
 *   tgeompoint / tgeogpoint / th3index → hex-WKB STRING (Temporal hex-WKB)
 *   H3Index                            → BIGINT (uint64 fits in Java long)
 *   h3indexset (Set of H3Index)        → hex-WKB STRING (Set hex-WKB)
 *   TimestampTz                        → java.sql.Timestamp / String
 *   interpType                         → INTEGER (NONE=0, DISCRETE=1,
 *                                                  STEP=2, LINEAR=3)
 *
 * Memory management: every native Pointer allocated by MEOS must be freed
 * via MeosMemory.free() in a finally block.  Pointers returned by JNR-
 * allocated output buffers (the *_value_at_timestamptz / *_value_n forms)
 * have a JNR Cleaner attached and must NOT be MeosMemory.free'd — see
 * feedback_jnr_allocated_buffer_nofree.md.
 */
public final class Th3IndexUDFs {

    private Th3IndexUDFs() {}

    // ==================================================================
    // Helpers — keep the per-UDF code concise
    // ==================================================================

    /** Default H3 resolution for the BerlinMOD prefilter — ~1.2 km cells. */
    public static final int DEFAULT_RESOLUTION = 7;

    private static final DateTimeFormatter PG_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");

    /** Spark Timestamp / String → MEOS OffsetDateTime via pg_timestamptz_in. */
    private static OffsetDateTime parseTs(Object arg) {
        if (arg == null) return null;
        if (arg instanceof java.sql.Timestamp) {
            return GeneratedFunctions.pg_timestamptz_in(
                ((java.sql.Timestamp) arg).toInstant().atOffset(ZoneOffset.UTC).format(PG_FMT), -1);
        }
        return GeneratedFunctions.pg_timestamptz_in(arg.toString().trim(), -1);
    }

    /** Convert a Number arg (Spark sends Int / Long / BigDecimal) to int. */
    private static int toInt(Number n) { return n == null ? 0 : n.intValue(); }

    /** Serialise a Temporal* result as hex-WKB and free the input pointer. */
    private static String tempHex(Pointer t) {
        if (t == null) return null;
        try { return GeneratedFunctions.temporal_as_hexwkb(t, (byte) 0); }
        finally { MeosMemory.free(t); }
    }

    /** Serialise a Set* result as hex-WKB and free the input pointer. */
    private static String setHex(Pointer s) {
        if (s == null) return null;
        try { return GeneratedFunctions.set_as_hexwkb(s, (byte) 0); }
        finally { MeosMemory.free(s); }
    }

    // ==================================================================
    // Static h3index SQL type — meos/include/h3/h3index.h
    // ==================================================================

    public static final UDF1<String, Long> h3IndexFromText = (s) -> {
        if (s == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_in(s);
    };

    public static final UDF1<Long, String> h3IndexAsText = (cell) -> {
        if (cell == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_out(cell);
    };

    public static final UDF1<String, Long> h3IndexParse = (s) -> {
        if (s == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_parse(s);
    };

    public static final UDF1<Long, String> h3IndexToString = (cell) -> {
        if (cell == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_to_string(cell);
    };

    public static final UDF2<Long, Long, Boolean> h3IndexEq = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_eq(a, b);
    };

    public static final UDF2<Long, Long, Boolean> h3IndexNe = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_ne(a, b);
    };

    public static final UDF2<Long, Long, Boolean> h3IndexLt = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_lt(a, b);
    };

    public static final UDF2<Long, Long, Boolean> h3IndexLe = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_le(a, b);
    };

    public static final UDF2<Long, Long, Boolean> h3IndexGt = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_gt(a, b);
    };

    public static final UDF2<Long, Long, Boolean> h3IndexGe = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_ge(a, b);
    };

    public static final UDF2<Long, Long, Integer> h3IndexCmp = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        return GeneratedFunctions.h3index_cmp(a, b);
    };

    public static final UDF1<Long, Long> h3IndexHash = (cell) -> {
        if (cell == null) return null;
        MeosThread.ensureReady();
        return (long) GeneratedFunctions.h3index_hash(cell);
    };

    // ==================================================================
    // h3indexset (Set of H3Index) — meos/include/h3/h3index_sets.h
    // All return Set* serialised as hex-WKB STRING.
    // ==================================================================

    public static final UDF2<Long, Integer, String> h3GridDisk = (origin, k) -> {
        if (origin == null || k == null) return null;
        MeosThread.ensureReady();
        return setHex(GeneratedFunctions.h3_grid_disk(origin, k));
    };

    public static final UDF2<Long, Integer, String> h3GridRing = (origin, k) -> {
        if (origin == null || k == null) return null;
        MeosThread.ensureReady();
        return setHex(GeneratedFunctions.h3_grid_ring(origin, k));
    };

    public static final UDF2<Long, Long, String> h3GridPathCells = (start, end) -> {
        if (start == null || end == null) return null;
        MeosThread.ensureReady();
        return setHex(GeneratedFunctions.h3_grid_path_cells(start, end));
    };

    public static final UDF2<Long, Integer, String> h3CellToChildren = (origin, childRes) -> {
        if (origin == null || childRes == null) return null;
        MeosThread.ensureReady();
        return setHex(GeneratedFunctions.h3_cell_to_children(origin, childRes));
    };

    public static final UDF1<String, String> h3CompactCells = (cellsHex) -> {
        if (cellsHex == null) return null;
        MeosThread.ensureReady();
        Pointer in = GeneratedFunctions.set_from_hexwkb(cellsHex);
        if (in == null) return null;
        try { return setHex(GeneratedFunctions.h3_compact_cells(in)); }
        finally { MeosMemory.free(in); }
    };

    public static final UDF2<String, Integer, String> h3UncompactCells = (cellsHex, res) -> {
        if (cellsHex == null || res == null) return null;
        MeosThread.ensureReady();
        Pointer in = GeneratedFunctions.set_from_hexwkb(cellsHex);
        if (in == null) return null;
        try { return setHex(GeneratedFunctions.h3_uncompact_cells(in, res)); }
        finally { MeosMemory.free(in); }
    };

    public static final UDF1<Long, String> h3OriginToDirectedEdges = (origin) -> {
        if (origin == null) return null;
        MeosThread.ensureReady();
        return setHex(GeneratedFunctions.h3_origin_to_directed_edges(origin));
    };

    public static final UDF1<Long, String> h3CellToVertexes = (cell) -> {
        if (cell == null) return null;
        MeosThread.ensureReady();
        return setHex(GeneratedFunctions.h3_cell_to_vertexes(cell));
    };

    public static final UDF1<Long, String> h3GetIcosahedronFaces = (cell) -> {
        if (cell == null) return null;
        MeosThread.ensureReady();
        return setHex(GeneratedFunctions.h3_get_icosahedron_faces(cell));
    };

    // ==================================================================
    // th3index input / output — meos_h3.h "Type inheritance"
    // ==================================================================

    public static final UDF1<String, String> th3IndexFromText = (s) -> {
        if (s == null) return null;
        MeosThread.ensureReady();
        return tempHex(GeneratedFunctions.th3index_in(s));
    };

    public static final UDF1<String, String> th3IndexInstFromText = (s) -> {
        if (s == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.th3indexinst_in(s);
        if (p == null) return null;
        try { return GeneratedFunctions.temporal_as_hexwkb(p, (byte) 0); }
        finally { MeosMemory.free(p); }
    };

    public static final UDF2<String, Integer, String> th3IndexSeqFromText = (s, interp) -> {
        if (s == null || interp == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.th3indexseq_in(s, interp);
        if (p == null) return null;
        try { return GeneratedFunctions.temporal_as_hexwkb(p, (byte) 0); }
        finally { MeosMemory.free(p); }
    };

    public static final UDF1<String, String> th3IndexSeqSetFromText = (s) -> {
        if (s == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.th3indexseqset_in(s);
        if (p == null) return null;
        try { return GeneratedFunctions.temporal_as_hexwkb(p, (byte) 0); }
        finally { MeosMemory.free(p); }
    };

    // ==================================================================
    // th3index constructors — meos_h3.h "Constructors"
    //
    // The instant + scalar make() forms are scalar-arg.  The seq / seqset
    // forms accept arrays which are not exposed here — callers can compose
    // by parsing instants and concatenating via temporal_merge / temporal_seq.
    // ==================================================================

    public static final UDF2<Long, Object, String> th3IndexMake = (cell, tsArg) -> {
        if (cell == null || tsArg == null) return null;
        MeosThread.ensureReady();
        OffsetDateTime t = parseTs(tsArg);
        return tempHex(GeneratedFunctions.th3index_make(cell, t));
    };

    public static final UDF2<Long, Object, String> th3IndexInstMake = (cell, tsArg) -> {
        if (cell == null || tsArg == null) return null;
        MeosThread.ensureReady();
        OffsetDateTime t = parseTs(tsArg);
        Pointer p = GeneratedFunctions.th3indexinst_make(cell, t);
        if (p == null) return null;
        try { return GeneratedFunctions.temporal_as_hexwkb(p, (byte) 0); }
        finally { MeosMemory.free(p); }
    };

    /**
     * th3indexseq_make(values[], times[], count, lower_inc, upper_inc) → TSequence hex-WKB.
     *
     * Spark passes parallel ARRAY<LONG> + ARRAY<TIMESTAMP>; we marshal both
     * to JNR-FFI native arrays and call the MEOS constructor.  The two arrays
     * must have the same length; count is inferred from the value array.
     */
    public static final UDF5<long[], Object[], Boolean, Boolean, Object, String> th3IndexSeqMake =
        (values, timestamps, lowerInc, upperInc, ignored) -> {
            if (values == null || timestamps == null) return null;
            if (values.length != timestamps.length) return null;
            MeosThread.ensureReady();
            int n = values.length;
            Runtime rt = Runtime.getSystemRuntime();
            Pointer valueBuf = rt.getMemoryManager().allocateDirect(8L * n);
            Pointer timeBuf = rt.getMemoryManager().allocateDirect(8L * n);
            for (int i = 0; i < n; i++) {
                valueBuf.putLong(i * 8L, values[i]);
                OffsetDateTime t = parseTs(timestamps[i]);
                if (t == null) return null;
                /* TimestampTz = microseconds since 2000-01-01 UTC. */
                long micros = (t.toEpochSecond() - 946684800L) * 1_000_000L
                    + t.getNano() / 1_000L;
                timeBuf.putLong(i * 8L, micros);
            }
            Pointer p = GeneratedFunctions.th3indexseq_make(
                valueBuf, timeBuf, n,
                lowerInc != null && lowerInc,
                upperInc != null && upperInc);
            if (p == null) return null;
            try { return GeneratedFunctions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    /**
     * th3indexseqset_make(sequences[]) → TSequenceSet hex-WKB.
     *
     * Spark passes ARRAY<STRING> of hex-WKB TSequence; we parse each into a
     * native Pointer, hand the array to the MEOS constructor, free the
     * intermediate pointers.
     */
    public static final UDF1<String[], String> th3IndexSeqSetMake = (sequencesHex) -> {
        if (sequencesHex == null) return null;
        MeosThread.ensureReady();
        int n = sequencesHex.length;
        Pointer[] seqs = new Pointer[n];
        try {
            for (int i = 0; i < n; i++) {
                seqs[i] = GeneratedFunctions.temporal_from_hexwkb(sequencesHex[i]);
                if (seqs[i] == null) return null;
            }
            Pointer buf = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8L * n);
            for (int i = 0; i < n; i++) {
                buf.putAddress(i * 8L, seqs[i].address());
            }
            Pointer p = GeneratedFunctions.th3indexseqset_make(buf, n);
            if (p == null) return null;
            try { return GeneratedFunctions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        } finally {
            for (Pointer s : seqs) if (s != null) MeosMemory.free(s);
        }
    };

    // ==================================================================
    // Accessors — meos_h3.h "Accessors"
    // ==================================================================

    public static final UDF1<String, Long> th3IndexStartValue = (th3idx) -> {
        if (th3idx == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try { return GeneratedFunctions.th3index_start_value(t); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF1<String, Long> th3IndexEndValue = (th3idx) -> {
        if (th3idx == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try { return GeneratedFunctions.th3index_end_value(t); }
        finally { MeosMemory.free(t); }
    };

    /**
     * th3index_values(th3idx) → ARRAY&lt;LONG&gt; (all distinct H3 cells in the trip's path).
     *
     * MEOS signature: H3Index *th3index_values(const Temporal *temp, int *count);
     * the H3Index buffer is owned by MEOS and must be freed by the caller.
     */
    public static final UDF1<String, long[]> th3IndexValues = (th3idx) -> {
        if (th3idx == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try {
            Pointer countOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
            Pointer arr = GeneratedFunctions.th3index_values(t, countOut);
            if (arr == null) return null;
            int count = countOut.getInt(0);
            long[] out = new long[count];
            for (int i = 0; i < count; i++) out[i] = arr.getLong((long) i * 8);
            MeosMemory.free(arr);
            return out;
        } finally { MeosMemory.free(t); }
    };

    /**
     * th3index_value_n(th3idx, n) → H3Index.
     * MEOS signature: bool th3index_value_n(const Temporal *, int n, H3Index *result).
     * JMEOS auto-allocates the H3Index output buffer; we read the value and
     * let the JNR Cleaner reclaim it (do NOT MeosMemory.free).
     */
    public static final UDF2<String, Integer, Long> th3IndexValueN = (th3idx, n) -> {
        if (th3idx == null || n == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try {
            Pointer result = GeneratedFunctions.th3index_value_n(t, n);
            return result == null ? null : result.getLong(0);
        } finally {
            MeosMemory.free(t);
        }
    };

    /**
     * th3index_value_at_timestamptz(th3idx, ts, strict) → H3Index.
     * MEOS signature: bool th3index_value_at_timestamptz(const Temporal *,
     *                                                     TimestampTz, bool, H3Index*).
     * JMEOS auto-allocates the output; treat the returned Pointer as a JNR
     * buffer per feedback_jnr_allocated_buffer_nofree.md.
     */
    public static final UDF3<String, Object, Boolean, Long> th3IndexValueAtTimestamp =
        (th3idx, tsArg, strict) -> {
            if (th3idx == null || tsArg == null) return null;
            MeosThread.ensureReady();
            OffsetDateTime ts = parseTs(tsArg);
            Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
            if (t == null) return null;
            try {
                Pointer result = GeneratedFunctions.th3index_value_at_timestamptz(
                    t, ts, strict != null && strict);
                return result == null ? null : result.getLong(0);
            } finally {
                MeosMemory.free(t);
            }
        };

    // ==================================================================
    // MEOS-level conversions — meos_h3.h "MEOS-level conversions"
    // ==================================================================

    public static final UDF1<String, String> tbigintToTh3Index = (tbi) -> {
        if (tbi == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(tbi);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.tbigint_to_th3index(t)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF1<String, String> th3IndexToTbigint = (th3idx) -> {
        if (th3idx == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.th3index_to_tbigint(t)); }
        finally { MeosMemory.free(t); }
    };

    // ==================================================================
    // Ever / always comparison operators — meos_h3.h
    // ==================================================================

    public static final UDF2<Long, String, Boolean> everEqH3IndexTh3Index =
        (cell, th3idx) -> evCmp(cell, th3idx, true,  "ever_eq");
    public static final UDF2<String, Long, Boolean> everEqTh3IndexH3Index =
        (th3idx, cell) -> evCmp(cell, th3idx, true,  "ever_eq_t");
    public static final UDF2<Long, String, Boolean> everNeH3IndexTh3Index =
        (cell, th3idx) -> evCmp(cell, th3idx, true,  "ever_ne");
    public static final UDF2<String, Long, Boolean> everNeTh3IndexH3Index =
        (th3idx, cell) -> evCmp(cell, th3idx, true,  "ever_ne_t");
    public static final UDF2<Long, String, Boolean> alwaysEqH3IndexTh3Index =
        (cell, th3idx) -> evCmp(cell, th3idx, false, "always_eq");
    public static final UDF2<String, Long, Boolean> alwaysEqTh3IndexH3Index =
        (th3idx, cell) -> evCmp(cell, th3idx, false, "always_eq_t");
    public static final UDF2<Long, String, Boolean> alwaysNeH3IndexTh3Index =
        (cell, th3idx) -> evCmp(cell, th3idx, false, "always_ne");
    public static final UDF2<String, Long, Boolean> alwaysNeTh3IndexH3Index =
        (th3idx, cell) -> evCmp(cell, th3idx, false, "always_ne_t");

    /** Dispatch helper for the 8 ever/always × eq/ne × cell-side variants. */
    private static Boolean evCmp(Long cell, String th3idx, boolean ever, String op) {
        if (cell == null || th3idx == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try {
            int r;
            switch (op) {
                case "ever_eq":     r = GeneratedFunctions.ever_eq_h3index_th3index(cell, t); break;
                case "ever_eq_t":   r = GeneratedFunctions.ever_eq_th3index_h3index(t, cell); break;
                case "ever_ne":     r = GeneratedFunctions.ever_ne_h3index_th3index(cell, t); break;
                case "ever_ne_t":   r = GeneratedFunctions.ever_ne_th3index_h3index(t, cell); break;
                case "always_eq":   r = GeneratedFunctions.always_eq_h3index_th3index(cell, t); break;
                case "always_eq_t": r = GeneratedFunctions.always_eq_th3index_h3index(t, cell); break;
                case "always_ne":   r = GeneratedFunctions.always_ne_h3index_th3index(cell, t); break;
                case "always_ne_t": r = GeneratedFunctions.always_ne_th3index_h3index(t, cell); break;
                default: throw new IllegalStateException(op);
            }
            return r < 0 ? null : r == 1;
        } finally {
            MeosMemory.free(t);
        }
    }

    public static final UDF2<String, String, Boolean> everEqTh3IndexTh3Index =
        (a, b) -> ttCmp(a, b, "ever_eq");
    public static final UDF2<String, String, Boolean> everNeTh3IndexTh3Index =
        (a, b) -> ttCmp(a, b, "ever_ne");
    public static final UDF2<String, String, Boolean> alwaysEqTh3IndexTh3Index =
        (a, b) -> ttCmp(a, b, "always_eq");
    public static final UDF2<String, String, Boolean> alwaysNeTh3IndexTh3Index =
        (a, b) -> ttCmp(a, b, "always_ne");

    /** Dispatch helper for the 4 trip×trip ever/always × eq/ne variants. */
    private static Boolean ttCmp(String a, String b, String op) {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.temporal_from_hexwkb(a);
        if (p == null) return null;
        try {
            Pointer q = GeneratedFunctions.temporal_from_hexwkb(b);
            if (q == null) return null;
            try {
                int r;
                switch (op) {
                    case "ever_eq":   r = GeneratedFunctions.ever_eq_th3index_th3index(p, q); break;
                    case "ever_ne":   r = GeneratedFunctions.ever_ne_th3index_th3index(p, q); break;
                    case "always_eq": r = GeneratedFunctions.always_eq_th3index_th3index(p, q); break;
                    case "always_ne": r = GeneratedFunctions.always_ne_th3index_th3index(p, q); break;
                    default: throw new IllegalStateException(op);
                }
                return r < 0 ? null : r == 1;
            } finally {
                MeosMemory.free(q);
            }
        } finally {
            MeosMemory.free(p);
        }
    }

    // ==================================================================
    // Temporal comparison operators — meos_h3.h
    // Return tbool serialised as hex-WKB.
    // ==================================================================

    public static final UDF2<Long, String, String> teqH3IndexTh3Index = (cell, th3idx) -> {
        if (cell == null || th3idx == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.teq_h3index_th3index(cell, t)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF2<String, Long, String> teqTh3IndexH3Index = (th3idx, cell) -> {
        if (th3idx == null || cell == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.teq_th3index_h3index(t, cell)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF2<String, String, String> teqTh3IndexTh3Index = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.temporal_from_hexwkb(a);
        if (p == null) return null;
        try {
            Pointer q = GeneratedFunctions.temporal_from_hexwkb(b);
            if (q == null) return null;
            try { return tempHex(GeneratedFunctions.teq_th3index_th3index(p, q)); }
            finally { MeosMemory.free(q); }
        } finally { MeosMemory.free(p); }
    };

    public static final UDF2<Long, String, String> tneH3IndexTh3Index = (cell, th3idx) -> {
        if (cell == null || th3idx == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.tne_h3index_th3index(cell, t)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF2<String, Long, String> tneTh3IndexH3Index = (th3idx, cell) -> {
        if (th3idx == null || cell == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.tne_th3index_h3index(t, cell)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF2<String, String, String> tneTh3IndexTh3Index = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.temporal_from_hexwkb(a);
        if (p == null) return null;
        try {
            Pointer q = GeneratedFunctions.temporal_from_hexwkb(b);
            if (q == null) return null;
            try { return tempHex(GeneratedFunctions.tne_th3index_th3index(p, q)); }
            finally { MeosMemory.free(q); }
        } finally { MeosMemory.free(p); }
    };

    // ==================================================================
    // Inspection — meos_h3.h
    // All return Temporal* (tint or tbool) serialised as hex-WKB.
    // ==================================================================

    public static final UDF1<String, String> th3IndexGetResolution =
        (h) -> tempUnary(h, "get_resolution");
    public static final UDF1<String, String> th3IndexGetBaseCellNumber =
        (h) -> tempUnary(h, "get_base_cell_number");
    public static final UDF1<String, String> th3IndexIsValidCell =
        (h) -> tempUnary(h, "is_valid_cell");
    public static final UDF1<String, String> th3IndexIsResClassIii =
        (h) -> tempUnary(h, "is_res_class_iii");
    public static final UDF1<String, String> th3IndexIsPentagon =
        (h) -> tempUnary(h, "is_pentagon");

    /** Dispatch helper for unary Temporal* → Temporal* th3index inspections. */
    private static String tempUnary(String h, String op) {
        if (h == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(h);
        if (t == null) return null;
        try {
            Pointer r;
            switch (op) {
                case "get_resolution":      r = GeneratedFunctions.th3index_get_resolution(t); break;
                case "get_base_cell_number":r = GeneratedFunctions.th3index_get_base_cell_number(t); break;
                case "is_valid_cell":       r = GeneratedFunctions.th3index_is_valid_cell(t); break;
                case "is_res_class_iii":    r = GeneratedFunctions.th3index_is_res_class_iii(t); break;
                case "is_pentagon":         r = GeneratedFunctions.th3index_is_pentagon(t); break;
                case "cell_to_parent_next": r = GeneratedFunctions.th3index_cell_to_parent_next(t); break;
                case "cell_to_center_child_next":
                                            r = GeneratedFunctions.th3index_cell_to_center_child_next(t); break;
                case "is_valid_directed_edge":
                                            r = GeneratedFunctions.th3index_is_valid_directed_edge(t); break;
                case "get_directed_edge_origin":
                                            r = GeneratedFunctions.th3index_get_directed_edge_origin(t); break;
                case "get_directed_edge_destination":
                                            r = GeneratedFunctions.th3index_get_directed_edge_destination(t); break;
                case "directed_edge_to_boundary":
                                            r = GeneratedFunctions.th3index_directed_edge_to_boundary(t); break;
                case "vertex_to_latlng":    r = GeneratedFunctions.th3index_vertex_to_latlng(t); break;
                case "is_valid_vertex":     r = GeneratedFunctions.th3index_is_valid_vertex(t); break;
                case "to_tgeogpoint":       r = GeneratedFunctions.th3index_to_tgeogpoint(t); break;
                case "to_tgeompoint":       r = GeneratedFunctions.th3index_to_tgeompoint(t); break;
                case "cell_to_boundary":    r = GeneratedFunctions.th3index_cell_to_boundary(t); break;
                default: throw new IllegalStateException(op);
            }
            return tempHex(r);
        } finally {
            MeosMemory.free(t);
        }
    }

    // ==================================================================
    // Hierarchy — meos_h3.h
    // ==================================================================

    public static final UDF2<String, Integer, String> th3IndexCellToParent = (h, res) -> {
        if (h == null || res == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(h);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.th3index_cell_to_parent(t, res)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF1<String, String> th3IndexCellToParentNext =
        (h) -> tempUnary(h, "cell_to_parent_next");

    public static final UDF2<String, Integer, String> th3IndexCellToCenterChild = (h, res) -> {
        if (h == null || res == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(h);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.th3index_cell_to_center_child(t, res)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF1<String, String> th3IndexCellToCenterChildNext =
        (h) -> tempUnary(h, "cell_to_center_child_next");

    public static final UDF2<String, Integer, String> th3IndexCellToChildPos = (h, parentRes) -> {
        if (h == null || parentRes == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(h);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.th3index_cell_to_child_pos(t, parentRes)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF3<String, String, Integer, String> th3IndexChildPosToCell =
        (childPos, parent, childRes) -> {
            if (childPos == null || parent == null || childRes == null) return null;
            MeosThread.ensureReady();
            Pointer cp = GeneratedFunctions.temporal_from_hexwkb(childPos);
            if (cp == null) return null;
            try {
                Pointer pa = GeneratedFunctions.temporal_from_hexwkb(parent);
                if (pa == null) return null;
                try { return tempHex(GeneratedFunctions.th3index_child_pos_to_cell(cp, pa, childRes)); }
                finally { MeosMemory.free(pa); }
            } finally {
                MeosMemory.free(cp);
            }
        };

    // ==================================================================
    // Lat/Lng conversion — meos_h3.h
    // ==================================================================

    public static final UDF2<String, Integer, String> tgeogpointToTh3Index = (h, res) -> {
        if (h == null || res == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(h);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.tgeogpoint_to_th3index(t, res)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF2<String, Integer, String> tgeompointToTh3Index = (h, res) -> {
        if (h == null || res == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(h);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.tgeompoint_to_th3index(t, res)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF1<String, String> th3IndexToTgeogpoint =
        (h) -> tempUnary(h, "to_tgeogpoint");
    public static final UDF1<String, String> th3IndexToTgeompoint =
        (h) -> tempUnary(h, "to_tgeompoint");
    public static final UDF1<String, String> th3IndexCellToBoundary =
        (h) -> tempUnary(h, "cell_to_boundary");

    /**
     * geomToH3Cell(geomWkt, resolution) → H3Index.
     *
     * Composed from the public API: a static POINT geometry is wrapped in a
     * single-instant tgeompoint, converted to th3index, and the start value is
     * extracted as the H3Index.  Returns NULL for non-POINT geometries (the
     * prefilter consumer treats NULL as "no prefilter for this row").  Polygon
     * coverage requires the upstream geo_to_h3index_set helper (separate PR).
     */
    public static final UDF2<String, Integer, Long> geomToH3Cell =
        (geomWkt, resolution) -> {
            if (geomWkt == null || resolution == null) return null;
            MeosThread.ensureReady();
            Pointer gs = GeneratedFunctions.geo_from_text(geomWkt, 0);
            if (gs == null) return null;
            try {
                Pointer inst = GeneratedFunctions.tpointinst_make(gs,
                    OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
                if (inst == null) return null;
                try {
                    Pointer th3 = GeneratedFunctions.tgeompoint_to_th3index(inst, resolution);
                    if (th3 == null) return null;
                    try { return GeneratedFunctions.th3index_start_value(th3); }
                    finally { MeosMemory.free(th3); }
                } finally {
                    MeosMemory.free(inst);
                }
            } finally {
                MeosMemory.free(gs);
            }
        };

    /**
     * geoToH3IndexSet(geomWkt, resolution) → STRING (hex-WKB h3indexset).
     *
     * Cross-platform spatial prefilter source for polygon-side queries
     * (BerlinMOD Q2 et al.).  Handles every WKT geometry type — POINT,
     * LINESTRING, POLYGON, MULTI*, GEOMETRYCOLLECTION — via the public
     * MEOS kernel geo_to_h3index_set (MobilityDB PR #938).
     */
    public static final UDF2<String, Integer, String> geoToH3IndexSet =
        (geomWkt, resolution) -> {
            if (geomWkt == null || resolution == null) return null;
            MeosThread.ensureReady();
            Pointer gs = GeneratedFunctions.geo_from_text(geomWkt, 0);
            if (gs == null) return null;
            try {
                Pointer set = GeneratedFunctions.geo_to_h3index_set(gs, resolution);
                return setHex(set);
            } finally {
                MeosMemory.free(gs);
            }
        };

    /**
     * everIntersectsH3IndexSetTh3Index(cellSetHex, th3idx) → BOOLEAN.
     *
     * Returns TRUE iff the trip's th3index sequence ever lies in any cell
     * of the candidate set.  Pair with geoToH3IndexSet to prefilter
     * polygon-side cross-join queries.  Wraps
     * ever_eq_h3indexset_th3index (MobilityDB PR #938).
     */
    public static final UDF2<String, String, Boolean> everIntersectsH3IndexSetTh3Index =
        (cellSetHex, th3idx) -> {
            if (cellSetHex == null || th3idx == null) return null;
            MeosThread.ensureReady();
            Pointer cells = GeneratedFunctions.set_from_hexwkb(cellSetHex);
            if (cells == null) return null;
            try {
                Pointer t = GeneratedFunctions.temporal_from_hexwkb(th3idx);
                if (t == null) return null;
                try {
                    int r = GeneratedFunctions.ever_eq_h3indexset_th3index(cells, t);
                    return r < 0 ? null : r == 1;
                } finally {
                    MeosMemory.free(t);
                }
            } finally {
                MeosMemory.free(cells);
            }
        };

    // ==================================================================
    // Directed edges — meos_h3.h
    // ==================================================================

    public static final UDF2<String, String, String> th3IndexAreNeighborCells = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.temporal_from_hexwkb(a);
        if (p == null) return null;
        try {
            Pointer q = GeneratedFunctions.temporal_from_hexwkb(b);
            if (q == null) return null;
            try { return tempHex(GeneratedFunctions.th3index_are_neighbor_cells(p, q)); }
            finally { MeosMemory.free(q); }
        } finally { MeosMemory.free(p); }
    };

    public static final UDF2<String, String, String> th3IndexCellsToDirectedEdge = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.temporal_from_hexwkb(a);
        if (p == null) return null;
        try {
            Pointer q = GeneratedFunctions.temporal_from_hexwkb(b);
            if (q == null) return null;
            try { return tempHex(GeneratedFunctions.th3index_cells_to_directed_edge(p, q)); }
            finally { MeosMemory.free(q); }
        } finally { MeosMemory.free(p); }
    };

    public static final UDF1<String, String> th3IndexIsValidDirectedEdge =
        (h) -> tempUnary(h, "is_valid_directed_edge");
    public static final UDF1<String, String> th3IndexGetDirectedEdgeOrigin =
        (h) -> tempUnary(h, "get_directed_edge_origin");
    public static final UDF1<String, String> th3IndexGetDirectedEdgeDestination =
        (h) -> tempUnary(h, "get_directed_edge_destination");
    public static final UDF1<String, String> th3IndexDirectedEdgeToBoundary =
        (h) -> tempUnary(h, "directed_edge_to_boundary");

    // ==================================================================
    // Vertices — meos_h3.h
    // ==================================================================

    public static final UDF2<String, Integer, String> th3IndexCellToVertex = (h, vertexNum) -> {
        if (h == null || vertexNum == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(h);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.th3index_cell_to_vertex(t, vertexNum)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF1<String, String> th3IndexVertexToLatlng =
        (h) -> tempUnary(h, "vertex_to_latlng");
    public static final UDF1<String, String> th3IndexIsValidVertex =
        (h) -> tempUnary(h, "is_valid_vertex");

    // ==================================================================
    // Grid traversal — meos_h3.h
    // ==================================================================

    public static final UDF2<String, String, String> th3IndexGridDistance = (a, b) -> {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.temporal_from_hexwkb(a);
        if (p == null) return null;
        try {
            Pointer q = GeneratedFunctions.temporal_from_hexwkb(b);
            if (q == null) return null;
            try { return tempHex(GeneratedFunctions.th3index_grid_distance(p, q)); }
            finally { MeosMemory.free(q); }
        } finally { MeosMemory.free(p); }
    };

    public static final UDF2<String, String, String> th3IndexCellToLocalIj = (origin, cell) -> {
        if (origin == null || cell == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.temporal_from_hexwkb(origin);
        if (p == null) return null;
        try {
            Pointer q = GeneratedFunctions.temporal_from_hexwkb(cell);
            if (q == null) return null;
            try { return tempHex(GeneratedFunctions.th3index_cell_to_local_ij(p, q)); }
            finally { MeosMemory.free(q); }
        } finally { MeosMemory.free(p); }
    };

    public static final UDF2<String, String, String> th3IndexLocalIjToCell = (origin, coord) -> {
        if (origin == null || coord == null) return null;
        MeosThread.ensureReady();
        Pointer p = GeneratedFunctions.temporal_from_hexwkb(origin);
        if (p == null) return null;
        try {
            Pointer q = GeneratedFunctions.temporal_from_hexwkb(coord);
            if (q == null) return null;
            try { return tempHex(GeneratedFunctions.th3index_local_ij_to_cell(p, q)); }
            finally { MeosMemory.free(q); }
        } finally { MeosMemory.free(p); }
    };

    // ==================================================================
    // Metrics — meos_h3.h
    // ==================================================================

    public static final UDF2<String, String, String> th3IndexCellArea = (h, unit) -> {
        if (h == null || unit == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(h);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.th3index_cell_area(t, unit)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF2<String, String, String> th3IndexEdgeLength = (h, unit) -> {
        if (h == null || unit == null) return null;
        MeosThread.ensureReady();
        Pointer t = GeneratedFunctions.temporal_from_hexwkb(h);
        if (t == null) return null;
        try { return tempHex(GeneratedFunctions.th3index_edge_length(t, unit)); }
        finally { MeosMemory.free(t); }
    };

    public static final UDF3<String, String, String, String> tgeogpointGreatCircleDistance =
        (a, b, unit) -> {
            if (a == null || b == null || unit == null) return null;
            MeosThread.ensureReady();
            Pointer p = GeneratedFunctions.temporal_from_hexwkb(a);
            if (p == null) return null;
            try {
                Pointer q = GeneratedFunctions.temporal_from_hexwkb(b);
                if (q == null) return null;
                try { return tempHex(GeneratedFunctions.tgeogpoint_great_circle_distance(p, q, unit)); }
                finally { MeosMemory.free(q); }
            } finally { MeosMemory.free(p); }
        };

    // ==================================================================
    // Registration — exposes every UDF above to Spark SQL.
    // ==================================================================

    public static void registerAll(SparkSession spark) {
        // Static h3index ops
        spark.udf().register("h3IndexFromText",     h3IndexFromText,    DataTypes.LongType);
        spark.udf().register("h3IndexAsText",       h3IndexAsText,      DataTypes.StringType);
        spark.udf().register("h3IndexParse",        h3IndexParse,       DataTypes.LongType);
        spark.udf().register("h3IndexToString",     h3IndexToString,    DataTypes.StringType);
        spark.udf().register("h3IndexEq",           h3IndexEq,          DataTypes.BooleanType);
        spark.udf().register("h3IndexNe",           h3IndexNe,          DataTypes.BooleanType);
        spark.udf().register("h3IndexLt",           h3IndexLt,          DataTypes.BooleanType);
        spark.udf().register("h3IndexLe",           h3IndexLe,          DataTypes.BooleanType);
        spark.udf().register("h3IndexGt",           h3IndexGt,          DataTypes.BooleanType);
        spark.udf().register("h3IndexGe",           h3IndexGe,          DataTypes.BooleanType);
        spark.udf().register("h3IndexCmp",          h3IndexCmp,         DataTypes.IntegerType);
        spark.udf().register("h3IndexHash",         h3IndexHash,        DataTypes.LongType);

        // h3indexset (Set ops)
        spark.udf().register("h3GridDisk",          h3GridDisk,         DataTypes.StringType);
        spark.udf().register("h3GridRing",          h3GridRing,         DataTypes.StringType);
        spark.udf().register("h3GridPathCells",     h3GridPathCells,    DataTypes.StringType);
        spark.udf().register("h3CellToChildren",    h3CellToChildren,   DataTypes.StringType);
        spark.udf().register("h3CompactCells",      h3CompactCells,     DataTypes.StringType);
        spark.udf().register("h3UncompactCells",    h3UncompactCells,   DataTypes.StringType);
        spark.udf().register("h3OriginToDirectedEdges",
                             h3OriginToDirectedEdges,                   DataTypes.StringType);
        spark.udf().register("h3CellToVertexes",    h3CellToVertexes,   DataTypes.StringType);
        spark.udf().register("h3GetIcosahedronFaces",
                             h3GetIcosahedronFaces,                     DataTypes.StringType);

        // th3index I/O + constructors
        spark.udf().register("th3IndexFromText",       th3IndexFromText,       DataTypes.StringType);
        spark.udf().register("th3IndexInstFromText",   th3IndexInstFromText,   DataTypes.StringType);
        spark.udf().register("th3IndexSeqFromText",    th3IndexSeqFromText,    DataTypes.StringType);
        spark.udf().register("th3IndexSeqSetFromText", th3IndexSeqSetFromText, DataTypes.StringType);
        spark.udf().register("th3IndexMake",           th3IndexMake,           DataTypes.StringType);
        spark.udf().register("th3IndexInstMake",       th3IndexInstMake,       DataTypes.StringType);
        spark.udf().register("th3IndexSeqMake",        th3IndexSeqMake,        DataTypes.StringType);
        spark.udf().register("th3IndexSeqSetMake",     th3IndexSeqSetMake,     DataTypes.StringType);

        // Accessors
        spark.udf().register("th3IndexStartValue",        th3IndexStartValue,
                             DataTypes.LongType);
        spark.udf().register("th3IndexEndValue",          th3IndexEndValue,
                             DataTypes.LongType);
        spark.udf().register("th3IndexValues",            th3IndexValues,
                             DataTypes.createArrayType(DataTypes.LongType));
        spark.udf().register("th3IndexValueN",            th3IndexValueN,
                             DataTypes.LongType);
        spark.udf().register("th3IndexValueAtTimestamp",  th3IndexValueAtTimestamp,
                             DataTypes.LongType);

        // Conversions
        spark.udf().register("tbigintToTh3Index",   tbigintToTh3Index,  DataTypes.StringType);
        spark.udf().register("th3IndexToTbigint",   th3IndexToTbigint,  DataTypes.StringType);

        // Ever / always (cell-side variants)
        spark.udf().register("everEqH3IndexTh3Index",     everEqH3IndexTh3Index,
                             DataTypes.BooleanType);
        spark.udf().register("everEqTh3IndexH3Index",     everEqTh3IndexH3Index,
                             DataTypes.BooleanType);
        spark.udf().register("everNeH3IndexTh3Index",     everNeH3IndexTh3Index,
                             DataTypes.BooleanType);
        spark.udf().register("everNeTh3IndexH3Index",     everNeTh3IndexH3Index,
                             DataTypes.BooleanType);
        spark.udf().register("alwaysEqH3IndexTh3Index",   alwaysEqH3IndexTh3Index,
                             DataTypes.BooleanType);
        spark.udf().register("alwaysEqTh3IndexH3Index",   alwaysEqTh3IndexH3Index,
                             DataTypes.BooleanType);
        spark.udf().register("alwaysNeH3IndexTh3Index",   alwaysNeH3IndexTh3Index,
                             DataTypes.BooleanType);
        spark.udf().register("alwaysNeTh3IndexH3Index",   alwaysNeTh3IndexH3Index,
                             DataTypes.BooleanType);

        // Ever / always (trip × trip variants)
        spark.udf().register("everEqTh3IndexTh3Index",    everEqTh3IndexTh3Index,
                             DataTypes.BooleanType);
        spark.udf().register("everNeTh3IndexTh3Index",    everNeTh3IndexTh3Index,
                             DataTypes.BooleanType);
        spark.udf().register("alwaysEqTh3IndexTh3Index",  alwaysEqTh3IndexTh3Index,
                             DataTypes.BooleanType);
        spark.udf().register("alwaysNeTh3IndexTh3Index",  alwaysNeTh3IndexTh3Index,
                             DataTypes.BooleanType);

        // Temporal comparisons
        spark.udf().register("teqH3IndexTh3Index",  teqH3IndexTh3Index,  DataTypes.StringType);
        spark.udf().register("teqTh3IndexH3Index",  teqTh3IndexH3Index,  DataTypes.StringType);
        spark.udf().register("teqTh3IndexTh3Index", teqTh3IndexTh3Index, DataTypes.StringType);
        spark.udf().register("tneH3IndexTh3Index",  tneH3IndexTh3Index,  DataTypes.StringType);
        spark.udf().register("tneTh3IndexH3Index",  tneTh3IndexH3Index,  DataTypes.StringType);
        spark.udf().register("tneTh3IndexTh3Index", tneTh3IndexTh3Index, DataTypes.StringType);

        // Inspection
        spark.udf().register("th3IndexGetResolution",     th3IndexGetResolution,
                             DataTypes.StringType);
        spark.udf().register("th3IndexGetBaseCellNumber", th3IndexGetBaseCellNumber,
                             DataTypes.StringType);
        spark.udf().register("th3IndexIsValidCell",       th3IndexIsValidCell,
                             DataTypes.StringType);
        spark.udf().register("th3IndexIsResClassIii",     th3IndexIsResClassIii,
                             DataTypes.StringType);
        spark.udf().register("th3IndexIsPentagon",        th3IndexIsPentagon,
                             DataTypes.StringType);

        // Hierarchy
        spark.udf().register("th3IndexCellToParent",        th3IndexCellToParent,
                             DataTypes.StringType);
        spark.udf().register("th3IndexCellToParentNext",    th3IndexCellToParentNext,
                             DataTypes.StringType);
        spark.udf().register("th3IndexCellToCenterChild",   th3IndexCellToCenterChild,
                             DataTypes.StringType);
        spark.udf().register("th3IndexCellToCenterChildNext", th3IndexCellToCenterChildNext,
                             DataTypes.StringType);
        spark.udf().register("th3IndexCellToChildPos",      th3IndexCellToChildPos,
                             DataTypes.StringType);
        spark.udf().register("th3IndexChildPosToCell",      th3IndexChildPosToCell,
                             DataTypes.StringType);

        // Lat/Lng conversion
        spark.udf().register("tgeogpointToTh3Index",  tgeogpointToTh3Index,  DataTypes.StringType);
        spark.udf().register("tgeompointToTh3Index",  tgeompointToTh3Index,  DataTypes.StringType);
        spark.udf().register("th3IndexToTgeogpoint",  th3IndexToTgeogpoint,  DataTypes.StringType);
        spark.udf().register("th3IndexToTgeompoint",  th3IndexToTgeompoint,  DataTypes.StringType);
        spark.udf().register("th3IndexCellToBoundary", th3IndexCellToBoundary, DataTypes.StringType);
        spark.udf().register("geomToH3Cell",          geomToH3Cell,          DataTypes.LongType);
        spark.udf().register("geoToH3IndexSet",       geoToH3IndexSet,       DataTypes.StringType);
        spark.udf().register("everIntersectsH3IndexSetTh3Index",
                             everIntersectsH3IndexSetTh3Index,                DataTypes.BooleanType);

        // Directed edges
        spark.udf().register("th3IndexAreNeighborCells",        th3IndexAreNeighborCells,
                             DataTypes.StringType);
        spark.udf().register("th3IndexCellsToDirectedEdge",     th3IndexCellsToDirectedEdge,
                             DataTypes.StringType);
        spark.udf().register("th3IndexIsValidDirectedEdge",     th3IndexIsValidDirectedEdge,
                             DataTypes.StringType);
        spark.udf().register("th3IndexGetDirectedEdgeOrigin",   th3IndexGetDirectedEdgeOrigin,
                             DataTypes.StringType);
        spark.udf().register("th3IndexGetDirectedEdgeDestination",
                             th3IndexGetDirectedEdgeDestination,            DataTypes.StringType);
        spark.udf().register("th3IndexDirectedEdgeToBoundary",  th3IndexDirectedEdgeToBoundary,
                             DataTypes.StringType);

        // Vertices
        spark.udf().register("th3IndexCellToVertex",   th3IndexCellToVertex,   DataTypes.StringType);
        spark.udf().register("th3IndexVertexToLatlng", th3IndexVertexToLatlng, DataTypes.StringType);
        spark.udf().register("th3IndexIsValidVertex",  th3IndexIsValidVertex,  DataTypes.StringType);

        // Grid traversal
        spark.udf().register("th3IndexGridDistance",   th3IndexGridDistance,   DataTypes.StringType);
        spark.udf().register("th3IndexCellToLocalIj",  th3IndexCellToLocalIj,  DataTypes.StringType);
        spark.udf().register("th3IndexLocalIjToCell",  th3IndexLocalIjToCell,  DataTypes.StringType);

        // Metrics
        spark.udf().register("th3IndexCellArea",                   th3IndexCellArea,
                             DataTypes.StringType);
        spark.udf().register("th3IndexEdgeLength",                 th3IndexEdgeLength,
                             DataTypes.StringType);
        spark.udf().register("tgeogpointGreatCircleDistance",      tgeogpointGreatCircleDistance,
                             DataTypes.StringType);
    }
}
