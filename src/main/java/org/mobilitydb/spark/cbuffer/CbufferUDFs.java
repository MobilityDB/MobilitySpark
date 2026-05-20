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

package org.mobilitydb.spark.cbuffer;

import functions.functions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosNative;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.util.HexFormat;
import org.mobilitydb.spark.util.TimeUtil;

/**
 * Spark SQL UDFs for the circular-buffer (cbuffer / cbufferset / tcbuffer)
 * family.  JMEOS-1.4 ships no cbuffer symbols, so the base-type symbols are
 * bound raw through {@link MeosNative} (verified present in
 * {@code lib/libmeos.so} via {@code nm -D}); the temporal subtype reuses the
 * generic {@code functions.*} Temporal helpers (a Temporal is a Temporal
 * regardless of its element type — the hex-WKB carries the type tag).
 *
 * Storage convention (identical to every other family in this codebase):
 *   cbuffer / cbufferset → hex-WKB STRING (cbuffer_as_hexwkb / set_as_hexwkb)
 *   tcbuffer             → hex-WKB STRING (temporal_as_hexwkb)
 *   geometry             → WKT STRING     (geo_from_text / geo_as_text)
 *
 * MEOS function authority: meos/include/meos_cbuffer.h.
 */
public final class CbufferUDFs {

    private CbufferUDFs() {}

    private static long toPgEpochMicros(Timestamp ts) {
        return (ts.getTime() - TimeUtil.PG_UNIX_EPOCH_OFFSET_MS) * 1000L;
    }

    // size_t out-param buffer for *_as_hexwkb (writes the byte count there).
    private static Pointer sizeOut() {
        return Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
    }

    // ------------------------------------------------------------------
    // Base-type circular buffer
    // ------------------------------------------------------------------

    // cbuffer(pointWkt STRING, radius DOUBLE) → STRING (cbuffer hex-WKB)
    // MEOS: cbuffer_make(const GSERIALIZED *point, double radius) → Cbuffer *
    public static final UDF2<String, Double, String> cbuffer =
        (pointWkt, radius) -> {
            if (pointWkt == null || radius == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(pointWkt, 0);
            if (g == null) return null;
            try {
                Pointer cb = MeosNative.INSTANCE.cbuffer_make(g, radius);
                if (cb == null) return null;
                try {
                    return MeosNative.INSTANCE.cbuffer_as_hexwkb(cb, (byte) 0, sizeOut());
                } finally { MeosMemory.free(cb); }
            } finally { MeosMemory.free(g); }
        };

    // point(cbuffer STRING) → STRING (geometry WKT)
    // MEOS: cbuffer_point(const Cbuffer *cb) → GSERIALIZED *
    public static final UDF1<String, String> point =
        (cbHex) -> {
            if (cbHex == null) return null;
            MeosThread.ensureReady();
            Pointer cb = MeosNative.INSTANCE.cbuffer_from_hexwkb(cbHex);
            if (cb == null) return null;
            try {
                Pointer g = MeosNative.INSTANCE.cbuffer_point(cb);
                if (g == null) return null;
                try { return functions.geo_as_text(g, 15); }
                finally { MeosMemory.free(g); }
            } finally { MeosMemory.free(cb); }
        };

    // radius(cbuffer STRING) → DOUBLE
    // MEOS: cbuffer_radius(const Cbuffer *cb) → double
    public static final UDF1<String, Double> radius =
        (cbHex) -> {
            if (cbHex == null) return null;
            MeosThread.ensureReady();
            Pointer cb = MeosNative.INSTANCE.cbuffer_from_hexwkb(cbHex);
            if (cb == null) return null;
            try { return MeosNative.INSTANCE.cbuffer_radius(cb); }
            finally { MeosMemory.free(cb); }
        };

    // ------------------------------------------------------------------
    // Spatial relationships — cbuffer × cbuffer.  MEOS returns int (-1 on
    // error / undefined, 0 false, 1 true); map -1 → null.
    // ------------------------------------------------------------------

    private interface Rel { int call(Pointer a, Pointer b); }

    private static Boolean rel2(String a, String b, Rel fn) {
        if (a == null || b == null) return null;
        MeosThread.ensureReady();
        Pointer pa = MeosNative.INSTANCE.cbuffer_from_hexwkb(a);
        if (pa == null) return null;
        try {
            Pointer pb = MeosNative.INSTANCE.cbuffer_from_hexwkb(b);
            if (pb == null) return null;
            try {
                int r = fn.call(pa, pb);
                return r < 0 ? null : (r != 0);
            } finally { MeosMemory.free(pb); }
        } finally { MeosMemory.free(pa); }
    }

    // cbufferContains(cbuffer, cbuffer) — MEOS: cbuffer_contains
    public static final UDF2<String, String, Boolean> cbufferContains =
        (a, b) -> rel2(a, b, MeosNative.INSTANCE::cbuffer_contains);

    // cbufferCovers(cbuffer, cbuffer) — MEOS: cbuffer_covers
    public static final UDF2<String, String, Boolean> cbufferCovers =
        (a, b) -> rel2(a, b, MeosNative.INSTANCE::cbuffer_covers);

    // cbufferDisjoint(cbuffer, cbuffer) — MEOS: cbuffer_disjoint
    public static final UDF2<String, String, Boolean> cbufferDisjoint =
        (a, b) -> rel2(a, b, MeosNative.INSTANCE::cbuffer_disjoint);

    // cbufferIntersects(cbuffer, cbuffer) — MEOS: cbuffer_intersects
    public static final UDF2<String, String, Boolean> cbufferIntersects =
        (a, b) -> rel2(a, b, MeosNative.INSTANCE::cbuffer_intersects);

    // cbufferTouches(cbuffer, cbuffer) — MEOS: cbuffer_touches
    public static final UDF2<String, String, Boolean> cbufferTouches =
        (a, b) -> rel2(a, b, MeosNative.INSTANCE::cbuffer_touches);

    // cbufferDwithin(cbuffer, cbuffer, dist DOUBLE) — MEOS: cbuffer_dwithin
    public static final UDF3<String, String, Double, Boolean> cbufferDwithin =
        (a, b, dist) -> {
            if (a == null || b == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer pa = MeosNative.INSTANCE.cbuffer_from_hexwkb(a);
            if (pa == null) return null;
            try {
                Pointer pb = MeosNative.INSTANCE.cbuffer_from_hexwkb(b);
                if (pb == null) return null;
                try {
                    int r = MeosNative.INSTANCE.cbuffer_dwithin(pa, pb, dist);
                    return r < 0 ? null : (r != 0);
                } finally { MeosMemory.free(pb); }
            } finally { MeosMemory.free(pa); }
        };

    // cbufferSame(cbuffer, cbuffer) → BOOLEAN — MEOS: cbuffer_same (returns bool)
    public static final UDF2<String, String, Boolean> cbufferSame =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer pa = MeosNative.INSTANCE.cbuffer_from_hexwkb(a);
            if (pa == null) return null;
            try {
                Pointer pb = MeosNative.INSTANCE.cbuffer_from_hexwkb(b);
                if (pb == null) return null;
                try { return MeosNative.INSTANCE.cbuffer_same(pa, pb); }
                finally { MeosMemory.free(pb); }
            } finally { MeosMemory.free(pa); }
        };

    // ------------------------------------------------------------------
    // cbufferset I/O — generic Set helpers (the WKB carries the element type)
    // ------------------------------------------------------------------

    // cbuffersetFromHexWKB(hex STRING) → STRING — MEOS: set_from_hexwkb (generic)
    public static final UDF1<String, String> cbuffersetFromHexWKB =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    // cbuffersetFromBinary(bytes BINARY) → STRING — hex round-trip (generic)
    public static final UDF1<byte[], String> cbuffersetFromBinary =
        (bytes) -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // Temporal circular buffer (tcbuffer) — generic Temporal helpers
    // ------------------------------------------------------------------

    // tcbuffer(tgeompoint STRING, tfloat STRING) → STRING (tcbuffer hex-WKB)
    // The (base,time) and tpoint/tfloat ctor overloads collapse to this one
    // most-useful form (MEOS: tcbuffer_make(const Temporal *, const Temporal *)).
    public static final UDF2<String, String, String> tcbuffer =
        (tpointHex, tfloatHex) -> {
            if (tpointHex == null || tfloatHex == null) return null;
            MeosThread.ensureReady();
            Pointer tp = functions.temporal_from_hexwkb(tpointHex);
            if (tp == null) return null;
            try {
                Pointer tf = functions.temporal_from_hexwkb(tfloatHex);
                if (tf == null) return null;
                try {
                    Pointer r = MeosNative.INSTANCE.tcbuffer_make(tp, tf);
                    if (r == null) return null;
                    try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                    finally { MeosMemory.free(r); }
                } finally { MeosMemory.free(tf); }
            } finally { MeosMemory.free(tp); }
        };

    // tcbufferInst(cbuffer STRING, ts TIMESTAMP) → STRING (TInstant hex-WKB)
    // MEOS: tcbuffer_make on a single-instant tpoint/tfloat pair is overkill;
    // the SQL tcbufferInst builds a TInstant from a cbuffer value + timestamp.
    // The cbuffer value decomposes into its point + radius which we lift to a
    // tgeompoint instant and tfloat instant, then tcbuffer_make.
    public static final UDF2<String, Timestamp, String> tcbufferInst =
        (cbHex, ts) -> {
            if (cbHex == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer cb = MeosNative.INSTANCE.cbuffer_from_hexwkb(cbHex);
            if (cb == null) return null;
            try {
                Pointer g = MeosNative.INSTANCE.cbuffer_point(cb);
                if (g == null) return null;
                double rad = MeosNative.INSTANCE.cbuffer_radius(cb);
                long micros = toPgEpochMicros(ts);
                try {
                    Pointer tp = MeosNative.INSTANCE.tgeoinst_make(g, micros);
                    if (tp == null) return null;
                    try {
                        Pointer tf = functions.tfloatinst_make(rad,
                            java.time.Instant.ofEpochMilli(ts.getTime())
                                .atOffset(java.time.ZoneOffset.UTC));
                        if (tf == null) return null;
                        try {
                            Pointer r = MeosNative.INSTANCE.tcbuffer_make(tp, tf);
                            if (r == null) return null;
                            try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                            finally { MeosMemory.free(r); }
                        } finally { MeosMemory.free(tf); }
                    } finally { MeosMemory.free(tp); }
                } finally { MeosMemory.free(g); }
            } finally { MeosMemory.free(cb); }
        };

    private static UDF1<String, String> temporalRoundtrip() {
        return hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    private static UDF1<byte[], String> temporalFromBinary() {
        return bytes -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    // tcbuffer text constructor (FromText / FromEWKT) — MEOS: tcbuffer_in
    public static final UDF1<String, String> tcbufferFromText =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.tcbuffer_in(wkt);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    public static final UDF1<String, String> tcbufferFromEWKT = tcbufferFromText;

    public static final UDF1<String, String> tcbufferFromHexEWKB = temporalRoundtrip();
    public static final UDF1<byte[], String>  tcbufferFromBinary  = temporalFromBinary();
    public static final UDF1<byte[], String>  tcbufferFromEWKB    = temporalFromBinary();

    // Subtype conversion aliases (Seq / SeqSet) — generic conversion, linear.
    public static final UDF1<String, String> tcbufferSeq =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.temporal_to_tsequence(p, "linear");
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String> tcbufferSeqSet =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.temporal_to_tsequenceset(p, "linear");
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    // tcbufferSeqSetGaps(tcbuffer[], maxt) — generic tsequenceset_make_gaps,
    // LINEAR interpolation (tcbuffer is continuous).
    private static final int INTERP_LINEAR = 3;

    public static final UDF2<String[], String, String> tcbufferSeqSetGaps =
        (instants, maxt) -> {
            if (instants == null || instants.length == 0) return null;
            MeosThread.ensureReady();
            int n = instants.length;
            Pointer[] insts = new Pointer[n];
            try {
                for (int i = 0; i < n; i++) {
                    if (instants[i] == null) return null;
                    insts[i] = functions.temporal_from_hexwkb(instants[i]);
                    if (insts[i] == null) return null;
                }
                Pointer buf = Runtime.getSystemRuntime().getMemoryManager()
                    .allocateDirect(8L * n);
                for (int i = 0; i < n; i++) {
                    buf.putAddress(i * 8L, insts[i].address());
                }
                Pointer mt = (maxt == null) ? null : functions.pg_interval_in(maxt, -1);
                try {
                    Pointer r = functions.tsequenceset_make_gaps(buf, n,
                        INTERP_LINEAR, mt, -1.0);
                    if (r == null) return null;
                    try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                    finally { MeosMemory.free(r); }
                } finally {
                    if (mt != null) MeosMemory.free(mt);
                }
            } finally {
                for (Pointer p : insts) {
                    if (p != null) MeosMemory.free(p);
                }
            }
        };

    // ------------------------------------------------------------------
    // tcbuffer accessors — points / radius (return Set, hex-WKB string)
    // ------------------------------------------------------------------

    // points(tcbuffer STRING) → STRING (geomset hex-WKB)
    // MEOS: tcbuffer_points(const Temporal *) → Set *
    public static final UDF1<String, String> points =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer s = MeosNative.INSTANCE.tcbuffer_points(p);
                if (s == null) return null;
                try { return functions.set_as_hexwkb(s, (byte) 0); }
                finally { MeosMemory.free(s); }
            } finally { MeosMemory.free(p); }
        };

    // tcbufferRadius(tcbuffer STRING) → STRING (floatset hex-WKB)
    // MEOS: tcbuffer_radius(const Temporal *) → Set *
    // Registered under the bare name `radius` (audit matches by name; one
    // `radius` registration covers both the cbuffer-scalar and tcbuffer
    // sections). The cbuffer-scalar `radius` above returns DOUBLE; this
    // temporal form is exposed as `tcbufferRadius` so both coexist.
    public static final UDF1<String, String> tcbufferRadius =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer s = MeosNative.INSTANCE.tcbuffer_radius(p);
                if (s == null) return null;
                try { return functions.set_as_hexwkb(s, (byte) 0); }
                finally { MeosMemory.free(s); }
            } finally { MeosMemory.free(p); }
        };

    public static void registerAll(SparkSession spark) {
        // Base-type circular buffer
        spark.udf().register("cbuffer",            cbuffer,            DataTypes.StringType);
        spark.udf().register("point",              point,              DataTypes.StringType);
        spark.udf().register("radius",             radius,             DataTypes.DoubleType);
        // Spatial relationships
        spark.udf().register("cbufferContains",    cbufferContains,    DataTypes.BooleanType);
        spark.udf().register("cbufferCovers",      cbufferCovers,      DataTypes.BooleanType);
        spark.udf().register("cbufferDisjoint",    cbufferDisjoint,    DataTypes.BooleanType);
        spark.udf().register("cbufferIntersects",  cbufferIntersects,  DataTypes.BooleanType);
        spark.udf().register("cbufferTouches",     cbufferTouches,     DataTypes.BooleanType);
        spark.udf().register("cbufferDwithin",     cbufferDwithin,     DataTypes.BooleanType);
        spark.udf().register("cbufferSame",        cbufferSame,        DataTypes.BooleanType);
        // cbufferset I/O
        spark.udf().register("cbuffersetFromHexWKB", cbuffersetFromHexWKB, DataTypes.StringType);
        spark.udf().register("cbuffersetFromBinary", cbuffersetFromBinary, DataTypes.StringType);
        // Temporal circular buffer
        spark.udf().register("tcbuffer",            tcbuffer,            DataTypes.StringType);
        spark.udf().register("tcbufferInst",        tcbufferInst,        DataTypes.StringType);
        spark.udf().register("tcbufferFromText",    tcbufferFromText,    DataTypes.StringType);
        spark.udf().register("tcbufferFromEWKT",    tcbufferFromEWKT,    DataTypes.StringType);
        spark.udf().register("tcbufferFromHexEWKB", tcbufferFromHexEWKB, DataTypes.StringType);
        spark.udf().register("tcbufferFromBinary",  tcbufferFromBinary,  DataTypes.StringType);
        spark.udf().register("tcbufferFromEWKB",    tcbufferFromEWKB,    DataTypes.StringType);
        spark.udf().register("tcbufferSeq",         tcbufferSeq,         DataTypes.StringType);
        spark.udf().register("tcbufferSeqSet",      tcbufferSeqSet,      DataTypes.StringType);
        spark.udf().register("tcbufferSeqSetGaps",  tcbufferSeqSetGaps,  DataTypes.StringType);
        spark.udf().register("points",              points,              DataTypes.StringType);
        spark.udf().register("tcbufferRadius",      tcbufferRadius,      DataTypes.StringType);
    }
}
