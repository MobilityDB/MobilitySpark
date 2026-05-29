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

package org.mobilitydb.spark.npoint;

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
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Spark SQL UDFs for the network-point (npoint / nsegment / npointset /
 * tnpoint) family, including the route-set operators of
 * {@code 091_tnpoint_routeops.in.sql}.
 *
 * JMEOS-1.4 ships no npoint symbols, so the base-type symbols are bound raw
 * through {@link MeosNative} (each verified present in {@code lib/libmeos.so}
 * via {@code nm -D}); the temporal subtype reuses the generic
 * {@code functions.*} Temporal helpers.
 *
 * Storage convention:
 *   npoint / nsegment / npointset → hex-WKB STRING (npoint_as_hexwkb /
 *                                   set_as_hexwkb)
 *   tnpoint                       → hex-WKB STRING (temporal_as_hexwkb)
 *
 * MEOS function authority: meos/include/meos_npoint.h and
 * meos/include/npoint/tnpoint_routeops.h.
 */
public final class NpointUDFs {

    private NpointUDFs() {}

    // interpType enum (meos.h): INTERP_NONE=0, DISCRETE=1, STEP=2, LINEAR=3
    private static final int INTERP_LINEAR = 3;

    private static Pointer sizeOut() {
        return Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
    }

    // ------------------------------------------------------------------
    // Base-type network point / segment
    // ------------------------------------------------------------------

    // npoint(rid BIGINT, pos DOUBLE) → STRING (npoint hex-WKB)
    // MEOS: npoint_make(int64 rid, double pos) → Npoint *
    public static final UDF2<Long, Double, String> npoint =
        (rid, pos) -> {
            if (rid == null || pos == null) return null;
            MeosThread.ensureReady();
            Pointer np = MeosNative.INSTANCE.npoint_make(rid, pos);
            if (np == null) return null;
            try {
                return MeosNative.INSTANCE.npoint_as_hexwkb(np, (byte) 0, sizeOut());
            } finally { MeosMemory.free(np); }
        };

    // nsegment(rid BIGINT, pos1 DOUBLE, pos2 DOUBLE) → STRING (nsegment hex)
    // MEOS: nsegment_make(int64 rid, double pos1, double pos2) → Nsegment *
    // nsegment has no hex-WKB serializer in MEOS; its canonical portable
    // form is its text output (nsegment_out), used here for the round-trip.
    public static final UDF3<Long, Double, Double, String> nsegment =
        (rid, pos1, pos2) -> {
            if (rid == null || pos1 == null || pos2 == null) return null;
            MeosThread.ensureReady();
            Pointer ns = MeosNative.INSTANCE.nsegment_make(rid, pos1, pos2);
            if (ns == null) return null;
            try { return MeosNative.INSTANCE.nsegment_out(ns, 15); }
            finally { MeosMemory.free(ns); }
        };

    // route(npoint STRING) → BIGINT — MEOS: npoint_route(const Npoint *)
    public static final UDF1<String, Long> route =
        (npHex) -> {
            if (npHex == null) return null;
            MeosThread.ensureReady();
            Pointer np = MeosNative.INSTANCE.npoint_from_hexwkb(npHex);
            if (np == null) return null;
            try { return MeosNative.INSTANCE.npoint_route(np); }
            finally { MeosMemory.free(np); }
        };

    // getPosition(npoint STRING) → DOUBLE — MEOS: npoint_position
    public static final UDF1<String, Double> getPosition =
        (npHex) -> {
            if (npHex == null) return null;
            MeosThread.ensureReady();
            Pointer np = MeosNative.INSTANCE.npoint_from_hexwkb(npHex);
            if (np == null) return null;
            try { return MeosNative.INSTANCE.npoint_position(np); }
            finally { MeosMemory.free(np); }
        };

    // startPosition(nsegment STRING) → DOUBLE — MEOS: nsegment_start_position
    public static final UDF1<String, Double> startPosition =
        (nsText) -> {
            if (nsText == null) return null;
            MeosThread.ensureReady();
            Pointer ns = MeosNative.INSTANCE.nsegment_in(nsText);
            if (ns == null) return null;
            try { return MeosNative.INSTANCE.nsegment_start_position(ns); }
            finally { MeosMemory.free(ns); }
        };

    // endPosition(nsegment STRING) → DOUBLE — MEOS: nsegment_end_position
    public static final UDF1<String, Double> endPosition =
        (nsText) -> {
            if (nsText == null) return null;
            MeosThread.ensureReady();
            Pointer ns = MeosNative.INSTANCE.nsegment_in(nsText);
            if (ns == null) return null;
            try { return MeosNative.INSTANCE.nsegment_end_position(ns); }
            finally { MeosMemory.free(ns); }
        };

    // npoint base-type I/O
    public static final UDF1<String, String> npointFromText =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer np = MeosNative.INSTANCE.npoint_in(s);
            if (np == null) return null;
            try { return MeosNative.INSTANCE.npoint_as_hexwkb(np, (byte) 0, sizeOut()); }
            finally { MeosMemory.free(np); }
        };
    public static final UDF1<String, String> npointFromEWKT = npointFromText;

    public static final UDF1<String, String> npointFromHexEWKB =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer np = MeosNative.INSTANCE.npoint_from_hexwkb(hex);
            if (np == null) return null;
            try { return MeosNative.INSTANCE.npoint_as_hexwkb(np, (byte) 0, sizeOut()); }
            finally { MeosMemory.free(np); }
        };

    public static final UDF1<byte[], String> npointFromBinary =
        (bytes) -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer np = MeosNative.INSTANCE.npoint_from_hexwkb(hex);
            if (np == null) return null;
            try { return MeosNative.INSTANCE.npoint_as_hexwkb(np, (byte) 0, sizeOut()); }
            finally { MeosMemory.free(np); }
        };
    public static final UDF1<byte[], String> npointFromEWKB = npointFromBinary;

    // ------------------------------------------------------------------
    // npointset I/O — generic Set helpers + npointset routes accessor
    // ------------------------------------------------------------------

    public static final UDF1<String, String> npointsetFromHexWKB =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String> npointsetFromText =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.npointset_in(s);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    public static final UDF1<String, String> npointsetFromEWKT = npointsetFromText;

    public static final UDF1<byte[], String> npointsetFromBinary =
        (bytes) -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    public static final UDF1<byte[], String> npointsetFromEWKB = npointsetFromBinary;

    // routes(npointset STRING | tnpoint STRING) → STRING (bigintset hex-WKB)
    // Dispatches on payload: a Set deserialises via set_from_hexwkb, a
    // Temporal via temporal_from_hexwkb.  Covers both npointset_routes
    // (082) and tnpoint_routes (083) under the single bare name `routes`.
    public static final UDF1<String, String> routes =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer set = functions.set_from_hexwkb(hex);
            if (set != null) {
                try {
                    Pointer r = MeosNative.INSTANCE.npointset_routes(set);
                    if (r == null) return null;
                    try { return functions.set_as_hexwkb(r, (byte) 0); }
                    finally { MeosMemory.free(r); }
                } finally { MeosMemory.free(set); }
            }
            Pointer t = functions.temporal_from_hexwkb(hex);
            if (t == null) return null;
            try {
                Pointer r = MeosNative.INSTANCE.tnpoint_routes(t);
                if (r == null) return null;
                try { return functions.set_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(t); }
        };

    // ------------------------------------------------------------------
    // Temporal network point (tnpoint)
    // ------------------------------------------------------------------

    // tnpoint(npoint STRING, ts TIMESTAMP) → STRING (TInstant hex-WKB)
    // The (base,time) ctor is the BerlinMOD-relevant overload.
    // MEOS: tnpointinst_make(const Npoint *np, TimestampTz t) → TInstant *
    public static final UDF2<String, Timestamp, String> tnpoint =
        (npHex, ts) -> {
            if (npHex == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer np = MeosNative.INSTANCE.npoint_from_hexwkb(npHex);
            if (np == null) return null;
            try {
                long micros = (ts.getTime() - 946684800L * 1000L) * 1000L;
                Pointer r = MeosNative.INSTANCE.tnpointinst_make(np, micros);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(np); }
        };

    public static final UDF2<String, Timestamp, String> tnpointInst = tnpoint;

    // tnpoint route accessor — registered under bare name `route` already
    // (covers npoint/081 + tnpoint/083). The temporal form:
    public static final UDF1<String, Long> tnpointRoute =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(hex);
            if (t == null) return null;
            try { return MeosNative.INSTANCE.tnpoint_route(t); }
            finally { MeosMemory.free(t); }
        };

    // positions(tnpoint STRING) → ARRAY<STRING> (each nsegment as text)
    // MEOS: tnpoint_positions(const Temporal *, int *count) → Nsegment **
    public static final UDF1<String, List<String>> positions =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(hex);
            if (t == null) return null;
            try {
                Pointer cnt = sizeOut();
                Pointer arr = MeosNative.INSTANCE.tnpoint_positions(t, cnt);
                if (arr == null) return null;
                int count = cnt.getInt(0);
                List<String> result = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    Pointer ns = arr.getPointer((long) i * 8);
                    if (ns == null) { result.add(null); continue; }
                    result.add(MeosNative.INSTANCE.nsegment_out(ns, 15));
                    MeosMemory.free(ns);
                }
                MeosMemory.free(arr);
                return result;
            } finally { MeosMemory.free(t); }
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

    public static final UDF1<String, String> tnpointFromHexWKB = temporalRoundtrip();
    public static final UDF1<byte[], String>  tnpointFromBinary = temporalFromBinary();

    public static final UDF1<String, String> tnpointSeq =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.temporal_to_tsequence(p, INTERP_LINEAR);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String> tnpointSeqSet =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.temporal_to_tsequenceset(p, INTERP_LINEAR);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };


    public static final UDF2<String[], String, String> tnpointSeqSetGaps =
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
    // Route-set operators (091_tnpoint_routeops.in.sql)
    //
    // One bare UDF per name dispatching the libmeos-exported overloads:
    //   - (tnpoint, bigint)      → *_rid_tnpoint_bigint
    //   - (tnpoint, bigintset)   → *_rid_tnpoint_bigintset    (bigintset hex)
    //   - (tnpoint, tnpoint)     → *_rid_tnpoint_tnpoint
    // Argument 2 is read as a STRING; "<digits>" → bigint form, a Set
    // hex-WKB → bigintset form, a Temporal hex-WKB → tnpoint form.
    // The kernel's `invert` flag is false (temp is the first operand).
    // MEOS authority: meos/include/npoint/tnpoint_routeops.h.
    // ------------------------------------------------------------------

    private interface RidBigint  { boolean call(Pointer t, long rid, boolean inv); }
    private interface RidSet     { boolean call(Pointer t, Pointer s, boolean inv); }
    private interface RidTnpoint { boolean call(Pointer t1, Pointer t2); }

    private static Boolean ridDispatch(String tnpHex, String arg,
            RidBigint fb, RidSet fs, RidTnpoint ft) {
        if (tnpHex == null || arg == null) return null;
        MeosThread.ensureReady();
        Pointer t = functions.temporal_from_hexwkb(tnpHex);
        if (t == null) return null;
        try {
            // bigint literal?
            String a = arg.trim();
            if (a.matches("-?\\d+")) {
                if (fb == null) return null;
                return fb.call(t, Long.parseLong(a), false);
            }
            // bigintset hex-WKB?
            Pointer s = functions.set_from_hexwkb(a);
            if (s != null) {
                try {
                    if (fs == null) return null;
                    return fs.call(t, s, false);
                } finally { MeosMemory.free(s); }
            }
            // tnpoint hex-WKB
            Pointer t2 = functions.temporal_from_hexwkb(a);
            if (t2 == null) return null;
            try {
                if (ft == null) return null;
                return ft.call(t, t2);
            } finally { MeosMemory.free(t2); }
        } finally { MeosMemory.free(t); }
    }

    public static final UDF2<String, String, Boolean> containsRid =
        (t, a) -> ridDispatch(t, a,
            MeosNative.INSTANCE::contains_rid_tnpoint_bigint,
            MeosNative.INSTANCE::contains_rid_tnpoint_bigintset,
            MeosNative.INSTANCE::contains_rid_tnpoint_tnpoint);

    public static final UDF2<String, String, Boolean> containedRid =
        (t, a) -> ridDispatch(t, a,
            MeosNative.INSTANCE::contained_rid_tnpoint_bigint,
            MeosNative.INSTANCE::contained_rid_tnpoint_bigintset,
            MeosNative.INSTANCE::contained_rid_tnpoint_tnpoint);

    public static final UDF2<String, String, Boolean> sameRid =
        (t, a) -> ridDispatch(t, a,
            MeosNative.INSTANCE::same_rid_tnpoint_bigint,
            MeosNative.INSTANCE::same_rid_tnpoint_bigintset,
            MeosNative.INSTANCE::same_rid_tnpoint_tnpoint);

    // overlaps_rid has no (tnpoint,bigint) kernel exported — only
    // bigintset and tnpoint forms.
    public static final UDF2<String, String, Boolean> overlapsRid =
        (t, a) -> ridDispatch(t, a,
            null,
            MeosNative.INSTANCE::overlaps_rid_tnpoint_bigintset,
            MeosNative.INSTANCE::overlaps_rid_tnpoint_tnpoint);

    public static void registerAll(SparkSession spark) {
        // Base-type network point / segment
        spark.udf().register("npoint",        npoint,        DataTypes.StringType);
        spark.udf().register("nsegment",      nsegment,      DataTypes.StringType);
        spark.udf().register("route",         route,         DataTypes.LongType);
        spark.udf().register("getPosition",   getPosition,   DataTypes.DoubleType);
        spark.udf().register("startPosition", startPosition, DataTypes.DoubleType);
        spark.udf().register("endPosition",   endPosition,   DataTypes.DoubleType);
        spark.udf().register("npointFromText",    npointFromText,    DataTypes.StringType);
        spark.udf().register("npointFromEWKT",    npointFromEWKT,    DataTypes.StringType);
        spark.udf().register("npointFromHexEWKB", npointFromHexEWKB, DataTypes.StringType);
        spark.udf().register("npointFromBinary",  npointFromBinary,  DataTypes.StringType);
        spark.udf().register("npointFromEWKB",    npointFromEWKB,    DataTypes.StringType);
        // npointset
        spark.udf().register("npointsetFromHexWKB", npointsetFromHexWKB, DataTypes.StringType);
        spark.udf().register("npointsetFromText",   npointsetFromText,   DataTypes.StringType);
        spark.udf().register("npointsetFromEWKT",   npointsetFromEWKT,   DataTypes.StringType);
        spark.udf().register("npointsetFromBinary", npointsetFromBinary, DataTypes.StringType);
        spark.udf().register("npointsetFromEWKB",   npointsetFromEWKB,   DataTypes.StringType);
        spark.udf().register("routes",              routes,              DataTypes.StringType);
        // Temporal network point
        spark.udf().register("tnpoint",          tnpoint,          DataTypes.StringType);
        spark.udf().register("tnpointInst",      tnpointInst,      DataTypes.StringType);
        spark.udf().register("tnpointRoute",     tnpointRoute,     DataTypes.LongType);
        spark.udf().register("positions",        positions,
            DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tnpointFromHexWKB", tnpointFromHexWKB, DataTypes.StringType);
        spark.udf().register("tnpointFromBinary", tnpointFromBinary, DataTypes.StringType);
        spark.udf().register("tnpointSeq",        tnpointSeq,        DataTypes.StringType);
        spark.udf().register("tnpointSeqSet",     tnpointSeqSet,     DataTypes.StringType);
        spark.udf().register("tnpointSeqSetGaps", tnpointSeqSetGaps, DataTypes.StringType);
        // Route-set operators
        spark.udf().register("containsRid",  containsRid,  DataTypes.BooleanType);
        spark.udf().register("containedRid", containedRid, DataTypes.BooleanType);
        spark.udf().register("sameRid",      sameRid,      DataTypes.BooleanType);
        spark.udf().register("overlapsRid",  overlapsRid,  DataTypes.BooleanType);
    }
}
