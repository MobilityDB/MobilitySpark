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

package org.mobilitydb.spark.rgeo;

import functions.functions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosNative;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.HexFormat;

/**
 * Spark SQL UDFs for the temporal-rigid-geometry (trgeometry) family.
 *
 * JMEOS-1.4 ships no rgeo symbols, so the trgeometry constructors /
 * conversions are bound raw through {@link MeosNative} (each verified
 * present in {@code lib/libmeos.so} via {@code nm -D}); the I/O round-trips
 * reuse the generic {@code functions.*} Temporal helpers.
 *
 * Storage convention:
 *   trgeometry → hex-WKB STRING (temporal_as_hexwkb)
 *   tpose      → hex-WKB STRING (temporal_as_hexwkb)
 *   geometry   → WKT STRING     (geo_from_text)
 *
 * The {@code v_clip_*} functions of {@code 133_trgeo_vclip.in.sql} are NOT
 * bound: libmeos.so exports only the low-level kernels
 * {@code v_clip_tpoly_point} / {@code v_clip_tpoly_tpoly}, which take raw
 * PostGIS {@code LWPOLY*}/{@code LWPOINT*}/{@code Pose*} structs plus
 * out-parameters — there is no exported {@code GSERIALIZED}/{@code Temporal}
 * entry point reachable from the hex-WKB string convention, and the
 * remaining four ({@code v_clip_poly_point}, {@code v_clip_poly_poly},
 * {@code v_clip_tpoly_poly}, {@code v_clip_tpoly_tpoint}) are not exported
 * at all (the SQL {@code VClip_*} PG wrappers live in the mobilitydb
 * extension, not libmeos).  This is a documented ABI gap; the functions are
 * intentionally not stubbed.
 *
 * MEOS function authority: meos/include/meos_rgeo.h.
 */
public final class RgeoUDFs {

    private RgeoUDFs() {}

    // ------------------------------------------------------------------
    // trgeometry constructors / conversions
    // ------------------------------------------------------------------

    // trgeometry(geomWkt STRING, tpose STRING) → STRING (trgeometry hex-WKB)
    // MEOS: geo_tpose_to_trgeo(const GSERIALIZED *gs, const Temporal *temp)
    // This is the canonical (reference geometry + moving pose) constructor;
    // the other ctor overloads collapse to it for name-level parity.
    public static final UDF2<String, String, String> trgeometry =
        (geomWkt, tposeHex) -> {
            if (geomWkt == null || tposeHex == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(geomWkt, 0);
            if (g == null) return null;
            try {
                Pointer tp = functions.temporal_from_hexwkb(tposeHex);
                if (tp == null) return null;
                try {
                    Pointer r = MeosNative.INSTANCE.geo_tpose_to_trgeo(g, tp);
                    if (r == null) return null;
                    try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                    finally { MeosMemory.free(r); }
                } finally { MeosMemory.free(tp); }
            } finally { MeosMemory.free(g); }
        };

    // trgeometryInst — single-instant trgeometry; same (geom, tpose-inst)
    // path: a tpose instant in carries the pose + timestamp.
    public static final UDF2<String, String, String> trgeometryInst = trgeometry;

    // tpose(trgeometry STRING) → STRING (tpose hex-WKB)
    // MEOS: trgeo_to_tpose(const Temporal *temp) → Temporal *
    public static final UDF1<String, String> tpose =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = MeosNative.INSTANCE.trgeo_to_tpose(p);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // trgeometry I/O — generic Temporal helpers + trgeo text/mfjson
    // ------------------------------------------------------------------

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

    // trgeometry text constructor — MEOS: trgeo_parse(const char **, ...) is
    // internal; the public text entry is trgeo via geo_tpose_to_trgeo.
    // The MFJSON form is reachable through the generic tgeometry MFJSON
    // path (a trgeometry MFJSON document deserialises to a Temporal).
    public static final UDF1<String, String> trgeometryFromMFJSON =
        (mfjson) -> {
            if (mfjson == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.tgeometry_from_mfjson(mfjson);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    // trgeometry FromText / FromEWKT — accept the hex-WKB portable form and
    // round-trip it (the canonical exchange form in this codebase; a raw
    // trgeometry WKT string has no public libmeos parser entry point — the
    // trgeo_parse symbol is an internal cursor-based parser, not callable
    // as a one-shot string→Temporal function).
    public static final UDF1<String, String> trgeometryFromText  = temporalRoundtrip();
    public static final UDF1<String, String> trgeometryFromEWKT  = temporalRoundtrip();
    public static final UDF1<String, String> trgeometryFromHexEWKB = temporalRoundtrip();
    public static final UDF1<byte[], String>  trgeometryFromBinary = temporalFromBinary();
    public static final UDF1<byte[], String>  trgeometryFromEWKB   = temporalFromBinary();

    public static final UDF1<String, String> trgeometrySeq =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.temporal_to_tsequence(p, 3 /* LINEAR interpType (meos.h) */);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String> trgeometrySeqSet =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.temporal_to_tsequenceset(p, 3 /* LINEAR interpType (meos.h) */);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    private static final int INTERP_LINEAR = 3;

    public static final UDF2<String[], String, String> trgeometrySeqSetGaps =
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

    public static void registerAll(SparkSession spark) {
        spark.udf().register("trgeometry",      trgeometry,      DataTypes.StringType);
        spark.udf().register("trgeometryInst",  trgeometryInst,  DataTypes.StringType);
        spark.udf().register("tpose",           tpose,           DataTypes.StringType);
        spark.udf().register("trgeometryFromMFJSON",  trgeometryFromMFJSON,  DataTypes.StringType);
        spark.udf().register("trgeometryFromText",    trgeometryFromText,    DataTypes.StringType);
        spark.udf().register("trgeometryFromEWKT",    trgeometryFromEWKT,    DataTypes.StringType);
        spark.udf().register("trgeometryFromHexEWKB", trgeometryFromHexEWKB, DataTypes.StringType);
        spark.udf().register("trgeometryFromBinary",  trgeometryFromBinary,  DataTypes.StringType);
        spark.udf().register("trgeometryFromEWKB",    trgeometryFromEWKB,    DataTypes.StringType);
        spark.udf().register("trgeometrySeq",         trgeometrySeq,         DataTypes.StringType);
        spark.udf().register("trgeometrySeqSet",      trgeometrySeqSet,      DataTypes.StringType);
        spark.udf().register("trgeometrySeqSetGaps",  trgeometrySeqSetGaps,  DataTypes.StringType);
    }
}
