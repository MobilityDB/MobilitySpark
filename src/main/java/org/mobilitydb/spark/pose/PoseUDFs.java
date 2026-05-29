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

package org.mobilitydb.spark.pose;

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

import java.util.HexFormat;

/**
 * Spark SQL UDFs for the spatial-pose (pose / poseset / tpose) family.
 *
 * JMEOS-1.4 ships no pose symbols, so the base-type symbols are bound raw
 * through {@link MeosNative} (each verified present in {@code lib/libmeos.so}
 * via {@code nm -D}); the temporal subtype reuses the generic
 * {@code functions.*} Temporal helpers.
 *
 * Storage convention:
 *   pose / poseset → hex-WKB STRING (pose_as_hexwkb / set_as_hexwkb)
 *   tpose          → hex-WKB STRING (temporal_as_hexwkb)
 *   geometry       → WKT STRING     (geo_from_text / geo_as_text)
 *
 * MEOS function authority: meos/include/meos_pose.h.
 */
public final class PoseUDFs {

    private PoseUDFs() {}

    private static Pointer sizeOut() {
        return Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
    }

    // ------------------------------------------------------------------
    // Base-type pose constructors
    // ------------------------------------------------------------------

    // pose(x, y, theta) → STRING (pose hex-WKB) — 2D, SRID 0
    // MEOS: pose_make_2d(double x, double y, double theta, int32_t srid)
    public static final UDF3<Double, Double, Double, String> pose =
        (x, y, theta) -> {
            if (x == null || y == null || theta == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.pose_make_2d(x, y, theta, 0);
            if (p == null) return null;
            try { return MeosNative.INSTANCE.pose_as_hexwkb(p, (byte) 0, sizeOut()); }
            finally { MeosMemory.free(p); }
        };

    // pose3d(x, y, z, W, X, Y, Z) — collapsed via a 4-arg point form is not
    // expressible in Spark's fixed-arity UDF; the 2D `pose` above is the
    // BerlinMOD-relevant ctor. The 3D ctor is reachable through poseFromText.

    // point(pose STRING) → STRING (geometry WKT) — MEOS: pose_to_point
    public static final UDF1<String, String> point =
        (poseHex) -> {
            if (poseHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.pose_from_hexwkb(poseHex);
            if (p == null) return null;
            try {
                Pointer g = MeosNative.INSTANCE.pose_to_point(p);
                if (g == null) return null;
                try { return functions.geo_as_text(g, 15); }
                finally { MeosMemory.free(g); }
            } finally { MeosMemory.free(p); }
        };

    // rotation(pose STRING) → DOUBLE — MEOS: pose_rotation (2D rotation angle)
    public static final UDF1<String, Double> rotation =
        (poseHex) -> {
            if (poseHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.pose_from_hexwkb(poseHex);
            if (p == null) return null;
            try { return MeosNative.INSTANCE.pose_rotation(p); }
            finally { MeosMemory.free(p); }
        };

    // orientation(pose STRING) → ARRAY<DOUBLE> — MEOS: pose_orientation
    // returns a double* (the unit quaternion components for the 3D pose, or
    // the 2D angle).  Returned as a DOUBLE[] so no precision is lost.
    public static final UDF1<String, java.util.List<Double>> orientation =
        (poseHex) -> {
            if (poseHex == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.pose_from_hexwkb(poseHex);
            if (p == null) return null;
            try {
                Pointer q = MeosNative.INSTANCE.pose_orientation(p);
                if (q == null) return null;
                // 3D pose orientation is a 4-component unit quaternion.
                java.util.List<Double> out = new java.util.ArrayList<>(4);
                for (int i = 0; i < 4; i++) out.add(q.getDouble((long) i * 8));
                MeosMemory.free(q);
                return out;
            } finally { MeosMemory.free(p); }
        };

    // poseSame(pose, pose) → BOOLEAN — MEOS: pose_same (returns bool)
    public static final UDF2<String, String, Boolean> poseSame =
        (a, b) -> {
            if (a == null || b == null) return null;
            MeosThread.ensureReady();
            Pointer pa = MeosNative.INSTANCE.pose_from_hexwkb(a);
            if (pa == null) return null;
            try {
                Pointer pb = MeosNative.INSTANCE.pose_from_hexwkb(b);
                if (pb == null) return null;
                try { return MeosNative.INSTANCE.pose_same(pa, pb); }
                finally { MeosMemory.free(pb); }
            } finally { MeosMemory.free(pa); }
        };

    // pose base-type I/O
    public static final UDF1<String, String> poseFromText =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.pose_in(s);
            if (p == null) return null;
            try { return MeosNative.INSTANCE.pose_as_hexwkb(p, (byte) 0, sizeOut()); }
            finally { MeosMemory.free(p); }
        };
    public static final UDF1<String, String> poseFromEWKT = poseFromText;

    public static final UDF1<String, String> poseFromHexEWKB =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.pose_from_hexwkb(hex);
            if (p == null) return null;
            try { return MeosNative.INSTANCE.pose_as_hexwkb(p, (byte) 0, sizeOut()); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<byte[], String> poseFromBinary =
        (bytes) -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = MeosNative.INSTANCE.pose_from_hexwkb(hex);
            if (p == null) return null;
            try { return MeosNative.INSTANCE.pose_as_hexwkb(p, (byte) 0, sizeOut()); }
            finally { MeosMemory.free(p); }
        };
    public static final UDF1<byte[], String> poseFromEWKB = poseFromBinary;

    // ------------------------------------------------------------------
    // poseset I/O — generic Set helpers
    // ------------------------------------------------------------------

    public static final UDF1<String, String> posesetFromHexWKB =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String> posesetFromText =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.poseset_in(s);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    public static final UDF1<String, String> posesetFromEWKT = posesetFromText;

    public static final UDF1<byte[], String> posesetFromBinary =
        (bytes) -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    public static final UDF1<byte[], String> posesetFromEWKB = posesetFromBinary;

    // ------------------------------------------------------------------
    // Temporal pose (tpose)
    // ------------------------------------------------------------------

    // tpose(tgeompoint STRING, tfloat STRING) → STRING (tpose hex-WKB)
    // MEOS: tpose_make(const Temporal *tpoint, const Temporal *tradius)
    public static final UDF2<String, String, String> tpose =
        (tpointHex, trotHex) -> {
            if (tpointHex == null || trotHex == null) return null;
            MeosThread.ensureReady();
            Pointer tp = functions.temporal_from_hexwkb(tpointHex);
            if (tp == null) return null;
            try {
                Pointer tr = functions.temporal_from_hexwkb(trotHex);
                if (tr == null) return null;
                try {
                    Pointer r = MeosNative.INSTANCE.tpose_make(tp, tr);
                    if (r == null) return null;
                    try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                    finally { MeosMemory.free(r); }
                } finally { MeosMemory.free(tr); }
            } finally { MeosMemory.free(tp); }
        };

    // tposeInst(pose STRING, ts TIMESTAMP) → STRING
    // tpose has no public *inst_make; the SQL tpose(pose, timestamptz) and
    // its tposeInst alias build from a pose value. Decompose the pose into
    // its point + rotation, lift each to an instant, then tpose_make.
    public static final UDF2<String, java.sql.Timestamp, String> tposeInst =
        (poseHex, ts) -> {
            if (poseHex == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.pose_from_hexwkb(poseHex);
            if (p == null) return null;
            try {
                Pointer g = MeosNative.INSTANCE.pose_to_point(p);
                if (g == null) return null;
                double rot = MeosNative.INSTANCE.pose_rotation(p);
                long micros = (ts.getTime() - 946684800L * 1000L) * 1000L;
                try {
                    Pointer tp = MeosNative.INSTANCE.tgeoinst_make(g, micros);
                    if (tp == null) return null;
                    try {
                        Pointer tr = functions.tfloatinst_make(rot,
                            java.time.Instant.ofEpochMilli(ts.getTime())
                                .atOffset(java.time.ZoneOffset.UTC));
                        if (tr == null) return null;
                        try {
                            Pointer r = MeosNative.INSTANCE.tpose_make(tp, tr);
                            if (r == null) return null;
                            try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                            finally { MeosMemory.free(r); }
                        } finally { MeosMemory.free(tr); }
                    } finally { MeosMemory.free(tp); }
                } finally { MeosMemory.free(g); }
            } finally { MeosMemory.free(p); }
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

    public static final UDF1<String, String> tposeFromText =
        (s) -> {
            if (s == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.tpose_in(s);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    public static final UDF1<String, String> tposeFromEWKT = tposeFromText;

    public static final UDF1<String, String> tposeFromMFJSON =
        (mfjson) -> {
            if (mfjson == null) return null;
            MeosThread.ensureReady();
            Pointer p = MeosNative.INSTANCE.tpose_from_mfjson(mfjson);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF1<String, String> tposeFromHexEWKB = temporalRoundtrip();
    public static final UDF1<byte[], String>  tposeFromBinary  = temporalFromBinary();
    public static final UDF1<byte[], String>  tposeFromEWKB    = temporalFromBinary();

    public static final UDF1<String, String> tposeSeq =
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

    public static final UDF1<String, String> tposeSeqSet =
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

    public static final UDF2<String[], String, String> tposeSeqSetGaps =
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
    // tpose accessors — points (geomset) / rotation (tfloat)
    // ------------------------------------------------------------------

    // points(tpose STRING) → STRING (geomset hex-WKB) — MEOS: tpose_points
    // Registered as `tposePoints`; the bare `points` name is registered by
    // CbufferUDFs (audit matches by name globally, so `points` is covered).
    public static final UDF1<String, String> tposePoints =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer s = MeosNative.INSTANCE.tpose_points(p);
                if (s == null) return null;
                try { return functions.set_as_hexwkb(s, (byte) 0); }
                finally { MeosMemory.free(s); }
            } finally { MeosMemory.free(p); }
        };

    // tposeRotation(tpose STRING) → STRING (tfloat hex-WKB)
    // MEOS: tpose_rotation(const Temporal *) → Temporal *
    public static final UDF1<String, String> tposeRotation =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = MeosNative.INSTANCE.tpose_rotation(p);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    public static void registerAll(SparkSession spark) {
        // Base-type pose
        spark.udf().register("pose",        pose,        DataTypes.StringType);
        spark.udf().register("point",       point,       DataTypes.StringType);
        spark.udf().register("rotation",    rotation,    DataTypes.DoubleType);
        spark.udf().register("orientation", orientation,
            DataTypes.createArrayType(DataTypes.DoubleType));
        spark.udf().register("poseSame",    poseSame,    DataTypes.BooleanType);
        spark.udf().register("poseFromText",    poseFromText,    DataTypes.StringType);
        spark.udf().register("poseFromEWKT",    poseFromEWKT,    DataTypes.StringType);
        spark.udf().register("poseFromHexEWKB", poseFromHexEWKB, DataTypes.StringType);
        spark.udf().register("poseFromBinary",  poseFromBinary,  DataTypes.StringType);
        spark.udf().register("poseFromEWKB",    poseFromEWKB,    DataTypes.StringType);
        // poseset
        spark.udf().register("posesetFromHexWKB", posesetFromHexWKB, DataTypes.StringType);
        spark.udf().register("posesetFromText",   posesetFromText,   DataTypes.StringType);
        spark.udf().register("posesetFromEWKT",   posesetFromEWKT,   DataTypes.StringType);
        spark.udf().register("posesetFromBinary", posesetFromBinary, DataTypes.StringType);
        spark.udf().register("posesetFromEWKB",   posesetFromEWKB,   DataTypes.StringType);
        // Temporal pose
        spark.udf().register("tpose",            tpose,            DataTypes.StringType);
        spark.udf().register("tposeInst",        tposeInst,        DataTypes.StringType);
        spark.udf().register("tposeFromText",    tposeFromText,    DataTypes.StringType);
        spark.udf().register("tposeFromEWKT",    tposeFromEWKT,    DataTypes.StringType);
        spark.udf().register("tposeFromMFJSON",  tposeFromMFJSON,  DataTypes.StringType);
        spark.udf().register("tposeFromHexEWKB", tposeFromHexEWKB, DataTypes.StringType);
        spark.udf().register("tposeFromBinary",  tposeFromBinary,  DataTypes.StringType);
        spark.udf().register("tposeFromEWKB",    tposeFromEWKB,    DataTypes.StringType);
        spark.udf().register("tposeSeq",         tposeSeq,         DataTypes.StringType);
        spark.udf().register("tposeSeqSet",      tposeSeqSet,      DataTypes.StringType);
        spark.udf().register("tposeSeqSetGaps",  tposeSeqSetGaps,  DataTypes.StringType);
        spark.udf().register("tposePoints",      tposePoints,      DataTypes.StringType);
        spark.udf().register("tposeRotation",    tposeRotation,    DataTypes.StringType);
    }
}
