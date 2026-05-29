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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.mobilitydb.spark.util.TimeUtil;

/**
 * Spark SQL UDFs for typed temporal-instant constructors and typed
 * subtype-conversion aliases (tboolSeq → temporalToTsequence, etc.).
 *
 * MEOS function authority: meos/include/meos.h — *inst_make,
 *   temporal_to_tinstant/_to_tsequence/_to_tsequenceset.
 */
public final class SubtypeConstructorUDFs {

    private SubtypeConstructorUDFs() {}

    private static OffsetDateTime toOdt(Timestamp ts) {
        return Instant.ofEpochMilli(ts.getTime()).atOffset(ZoneOffset.UTC);
    }

    // ------------------------------------------------------------------
    // Per-type Inst constructors — (value, timestamp) → temporal instant
    // ------------------------------------------------------------------

    public static final UDF2<Boolean, Timestamp, String> tboolInst =
        (v, ts) -> {
            if (v == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tboolinst_make(v, toOdt(ts));
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF2<Integer, Timestamp, String> tintInst =
        (v, ts) -> {
            if (v == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tintinst_make(v, toOdt(ts));
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    public static final UDF2<Double, Timestamp, String> tfloatInst =
        (v, ts) -> {
            if (v == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tfloatinst_make(v, toOdt(ts));
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };

    // tpointInst((point WKT), timestamp) — used for tgeompoint/tgeogpoint
    public static final UDF2<String, Timestamp, String> tgeompointInst =
        (geomWkt, ts) -> {
            if (geomWkt == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(geomWkt, 0);
            if (g == null) return null;
            try {
                Pointer p = functions.tpointinst_make(g, toOdt(ts));
                if (p == null) return null;
                try { return functions.temporal_as_hexwkb(p, (byte) 0); }
                finally { MeosMemory.free(p); }
            } finally { MeosMemory.free(g); }
        };

    public static final UDF2<String, Timestamp, String> tgeogpointInst = tgeompointInst;

    // tgeometryInst((geometry WKT), timestamp) — for general geometry, not just points
    public static final UDF2<String, Timestamp, String> tgeometryInst =
        (geomWkt, ts) -> {
            if (geomWkt == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(geomWkt, 0);
            if (g == null) return null;
            try {
                Pointer p = org.mobilitydb.spark.MeosNative.INSTANCE
                    .tgeoinst_make(g, toPgEpochMicros(ts));
                if (p == null) return null;
                try { return functions.temporal_as_hexwkb(p, (byte) 0); }
                finally { MeosMemory.free(p); }
            } finally { MeosMemory.free(g); }
        };

    public static final UDF2<String, Timestamp, String> tgeographyInst = tgeometryInst;

    public static final UDF2<String, Timestamp, String> ttextInst =
        (v, ts) -> {
            if (v == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer txt = functions.cstring2text(v);
            if (txt == null) return null;
            try {
                Pointer p = functions.ttextinst_make(txt, toOdt(ts));
                if (p == null) return null;
                try { return functions.temporal_as_hexwkb(p, (byte) 0); }
                finally { MeosMemory.free(p); }
            } finally { MeosMemory.free(txt); }
        };

    // ------------------------------------------------------------------
    // Typed Seq / SeqSet aliases — call the generic conversion with a
    // default 'linear' interpolation.
    // ------------------------------------------------------------------

    // interpType enum (meos.h): INTERP_NONE=0, DISCRETE=1, STEP=2, LINEAR=3
    private static final int INTERP_LINEAR = 3;

    private static UDF1<String, String> seqAlias() {
        return hex -> {
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
    }

    private static UDF1<String, String> seqSetAlias() {
        return hex -> {
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
    }

    public static final UDF1<String, String> tboolSeq      = seqAlias();
    public static final UDF1<String, String> tintSeq       = seqAlias();
    public static final UDF1<String, String> tfloatSeq     = seqAlias();
    public static final UDF1<String, String> ttextSeq      = seqAlias();
    public static final UDF1<String, String> tgeompointSeq = seqAlias();
    public static final UDF1<String, String> tgeogpointSeq = seqAlias();
    public static final UDF1<String, String> tgeometrySeq  = seqAlias();
    public static final UDF1<String, String> tgeographySeq = seqAlias();

    public static final UDF1<String, String> tboolSeqSet      = seqSetAlias();
    public static final UDF1<String, String> tintSeqSet       = seqSetAlias();
    public static final UDF1<String, String> tfloatSeqSet     = seqSetAlias();
    public static final UDF1<String, String> ttextSeqSet      = seqSetAlias();
    public static final UDF1<String, String> tgeompointSeqSet = seqSetAlias();
    public static final UDF1<String, String> tgeogpointSeqSet = seqSetAlias();
    public static final UDF1<String, String> tgeometrySeqSet  = seqSetAlias();
    public static final UDF1<String, String> tgeographySeqSet = seqSetAlias();

    // ------------------------------------------------------------------
    // Accessor aliases (MobilityDB SQL bare names)
    // ------------------------------------------------------------------

    // temporal subtype label (Inst | Seq | SeqSet)
    public static final UDF1<String, String> tempSubtype =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.temporal_subtype(p); }
            finally { MeosMemory.free(p); }
        };

    // memory size of the serialised temporal value
    public static final UDF1<String, Integer> memSize =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try { return org.mobilitydb.spark.MeosNative.INSTANCE.temporal_mem_size(p); }
            finally { MeosMemory.free(p); }
        };

    // getTime → temporal_time → SpanSet hex
    public static final UDF1<String, String> getTime =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer ss = functions.temporal_time(p);
                if (ss == null) return null;
                try { return functions.spanset_as_hexwkb(ss, (byte) 0); }
                finally { MeosMemory.free(ss); }
            } finally { MeosMemory.free(p); }
        };

    // deleteTime(temporal, ts) → temporal with the timestamp removed (connect=true)
    public static final UDF2<String, Timestamp, String> deleteTime =
        (hex, ts) -> {
            if (hex == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = functions.temporal_delete_timestamptz(p, toOdt(ts), true);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    private static long toPgEpochMicros(Timestamp ts) {
        return (ts.getTime() - TimeUtil.PG_UNIX_EPOCH_OFFSET_MS) * 1000L;
    }

    // beforeTimestamp(temporal, ts) → temporal restricted to before ts
    public static final UDF2<String, Timestamp, String> beforeTimestamp =
        (hex, ts) -> {
            if (hex == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = org.mobilitydb.spark.MeosNative.INSTANCE
                    .temporal_before_timestamptz(p, toPgEpochMicros(ts));
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    public static final UDF2<String, Timestamp, String> afterTimestamp =
        (hex, ts) -> {
            if (hex == null || ts == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer r = org.mobilitydb.spark.MeosNative.INSTANCE
                    .temporal_after_timestamptz(p, toPgEpochMicros(ts));
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(p); }
        };

    public static void registerAll(SparkSession spark) {
        // Inst constructors
        spark.udf().register("tboolInst",  tboolInst,  DataTypes.StringType);
        spark.udf().register("tintInst",   tintInst,   DataTypes.StringType);
        spark.udf().register("tfloatInst", tfloatInst, DataTypes.StringType);
        spark.udf().register("ttextInst",  ttextInst,  DataTypes.StringType);
        spark.udf().register("tgeompointInst", tgeompointInst, DataTypes.StringType);
        spark.udf().register("tgeogpointInst", tgeogpointInst, DataTypes.StringType);
        spark.udf().register("tgeometryInst",  tgeometryInst,  DataTypes.StringType);
        spark.udf().register("tgeographyInst", tgeographyInst, DataTypes.StringType);

        // Seq aliases
        spark.udf().register("tboolSeq",      tboolSeq,      DataTypes.StringType);
        spark.udf().register("tintSeq",       tintSeq,       DataTypes.StringType);
        spark.udf().register("tfloatSeq",     tfloatSeq,     DataTypes.StringType);
        spark.udf().register("ttextSeq",      ttextSeq,      DataTypes.StringType);
        spark.udf().register("tgeompointSeq", tgeompointSeq, DataTypes.StringType);
        spark.udf().register("tgeogpointSeq", tgeogpointSeq, DataTypes.StringType);
        spark.udf().register("tgeometrySeq",  tgeometrySeq,  DataTypes.StringType);
        spark.udf().register("tgeographySeq", tgeographySeq, DataTypes.StringType);

        // SeqSet aliases
        spark.udf().register("tboolSeqSet",      tboolSeqSet,      DataTypes.StringType);
        spark.udf().register("tintSeqSet",       tintSeqSet,       DataTypes.StringType);
        spark.udf().register("tfloatSeqSet",     tfloatSeqSet,     DataTypes.StringType);
        spark.udf().register("ttextSeqSet",      ttextSeqSet,      DataTypes.StringType);
        spark.udf().register("tgeompointSeqSet", tgeompointSeqSet, DataTypes.StringType);
        spark.udf().register("tgeogpointSeqSet", tgeogpointSeqSet, DataTypes.StringType);
        spark.udf().register("tgeometrySeqSet",  tgeometrySeqSet,  DataTypes.StringType);
        spark.udf().register("tgeographySeqSet", tgeographySeqSet, DataTypes.StringType);

        // Accessor aliases
        spark.udf().register("tempSubtype", tempSubtype, DataTypes.StringType);
        spark.udf().register("memSize",     memSize,     DataTypes.IntegerType);
        spark.udf().register("getTime",     getTime,     DataTypes.StringType);
        spark.udf().register("deleteTime",      deleteTime,      DataTypes.StringType);
        spark.udf().register("beforeTimestamp", beforeTimestamp, DataTypes.StringType);
        spark.udf().register("afterTimestamp",  afterTimestamp,  DataTypes.StringType);
    }
}
