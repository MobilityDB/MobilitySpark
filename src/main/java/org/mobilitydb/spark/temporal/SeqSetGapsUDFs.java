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

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for the SeqSetGaps constructor family — long-standing
 * user request from MobilityDB issue #187.
 *
 * Constructs a temporal sequence-set from an array of temporal instants
 * accounting for gaps: consecutive instants further apart than {@code maxt}
 * (in time) or {@code maxdist} (in value space, for tnumber/tpoint) start
 * a new sequence within the resulting sequence-set.
 *
 * MEOS function authority: tsequenceset_make_gaps in meos/include/meos.h.
 *
 * Per-type variants set the default interpolation:
 *   - tbool/tint/ttext: STEP (=2)
 *   - tfloat/tgeompoint/tgeogpoint/tgeometry/tgeography: LINEAR (=3)
 */
public final class SeqSetGapsUDFs {

    private SeqSetGapsUDFs() {}

    private static final int INTERP_STEP   = 2;
    private static final int INTERP_LINEAR = 3;

    /**
     * Core builder: deserialise N temporal-instant hex strings, pack their
     * pointers into a native TInstant** buffer, call tsequenceset_make_gaps,
     * serialise the result, free everything.
     */
    private static String build(String[] instants, String maxtStr, double maxdist, int interp) {
        if (instants == null || instants.length == 0) return null;
        MeosThread.ensureReady();

        int n = instants.length;
        Pointer[] insts = new Pointer[n];
        try {
            for (int i = 0; i < n; i++) {
                if (instants[i] == null) return null;
                insts[i] = GeneratedFunctions.temporal_from_hexwkb(instants[i]);
                if (insts[i] == null) return null;
            }
            Pointer buf = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8L * n);
            for (int i = 0; i < n; i++) {
                buf.putAddress(i * 8L, insts[i].address());
            }
            Pointer maxt = (maxtStr == null) ? null : GeneratedFunctions.pg_interval_in(maxtStr, -1);
            try {
                Pointer r = GeneratedFunctions.tsequenceset_make_gaps(buf, n, interp, maxt, maxdist);
                if (r == null) return null;
                try { return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0); }
                finally { MeosMemory.free(r); }
            } finally {
                if (maxt != null) MeosMemory.free(maxt);
            }
        } finally {
            for (Pointer p : insts) {
                if (p != null) MeosMemory.free(p);
            }
        }
    }

    // tboolSeqSetGaps(tbool[], maxt) — STEP interpolation, no maxdist
    public static final UDF2<String[], String, String> tboolSeqSetGaps =
        (instants, maxt) -> build(instants, maxt, -1.0, INTERP_STEP);

    // ttextSeqSetGaps(ttext[], maxt) — STEP, no maxdist
    public static final UDF2<String[], String, String> ttextSeqSetGaps =
        (instants, maxt) -> build(instants, maxt, -1.0, INTERP_STEP);

    // tintSeqSetGaps(tint[], maxt, maxdist) — STEP, optional maxdist
    public static final UDF3<String[], String, Double, String> tintSeqSetGaps =
        (instants, maxt, maxdist) -> build(instants, maxt,
            maxdist == null ? -1.0 : maxdist, INTERP_STEP);

    // tfloatSeqSetGaps(tfloat[], maxt, maxdist, interpStr) — LINEAR default
    public static final UDF4<String[], String, Double, String, String> tfloatSeqSetGaps =
        (instants, maxt, maxdist, interpStr) -> build(instants, maxt,
            maxdist == null ? -1.0 : maxdist, parseInterp(interpStr, INTERP_LINEAR));

    // tgeompointSeqSetGaps / tgeogpointSeqSetGaps / tgeometrySeqSetGaps /
    // tgeographySeqSetGaps — LINEAR default, optional maxdist + interp string
    public static final UDF4<String[], String, Double, String, String> tgeompointSeqSetGaps =
        (instants, maxt, maxdist, interpStr) -> build(instants, maxt,
            maxdist == null ? -1.0 : maxdist, parseInterp(interpStr, INTERP_LINEAR));

    public static final UDF4<String[], String, Double, String, String> tgeogpointSeqSetGaps = tgeompointSeqSetGaps;
    public static final UDF4<String[], String, Double, String, String> tgeometrySeqSetGaps  = tgeompointSeqSetGaps;
    public static final UDF4<String[], String, Double, String, String> tgeographySeqSetGaps = tgeompointSeqSetGaps;

    private static int parseInterp(String s, int dflt) {
        if (s == null) return dflt;
        switch (s.toLowerCase()) {
            case "linear":   return INTERP_LINEAR;
            case "step":     return INTERP_STEP;
            case "discrete": return 1;
            default:         return dflt;
        }
    }

    public static void registerAll(SparkSession spark) {
        spark.udf().register("tboolSeqSetGaps",       tboolSeqSetGaps,       DataTypes.StringType);
        spark.udf().register("ttextSeqSetGaps",       ttextSeqSetGaps,       DataTypes.StringType);
        spark.udf().register("tintSeqSetGaps",        tintSeqSetGaps,        DataTypes.StringType);
        spark.udf().register("tfloatSeqSetGaps",      tfloatSeqSetGaps,      DataTypes.StringType);
        spark.udf().register("tgeompointSeqSetGaps",  tgeompointSeqSetGaps,  DataTypes.StringType);
        spark.udf().register("tgeogpointSeqSetGaps",  tgeogpointSeqSetGaps,  DataTypes.StringType);
        spark.udf().register("tgeometrySeqSetGaps",   tgeometrySeqSetGaps,   DataTypes.StringType);
        spark.udf().register("tgeographySeqSetGaps",  tgeographySeqSetGaps,  DataTypes.StringType);
    }
}
