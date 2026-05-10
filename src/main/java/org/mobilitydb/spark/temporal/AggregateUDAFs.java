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
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * Spark SQL UDAFs (typed Aggregators) for temporal aggregate functions.
 *
 * Each UDAF collects hex-WKB strings from each row into a newline-delimited
 * buffer (BUF = String).  The actual MEOS aggregation runs inside finish()
 * by replaying the transfn over each collected value and calling finalfn.
 * This design keeps the buffer serializable between Spark stages while still
 * using the correct MEOS aggregate semantics.
 *
 * MEOS function authority: meos/include/meos.h (temporal aggregate transfns)
 *
 * Registration: call registerAll(spark).  In SQL, use tCount(col),
 * tAnd(col), tOr(col), tIntMin(col), tIntMax(col), tIntSum(col),
 * tFloatMin(col), tFloatMax(col), tFloatSum(col), tTextMin(col),
 * tTextMax(col), tCentroid(col), tExtent(col).
 */
public final class AggregateUDAFs {

    private AggregateUDAFs() {}

    // ------------------------------------------------------------------
    // Shared helpers
    // ------------------------------------------------------------------

    /** Split buffer on newlines; skip blank entries. */
    private static String[] entries(String buf) {
        if (buf == null || buf.isBlank()) return new String[0];
        return buf.split("\n");
    }

    private static String append(String buf, String hex) {
        if (hex == null || hex.isBlank()) return buf;
        if (buf == null || buf.isBlank()) return hex;
        return buf + "\n" + hex;
    }

    private static String merge(String b1, String b2) {
        if (b1 == null || b1.isBlank()) return b2;
        if (b2 == null || b2.isBlank()) return b1;
        return b1 + "\n" + b2;
    }

    /** Serialize a temporal Pointer to hex-WKB and free it. */
    private static String hexOut(Pointer r) {
        if (r == null) return null;
        try {
            return functions.temporal_as_hexwkb(r, (byte) 0);
        } finally {
            MeosMemory.free(r);
        }
    }

    /** Serialize an STBox Pointer to hex-WKB and free it. */
    private static String stboxHex(Pointer p) {
        if (p == null) return null;
        try {
            Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
            return functions.stbox_as_hexwkb(p, (byte) 0, sizeOut);
        } finally {
            MeosMemory.free(p);
        }
    }

    // ------------------------------------------------------------------
    // tCount — count how many temporal values are defined at each instant
    // Returns: tint hex-WKB
    // MEOS: temporal_tcount_transfn + temporal_tagg_finalfn
    // ------------------------------------------------------------------

    public static final class TCountAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }

        @Override public String reduce(String buf, String hex) {
            return append(buf, hex);
        }

        @Override public String merge(String b1, String b2) {
            return AggregateUDAFs.merge(b1, b2);
        }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.temporal_tcount_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    // ------------------------------------------------------------------
    // tAnd — temporal AND over tbool values
    // Returns: tbool hex-WKB
    // MEOS: tbool_tand_transfn + temporal_tagg_finalfn
    // ------------------------------------------------------------------

    public static final class TAndAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tbool_tand_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    // ------------------------------------------------------------------
    // tOr — temporal OR over tbool values
    // Returns: tbool hex-WKB
    // MEOS: tbool_tor_transfn + temporal_tagg_finalfn
    // ------------------------------------------------------------------

    public static final class TOrAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tbool_tor_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    // ------------------------------------------------------------------
    // tIntMin / tIntMax / tIntSum — temporal aggregates on tint
    // Returns: tint hex-WKB
    // ------------------------------------------------------------------

    public static final class TIntMinAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tint_tmin_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    public static final class TIntMaxAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tint_tmax_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    public static final class TIntSumAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tint_tsum_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    // ------------------------------------------------------------------
    // tFloatMin / tFloatMax / tFloatSum — temporal aggregates on tfloat
    // Returns: tfloat hex-WKB
    // ------------------------------------------------------------------

    public static final class TFloatMinAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tfloat_tmin_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    public static final class TFloatMaxAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tfloat_tmax_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    public static final class TFloatSumAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tfloat_tsum_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    // ------------------------------------------------------------------
    // tTextMin / tTextMax — temporal aggregates on ttext
    // Returns: ttext hex-WKB
    // ------------------------------------------------------------------

    public static final class TTextMinAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.ttext_tmin_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    public static final class TTextMaxAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.ttext_tmax_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.temporal_tagg_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    // ------------------------------------------------------------------
    // tCentroid — temporal centroid over tpoint values
    // Returns: tpoint hex-WKB (the moving centroid of the input points)
    // MEOS: tpoint_tcentroid_transfn + tpoint_tcentroid_finalfn
    // ------------------------------------------------------------------

    public static final class TCentroidAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tpoint_tcentroid_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            if (state == null) return null;
            return hexOut(functions.tpoint_tcentroid_finalfn(state));
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    // ------------------------------------------------------------------
    // tExtent — bounding STBox over all tpoint values
    // Returns: stbox hex-WKB
    // MEOS: tspatial_extent_transfn (state is STBox*, not SkipList*)
    // ------------------------------------------------------------------

    public static final class TExtentAgg extends Aggregator<String, String, String>
            implements Serializable {
        @Override public String zero() { return ""; }
        @Override public String reduce(String buf, String hex) { return append(buf, hex); }
        @Override public String merge(String b1, String b2) { return AggregateUDAFs.merge(b1, b2); }

        @Override public String finish(String buf) {
            MeosThread.ensureReady();
            String[] hexes = entries(buf);
            if (hexes.length == 0) return null;
            Pointer state = null;
            for (String hex : hexes) {
                Pointer inp = functions.temporal_from_hexwkb(hex);
                if (inp == null) continue;
                Pointer next = functions.tspatial_extent_transfn(state, inp);
                MeosMemory.free(inp);
                state = next;
            }
            return stboxHex(state);
        }

        @Override public Encoder<String> bufferEncoder() { return Encoders.STRING(); }
        @Override public Encoder<String> outputEncoder() { return Encoders.STRING(); }
    }

    // ------------------------------------------------------------------
    // REGISTRATION
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        spark.udf().register("tCount",    org.apache.spark.sql.functions.udaf(new TCountAgg(),    Encoders.STRING()));
        spark.udf().register("tAnd",      org.apache.spark.sql.functions.udaf(new TAndAgg(),      Encoders.STRING()));
        spark.udf().register("tOr",       org.apache.spark.sql.functions.udaf(new TOrAgg(),       Encoders.STRING()));
        spark.udf().register("tIntMin",   org.apache.spark.sql.functions.udaf(new TIntMinAgg(),   Encoders.STRING()));
        spark.udf().register("tIntMax",   org.apache.spark.sql.functions.udaf(new TIntMaxAgg(),   Encoders.STRING()));
        spark.udf().register("tIntSum",   org.apache.spark.sql.functions.udaf(new TIntSumAgg(),   Encoders.STRING()));
        spark.udf().register("tFloatMin", org.apache.spark.sql.functions.udaf(new TFloatMinAgg(), Encoders.STRING()));
        spark.udf().register("tFloatMax", org.apache.spark.sql.functions.udaf(new TFloatMaxAgg(), Encoders.STRING()));
        spark.udf().register("tFloatSum", org.apache.spark.sql.functions.udaf(new TFloatSumAgg(), Encoders.STRING()));
        spark.udf().register("tTextMin",  org.apache.spark.sql.functions.udaf(new TTextMinAgg(),  Encoders.STRING()));
        spark.udf().register("tTextMax",  org.apache.spark.sql.functions.udaf(new TTextMaxAgg(),  Encoders.STRING()));
        spark.udf().register("tCentroid", org.apache.spark.sql.functions.udaf(new TCentroidAgg(), Encoders.STRING()));
        spark.udf().register("tExtent",   org.apache.spark.sql.functions.udaf(new TExtentAgg(),   Encoders.STRING()));
    }
}
