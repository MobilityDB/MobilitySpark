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

import functions.functions;
import jnr.ffi.Pointer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

/**
 * Spark UDFs for the H3 prefilter surface used by the BerlinMOD
 * th3index variant queries.  The MEOS symbols are bound directly via
 * JNR-FFI (see {@link H3IndexJnrBindings}) so this works against
 * any libmeos.so that exports the four h3 prefilter functions,
 * independent of whether the JMEOS jar generator supports H3Index.
 *
 * UDFs:
 *
 *   tgeompointToTh3index(STRING tgeompoint_hex, INT resolution) -> STRING th3index_hex
 *   geoToH3IndexSet(STRING ewkb_hex, INT resolution) -> STRING h3indexset_hex
 *   everEqTh3IndexTh3Index(STRING th3index1_hex, STRING th3index2_hex) -> BOOLEAN
 *   everIntersectsH3IndexSetTh3Index(STRING h3indexset_hex, STRING th3index_hex) -> BOOLEAN
 *
 * Each STRING is the MEOS hex-WKB encoding of the underlying value.
 */
public final class Th3IndexPrefilterUDFs {

    private Th3IndexPrefilterUDFs() {}

    /* tgeompoint hex-WKB -> th3index hex-WKB at the given resolution. */
    public static final UDF2<String, Integer, String> tgeompointToTh3index =
        (tripHex, resolution) -> {
            if (tripHex == null || resolution == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(tripHex);
            if (t == null) return null;
            try {
                Pointer th3 = H3IndexJnrBindings.LIB.tgeompoint_to_th3index(t, resolution);
                if (th3 == null) return null;
                try {
                    return functions.temporal_as_hexwkb(th3, (byte) 0);
                } finally {
                    MeosMemory.free(th3);
                }
            } finally {
                MeosMemory.free(t);
            }
        };

    /* static geometry WKT -> h3indexset hex-WKB at the given resolution.
     * Caller passes the geometry's WKT in EPSG:4326 coordinates (e.g.
     * `ST_AsText(ST_Transform(g, 4326))`).  The MEOS GSERIALIZED carries
     * the SRID via the second argument to geo_from_text (4326 here, since
     * H3 cells are inherently geographic). */
    public static final UDF2<String, Integer, String> geoToH3IndexSet =
        (geoWkt, resolution) -> {
            if (geoWkt == null || resolution == null) return null;
            MeosThread.ensureReady();
            Pointer gs = functions.geo_from_text(geoWkt, 4326);
            if (gs == null) return null;
            try {
                Pointer set = H3IndexJnrBindings.LIB.geo_to_h3index_set(gs, resolution);
                if (set == null) return null;
                try {
                    return functions.set_as_hexwkb(set, (byte) 0);
                } finally {
                    MeosMemory.free(set);
                }
            } finally {
                MeosMemory.free(gs);
            }
        };

    /* trip x trip h3 prefilter: do two trip H3 cell sequences share a cell at the same instant? */
    public static final UDF2<String, String, Boolean> everEqTh3IndexTh3Index =
        (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer t1 = functions.temporal_from_hexwkb(h1);
            if (t1 == null) return null;
            try {
                Pointer t2 = functions.temporal_from_hexwkb(h2);
                if (t2 == null) return null;
                try {
                    int r = H3IndexJnrBindings.LIB.ever_eq_th3index_th3index(t1, t2);
                    return r == 1;
                } finally {
                    MeosMemory.free(t2);
                }
            } finally {
                MeosMemory.free(t1);
            }
        };

    /* trip x static h3 prefilter: does the trip H3 sequence ever pass through any
     * cell of a static H3 set? */
    public static final UDF2<String, String, Boolean> everIntersectsH3IndexSetTh3Index =
        (setHex, tripH3Hex) -> {
            if (setHex == null || tripH3Hex == null) return null;
            MeosThread.ensureReady();
            Pointer set = functions.set_from_hexwkb(setHex);
            if (set == null) return null;
            try {
                Pointer t = functions.temporal_from_hexwkb(tripH3Hex);
                if (t == null) return null;
                try {
                    int r = H3IndexJnrBindings.LIB.ever_eq_anyof_h3indexset_th3index(set, t);
                    return r == 1;
                } finally {
                    MeosMemory.free(t);
                }
            } finally {
                MeosMemory.free(set);
            }
        };

    /** Register all four prefilter UDFs on the given Spark session. */
    public static void registerAll(SparkSession spark) {
        spark.udf().register("tgeompointToTh3index",          tgeompointToTh3index,          DataTypes.StringType);
        spark.udf().register("geoToH3IndexSet",                geoToH3IndexSet,                DataTypes.StringType);
        spark.udf().register("everEqTh3IndexTh3Index",         everEqTh3IndexTh3Index,         DataTypes.BooleanType);
        spark.udf().register("everIntersectsH3IndexSetTh3Index", everIntersectsH3IndexSetTh3Index, DataTypes.BooleanType);
    }
}
