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

package org.mobilitydb.spark.geo;

import functions.functions;
import jnr.ffi.Pointer;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for temporal distance operations between tgeo/tnumber types.
 *
 * All functions return a hex-WKB tfloat (the distance evolving over time).
 * Input geometry is accepted as WKT strings.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
public final class DistanceUDFs {

    private DistanceUDFs() {}

    // ------------------------------------------------------------------
    // Spatial distance — tgeo × geometry
    // ------------------------------------------------------------------

    // tdistanceTgeoGeo(trip STRING, geomWkt STRING) → STRING  (tfloat hex-WKB)
    // MEOS: tdistance_tgeo_geo(const Temporal *, const GSERIALIZED *) → Temporal *
    public static final UDF2<String, String, String> tdistanceTgeoGeo =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gsptr = functions.geo_from_text(geomWkt, 0);
            if (gsptr == null) { MeosMemory.free(tptr); return null; }
            try {
                Pointer r = functions.tdistance_tgeo_geo(tptr, gsptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(tptr);
                MeosMemory.free(gsptr);
            }
        };

    // ------------------------------------------------------------------
    // Spatial distance — tgeo × tgeo
    // ------------------------------------------------------------------

    // tdistanceTgeoTgeo(trip1 STRING, trip2 STRING) → STRING  (tfloat hex-WKB)
    // MEOS: tdistance_tgeo_tgeo(const Temporal *, const Temporal *) → Temporal *
    public static final UDF2<String, String, String> tdistanceTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = functions.tdistance_tgeo_tgeo(p1, p2);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(p1);
                MeosMemory.free(p2);
            }
        };

    // ------------------------------------------------------------------
    // Number distance — tfloat × float
    // ------------------------------------------------------------------

    // tdistanceTfloatFloat(tfloat STRING, d DOUBLE) → STRING  (tfloat hex-WKB)
    // MEOS: tdistance_tfloat_float(const Temporal *, double) → Temporal *
    public static final UDF2<String, Double, String> tdistanceTfloatFloat =
        (hex, d) -> {
            if (hex == null || d == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            try {
                Pointer r = functions.tdistance_tfloat_float(ptr, d);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Number distance — tint × int
    // ------------------------------------------------------------------

    // tdistanceTintInt(tint STRING, i INT) → STRING  (tint hex-WKB)
    // MEOS: tdistance_tint_int(const Temporal *, int) → Temporal *
    public static final UDF2<String, Integer, String> tdistanceTintInt =
        (hex, i) -> {
            if (hex == null || i == null) return null;
            MeosThread.ensureReady();
            Pointer ptr = functions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            try {
                Pointer r = functions.tdistance_tint_int(ptr, i);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(ptr);
            }
        };

    // ------------------------------------------------------------------
    // Number distance — tnumber × tnumber
    // ------------------------------------------------------------------

    // tdistanceTnumberTnumber(t1 STRING, t2 STRING) → STRING  (tfloat hex-WKB)
    // MEOS: tdistance_tnumber_tnumber(const Temporal *, const Temporal *) → Temporal *
    public static final UDF2<String, String, String> tdistanceTnumberTnumber =
        (hex1, hex2) -> {
            if (hex1 == null || hex2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(hex1);
            if (p1 == null) return null;
            Pointer p2 = functions.temporal_from_hexwkb(hex2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = functions.tdistance_tnumber_tnumber(p1, p2);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(p1);
                MeosMemory.free(p2);
            }
        };

    // ------------------------------------------------------------------
    // Nearest approach distance (NAD) — returns Double (null on failure)
    // MEOS returns DBL_MAX when the inputs never approach; map to null.
    // ------------------------------------------------------------------

    // nadTgeoGeo(trip STRING, geomWkt STRING) → DOUBLE
    // MEOS: nad_tgeo_geo(const Temporal *, const GSERIALIZED *) → double
    public static final UDF2<String, String, Double> nadTgeoGeo =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gsptr = functions.geo_from_text(geomWkt, 0);
            if (gsptr == null) { MeosMemory.free(tptr); return null; }
            try {
                double d = functions.nad_tgeo_geo(tptr, gsptr);
                return d == Double.MAX_VALUE ? null : d;
            } finally {
                MeosMemory.free(tptr);
                MeosMemory.free(gsptr);
            }
        };

    // nadTgeoStbox(trip STRING, stboxHex STRING) → DOUBLE
    // MEOS: nad_tgeo_stbox(const Temporal *, const STBox *) → double
    public static final UDF2<String, String, Double> nadTgeoStbox =
        (trip, stboxHex) -> {
            if (trip == null || stboxHex == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer sptr = functions.stbox_from_hexwkb(stboxHex);
            if (sptr == null) { MeosMemory.free(tptr); return null; }
            try {
                double d = functions.nad_tgeo_stbox(tptr, sptr);
                return d == Double.MAX_VALUE ? null : d;
            } finally {
                MeosMemory.free(tptr);
                MeosMemory.free(sptr);
            }
        };

    // nadTgeoTgeo(trip1 STRING, trip2 STRING) → DOUBLE
    // MEOS: nad_tgeo_tgeo(const Temporal *, const Temporal *) → double
    public static final UDF2<String, String, Double> nadTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                double d = functions.nad_tgeo_tgeo(p1, p2);
                return d == Double.MAX_VALUE ? null : d;
            } finally {
                MeosMemory.free(p1);
                MeosMemory.free(p2);
            }
        };

    // ------------------------------------------------------------------
    // Nearest approach instant (NAI) — returns hex-WKB TInstant (STRING)
    // ------------------------------------------------------------------

    // naiTgeoGeo(trip STRING, geomWkt STRING) → STRING  (TInstant hex-WKB)
    // MEOS: nai_tgeo_geo(const Temporal *, const GSERIALIZED *) → TInstant *
    public static final UDF2<String, String, String> naiTgeoGeo =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gsptr = functions.geo_from_text(geomWkt, 0);
            if (gsptr == null) { MeosMemory.free(tptr); return null; }
            try {
                Pointer r = functions.nai_tgeo_geo(tptr, gsptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(tptr);
                MeosMemory.free(gsptr);
            }
        };

    // ------------------------------------------------------------------
    // Shortest line — returns geometry (WKT) of the closest-approach segment
    // ------------------------------------------------------------------

    // shortestLineTgeoGeo(trip STRING, geomWkt STRING) → STRING  (WKT geometry)
    // MEOS: shortestline_tgeo_geo(const Temporal *, const GSERIALIZED *) → GSERIALIZED *
    public static final UDF2<String, String, String> shortestLineTgeoGeo =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gsptr = functions.geo_from_text(geomWkt, 0);
            if (gsptr == null) { MeosMemory.free(tptr); return null; }
            try {
                Pointer r = functions.shortestline_tgeo_geo(tptr, gsptr);
                if (r == null) return null;
                try {
                    return functions.geo_as_text(r, 6);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(tptr);
                MeosMemory.free(gsptr);
            }
        };

    // shortestLineTgeoTgeo(trip1 STRING, trip2 STRING) → STRING  (WKT geometry)
    // MEOS: shortestline_tgeo_tgeo(const Temporal *, const Temporal *) → GSERIALIZED *
    public static final UDF2<String, String, String> shortestLineTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = functions.shortestline_tgeo_tgeo(p1, p2);
                if (r == null) return null;
                try {
                    return functions.geo_as_text(r, 6);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(p1);
                MeosMemory.free(p2);
            }
        };

    // naiTgeoTgeo(trip1 STRING, trip2 STRING) → STRING  (TInstant hex-WKB)
    // MEOS: nai_tgeo_tgeo(const Temporal *, const Temporal *) → TInstant *
    public static final UDF2<String, String, String> naiTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = functions.nai_tgeo_tgeo(p1, p2);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(p1);
                MeosMemory.free(p2);
            }
        };

    public static void registerAll(SparkSession spark) {
        spark.udf().register("tdistanceTgeoGeo",       tdistanceTgeoGeo,       DataTypes.StringType);
        spark.udf().register("tdistanceTgeoTgeo",      tdistanceTgeoTgeo,      DataTypes.StringType);
        spark.udf().register("tdistanceTfloatFloat",   tdistanceTfloatFloat,   DataTypes.StringType);
        spark.udf().register("tdistanceTintInt",       tdistanceTintInt,       DataTypes.StringType);
        spark.udf().register("tdistanceTnumberTnumber", tdistanceTnumberTnumber, DataTypes.StringType);

        spark.udf().register("nadTgeoGeo",           nadTgeoGeo,           DataTypes.DoubleType);
        spark.udf().register("nadTgeoStbox",          nadTgeoStbox,          DataTypes.DoubleType);
        spark.udf().register("nadTgeoTgeo",           nadTgeoTgeo,           DataTypes.DoubleType);
        spark.udf().register("naiTgeoGeo",            naiTgeoGeo,            DataTypes.StringType);
        spark.udf().register("naiTgeoTgeo",           naiTgeoTgeo,           DataTypes.StringType);
        spark.udf().register("shortestLineTgeoGeo",   shortestLineTgeoGeo,   DataTypes.StringType);
        spark.udf().register("shortestLineTgeoTgeo",  shortestLineTgeoTgeo,  DataTypes.StringType);

        // MobilityDB SQL bare-name aliases
        spark.udf().register("nearestApproachDistance", nadTgeoGeo,  DataTypes.DoubleType);
        spark.udf().register("nearestApproachInstant",  naiTgeoGeo,  DataTypes.StringType);
        spark.udf().register("shortestLine",            shortestLineTgeoGeo, DataTypes.StringType);
    }
}
