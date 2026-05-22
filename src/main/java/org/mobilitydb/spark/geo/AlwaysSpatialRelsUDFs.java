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
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import java.util.function.BiFunction;

/**
 * Spark SQL UDFs for "always" spatial relationship predicates: do all
 * times in the temporal value satisfy the relation? Counterpart to the
 * "ever" family in TempSpatialRelsUDFs.
 *
 * MEOS function authority: meos/include/meos_geo.h
 *
 * Native functions return int (-1 = error, 0 = false, 1 = true). The
 * UDF wrappers convert to Boolean (null on error).
 */
public final class AlwaysSpatialRelsUDFs {

    private AlwaysSpatialRelsUDFs() {}

    @FunctionalInterface
    private interface IntBiFn { int apply(Pointer a, Pointer b); }

    @FunctionalInterface
    private interface IntTriFn { int apply(Pointer a, Pointer b, double dist); }

    private static Boolean tri(int v) {
        return v == -1 ? null : v == 1;
    }

    private static UDF2<String, String, Boolean> tgeoGeo(IntBiFn fn) {
        return (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip); if (t == null) return null;
            Pointer g = functions.geo_from_text(geomWkt, 0);
            if (g == null) { MeosMemory.free(t); return null; }
            try { return tri(fn.apply(t, g)); }
            finally { MeosMemory.free(t, g); }
        };
    }

    private static UDF2<String, String, Boolean> tgeoTgeo(IntBiFn fn) {
        return (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1); if (p1 == null) return null;
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return tri(fn.apply(p1, p2)); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    private static UDF2<String, String, Boolean> geoTgeo(IntBiFn fn) {
        return (geomWkt, trip) -> {
            if (geomWkt == null || trip == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(geomWkt, 0); if (g == null) return null;
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) { MeosMemory.free(g); return null; }
            try { return tri(fn.apply(g, t)); }
            finally { MeosMemory.free(g, t); }
        };
    }

    private static UDF3<String, String, Double, Boolean> tgeoGeoDist(IntTriFn fn) {
        return (trip, geomWkt, dist) -> {
            if (trip == null || geomWkt == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip); if (t == null) return null;
            Pointer g = functions.geo_from_text(geomWkt, 0);
            if (g == null) { MeosMemory.free(t); return null; }
            try { return tri(fn.apply(t, g, dist)); }
            finally { MeosMemory.free(t, g); }
        };
    }

    private static UDF3<String, String, Double, Boolean> tgeoTgeoDist(IntTriFn fn) {
        return (trip1, trip2, dist) -> {
            if (trip1 == null || trip2 == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1); if (p1 == null) return null;
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return tri(fn.apply(p1, p2, dist)); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    public static final UDF2<String, String, Boolean> aDisjointTgeoGeo   = tgeoGeo(functions::adisjoint_tgeo_geo);
    public static final UDF2<String, String, Boolean> aDisjointTgeoTgeo  = tgeoTgeo(functions::adisjoint_tgeo_tgeo);
    public static final UDF2<String, String, Boolean> aIntersectsTgeoGeo = tgeoGeo(functions::aintersects_tgeo_geo);
    public static final UDF2<String, String, Boolean> aIntersectsTgeoTgeo = tgeoTgeo(functions::aintersects_tgeo_tgeo);
    public static final UDF2<String, String, Boolean> aTouchesTgeoGeo    = tgeoGeo(functions::atouches_tgeo_geo);
    public static final UDF2<String, String, Boolean> aTouchesTgeoTgeo   = tgeoTgeo(functions::atouches_tgeo_tgeo);
    public static final UDF2<String, String, Boolean> aContainsTgeoGeo   = tgeoGeo(functions::acontains_tgeo_geo);
    public static final UDF2<String, String, Boolean> aContainsTgeoTgeo  = tgeoTgeo(functions::acontains_tgeo_tgeo);
    public static final UDF2<String, String, Boolean> aContainsGeoTgeo   = geoTgeo(functions::acontains_geo_tgeo);
    public static final UDF2<String, String, Boolean> aCoversTgeoGeo     = tgeoGeo(functions::acovers_tgeo_geo);

    public static final UDF3<String, String, Double, Boolean> aDwithinTgeoGeo  = tgeoGeoDist(functions::adwithin_tgeo_geo);
    public static final UDF3<String, String, Double, Boolean> aDwithinTgeoTgeo = tgeoTgeoDist(functions::adwithin_tgeo_tgeo);

    public static void registerAll(SparkSession spark) {
        spark.udf().register("aDisjointTgeoGeo",     aDisjointTgeoGeo,     DataTypes.BooleanType);
        spark.udf().register("aDisjointTgeoTgeo",    aDisjointTgeoTgeo,    DataTypes.BooleanType);
        spark.udf().register("aIntersectsTgeoGeo",   aIntersectsTgeoGeo,   DataTypes.BooleanType);
        spark.udf().register("aIntersectsTgeoTgeo",  aIntersectsTgeoTgeo,  DataTypes.BooleanType);
        spark.udf().register("aTouchesTgeoGeo",      aTouchesTgeoGeo,      DataTypes.BooleanType);
        spark.udf().register("aTouchesTgeoTgeo",     aTouchesTgeoTgeo,     DataTypes.BooleanType);
        spark.udf().register("aContainsTgeoGeo",     aContainsTgeoGeo,     DataTypes.BooleanType);
        spark.udf().register("aContainsTgeoTgeo",    aContainsTgeoTgeo,    DataTypes.BooleanType);
        spark.udf().register("aContainsGeoTgeo",     aContainsGeoTgeo,     DataTypes.BooleanType);
        spark.udf().register("aCoversTgeoGeo",       aCoversTgeoGeo,       DataTypes.BooleanType);
        spark.udf().register("aDwithinTgeoGeo",      aDwithinTgeoGeo,      DataTypes.BooleanType);
        spark.udf().register("aDwithinTgeoTgeo",     aDwithinTgeoTgeo,     DataTypes.BooleanType);
    }
}
