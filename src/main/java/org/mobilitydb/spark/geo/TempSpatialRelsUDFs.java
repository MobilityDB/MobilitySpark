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

/**
 * Spark SQL UDFs for temporal spatial relationships on tpoint.
 *
 * These UDFs return a tbool (encoded as hex-WKB STRING) that is true at each
 * instant where the spatial relationship holds.  This complements the "ever"
 * predicates in GeoUDFs (eIntersects, eContains, eDwithin) which return a
 * scalar Boolean.
 *
 * Covered relationships:
 *   tDisjoint   — tgeompoint is disjoint from geometry at each instant
 *   tIntersects — tgeompoint intersects geometry at each instant
 *   tTouches    — tgeompoint touches geometry at each instant
 *
 * (tContains, tCovers, tDwithin are already provided in GeoAnalyticsUDFs.)
 *
 * Storage convention:
 *   tgeompoint  → hex-WKB STRING  (temporal_as_hexwkb)
 *   geometry    → WKT STRING      (geo_from_text with SRID from trip bbox)
 *   tbool result→ hex-WKB STRING
 *
 * MEOS function authority: meos/include/meos_geo.h (072_tgeo_tempspatialrels)
 */
public final class TempSpatialRelsUDFs {

    private TempSpatialRelsUDFs() {}

    private static int tripSrid(Pointer tptr) {
        Pointer bbox = functions.tspatial_to_stbox(tptr);
        if (bbox == null) return 0;
        try {
            return functions.stbox_srid(bbox);
        } finally {
            MeosMemory.free(bbox);
        }
    }

    private static String tempHexOut(Pointer r) {
        if (r == null) return null;
        try {
            return functions.temporal_as_hexwkb(r, (byte) 0);
        } finally {
            MeosMemory.free(r);
        }
    }

    // ------------------------------------------------------------------
    // tDisjoint(tpoint STRING, geomWkt STRING) → STRING (tbool hex-WKB)
    //
    // Returns a tbool that is true at instants where the moving point is
    // disjoint from (i.e. does not intersect) the static geometry.
    //
    // MEOS: tdisjoint_tgeo_geo(Temporal *, GSERIALIZED *)
    //       restr=false → return full tbool (not restricted to true/false instants)
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> tDisjoint =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                int srid = tripSrid(tptr);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return tempHexOut(functions.tdisjoint_tgeo_geo(tptr, gptr));
                } finally { MeosMemory.free(gptr); }
            } finally { MeosMemory.free(tptr); }
        };

    // ------------------------------------------------------------------
    // tIntersects(tpoint STRING, geomWkt STRING) → STRING (tbool hex-WKB)
    //
    // Returns a tbool that is true at instants where the moving point
    // intersects the static geometry.
    //
    // MEOS: tintersects_tgeo_geo(Temporal *, GSERIALIZED *)
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> tIntersects =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                int srid = tripSrid(tptr);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return tempHexOut(functions.tintersects_tgeo_geo(tptr, gptr));
                } finally { MeosMemory.free(gptr); }
            } finally { MeosMemory.free(tptr); }
        };

    // ------------------------------------------------------------------
    // tTouches(tpoint STRING, geomWkt STRING) → STRING (tbool hex-WKB)
    //
    // Returns a tbool that is true at instants where the moving point
    // touches (shares boundary with) the static geometry.
    //
    // MEOS: ttouches_tgeo_geo(Temporal *, GSERIALIZED *)
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> tTouches =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                int srid = tripSrid(tptr);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return tempHexOut(functions.ttouches_tgeo_geo(tptr, gptr));
                } finally { MeosMemory.free(gptr); }
            } finally { MeosMemory.free(tptr); }
        };

    // ------------------------------------------------------------------
    // tDisjointTgeoTgeo(trip1 STRING, trip2 STRING) → STRING (tbool hex-WKB)
    // tIntersectsTgeoTgeo(trip1 STRING, trip2 STRING) → STRING
    // tTouchesTogeoTgeo(trip1 STRING, trip2 STRING) → STRING
    //
    // Temporal predicates for two moving objects.
    //
    // MEOS: tdisjoint_tgeo_tgeo, tintersects_tgeo_tgeo, ttouches_tgeo_tgeo
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> tDisjointTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    return tempHexOut(functions.tdisjoint_tgeo_tgeo(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, String> tIntersectsTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    return tempHexOut(functions.tintersects_tgeo_tgeo(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    public static final UDF2<String, String, String> tTouchesTogeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    return tempHexOut(functions.ttouches_tgeo_tgeo(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // tContainsTgeoGeo(trip STRING, geomWkt STRING) → STRING
    // tContainsTgeoTgeo(trip1 STRING, trip2 STRING) → STRING
    //
    // Note: tContains(geomWkt, trip) already exists in GeoAnalyticsUDFs
    // (tcontains_geo_tgeo — container is static).  These variants cover
    // the case where the moving object is the container.
    //
    // MEOS: tcontains_tgeo_geo, tcontains_tgeo_tgeo
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> tContainsTgeoGeo =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                int srid = tripSrid(tptr);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return tempHexOut(functions.tcontains_tgeo_geo(tptr, gptr));
                } finally { MeosMemory.free(gptr); }
            } finally { MeosMemory.free(tptr); }
        };

    public static final UDF2<String, String, String> tContainsTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    return tempHexOut(functions.tcontains_tgeo_tgeo(p1, p2));
                } finally { MeosMemory.free(p2); }
            } finally { MeosMemory.free(p1); }
        };

    // ------------------------------------------------------------------
    // tCoversTgeoGeo(trip STRING, geomWkt STRING) → STRING
    //
    // MEOS: tcovers_tgeo_geo
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> tCoversTgeoGeo =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                int srid = tripSrid(tptr);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return tempHexOut(functions.tcovers_tgeo_geo(tptr, gptr));
                } finally { MeosMemory.free(gptr); }
            } finally { MeosMemory.free(tptr); }
        };

    // ------------------------------------------------------------------
    // tDwithinTgeoGeo(trip STRING, geomWkt STRING, dist DOUBLE) → STRING
    //
    // MEOS: tdwithin_tgeo_geo
    // ------------------------------------------------------------------
    public static final UDF3<String, String, Number, String> tDwithinTgeoGeo =
        (trip, geomWkt, dist) -> {
            if (trip == null || geomWkt == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                int srid = tripSrid(tptr);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return tempHexOut(functions.tdwithin_tgeo_geo(tptr, gptr, dist.doubleValue()));
                } finally { MeosMemory.free(gptr); }
            } finally { MeosMemory.free(tptr); }
        };

    // ------------------------------------------------------------------
    // REGISTRATION
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        spark.udf().register("tDisjoint",            tDisjoint,            DataTypes.StringType);
        spark.udf().register("tIntersects",          tIntersects,          DataTypes.StringType);
        spark.udf().register("tTouches",             tTouches,             DataTypes.StringType);
        spark.udf().register("tDisjointTgeoTgeo",    tDisjointTgeoTgeo,    DataTypes.StringType);
        spark.udf().register("tIntersectsTgeoTgeo",  tIntersectsTgeoTgeo,  DataTypes.StringType);
        spark.udf().register("tTouchesTogeoTgeo",    tTouchesTogeoTgeo,    DataTypes.StringType);
        spark.udf().register("tContainsTgeoGeo",     tContainsTgeoGeo,     DataTypes.StringType);
        spark.udf().register("tContainsTgeoTgeo",    tContainsTgeoTgeo,    DataTypes.StringType);
        spark.udf().register("tCoversTgeoGeo",       tCoversTgeoGeo,       DataTypes.StringType);
        spark.udf().register("tDwithinTgeoGeo",      tDwithinTgeoGeo,      DataTypes.StringType);
    }
}
