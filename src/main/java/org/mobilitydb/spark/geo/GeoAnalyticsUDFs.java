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
import jnr.ffi.Runtime;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

/**
 * Spark SQL UDFs for high-value spatial analytics on temporal geometry types.
 *
 * Coverage:
 *   - Plain geometry operations: geomBuffer, geomConvexHull, geomIntersection
 *   - SRID / CRS management: setSRID, transform
 *   - Text output: asText, asEWKT
 *   - Elevation restriction: atElevation (3-D trips)
 *   - Temporal spatial predicates (TBool): tContains, tCovers, tDwithin
 *   - Bearing analytics: bearingToPoint, bearing
 *   - Spatial aggregates: twCentroid
 *
 * Storage convention:
 *   tgeompoint  → hex-WKB STRING  (temporal_as_hexwkb)
 *   geometry    → WKT STRING      (parsed via geo_from_text)
 *   TBool result → hex-WKB STRING
 *   TFloat result → hex-WKB STRING
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
public final class GeoAnalyticsUDFs {

    private GeoAnalyticsUDFs() {}

    // Convenience: decode a trip and extract its SRID from its bounding box
    private static int tripSrid(Pointer tptr) {
        Pointer bbox = functions.tspatial_to_stbox(tptr);
        return (bbox != null) ? functions.stbox_srid(bbox) : 0;
    }

    // ------------------------------------------------------------------
    // GEOMETRY OPERATIONS  (plain geometry in/out, WKT strings)
    //
    // MEOS: geom_buffer(gs, size, params)  meos_geo.h:401
    //       geom_convex_hull(gs)           meos_geo.h:403
    //       geom_intersection2d(gs1, gs2)  meos_geo.h:405
    // ------------------------------------------------------------------

    // geomBuffer("POLYGON((...))", 100.0) → WKT of buffered polygon
    public static final UDF2<String, Double, String> geomBuffer =
        (geomWkt, radius) -> {
            if (geomWkt == null || radius == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(geomWkt, 0);
            if (g == null) return null;
            Pointer r = functions.geom_buffer(g, radius, "");
            if (r == null) return null;
            return functions.geo_as_text(r, 15);
        };

    // geomConvexHull("MULTIPOINT((0 0),(1 1),(0 1))") → "POLYGON((0 0,0 1,1 1,0 0))"
    public static final UDF1<String, String> geomConvexHull =
        (geomWkt) -> {
            if (geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(geomWkt, 0);
            if (g == null) return null;
            Pointer r = functions.geom_convex_hull(g);
            if (r == null) return null;
            return functions.geo_as_text(r, 15);
        };

    // geomIntersection("POLYGON(...)", "POLYGON(...)") → WKT of intersection
    public static final UDF2<String, String, String> geomIntersection =
        (geomWkt1, geomWkt2) -> {
            if (geomWkt1 == null || geomWkt2 == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(geomWkt1, 0);
            Pointer g2 = functions.geo_from_text(geomWkt2, 0);
            if (g1 == null || g2 == null) return null;
            Pointer r = functions.geom_intersection2d(g1, g2);
            if (r == null) return null;
            return functions.geo_as_text(r, 15);
        };

    // ------------------------------------------------------------------
    // SRID / CRS MANAGEMENT  (temporal in/out, hex-WKB strings)
    //
    // MEOS: tspatial_set_srid(temp, srid)  meos_geo.h:687
    //       tspatial_transform(temp, srid)  meos_geo.h:688
    // ------------------------------------------------------------------

    // setSRID(trip, 4326) → same trip with SRID label changed (no reprojection)
    public static final UDF2<String, Integer, String> setSRID =
        (trip, srid) -> {
            if (trip == null || srid == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer r = functions.tspatial_set_srid(tptr, srid);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // transform(trip, 4326) → trip reprojected to SRID 4326
    public static final UDF2<String, Integer, String> transform =
        (trip, srid) -> {
            if (trip == null || srid == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer r = functions.tspatial_transform(tptr, srid);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // TEXT OUTPUT  (temporal in, WKT/EWKT string out)
    //
    // MEOS: tspatial_as_text(temp, maxdd)  meos_geo.h:620
    //       tspatial_as_ewkt(temp, maxdd)  meos_geo.h:619
    // ------------------------------------------------------------------

    // asText(trip, 6) → "[POINT(1.234567 2.345678)@2020-01-01, ...]"
    public static final UDF2<String, Integer, String> asText =
        (trip, maxdd) -> {
            if (trip == null || maxdd == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            return functions.tspatial_as_text(tptr, maxdd);
        };

    // asEWKT(trip, 6) → "[SRID=4326;POINT(...)@2020-01-01, ...]"
    public static final UDF2<String, Integer, String> asEWKT =
        (trip, maxdd) -> {
            if (trip == null || maxdd == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            return functions.tspatial_as_ewkt(tptr, maxdd);
        };

    // ------------------------------------------------------------------
    // ELEVATION RESTRICTION  (3-D tgeompoint, hex-WKB out)
    //
    // MEOS: tpoint_at_elevation(temp, zspan)  meos_geo.h:699
    //       floatspan_make(lower, upper, lower_inc, upper_inc)
    //
    // zmin/zmax define the closed [zmin, zmax] elevation band.
    // Returns null when no portion of the trip falls in the band.
    // ------------------------------------------------------------------

    // atElevation(trip, 0.0, 100.0) → sub-trip within elevation band [0,100]
    public static final UDF3<String, Double, Double, String> atElevation =
        (trip, zmin, zmax) -> {
            if (trip == null || zmin == null || zmax == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer zspan = functions.floatspan_make(zmin, zmax, true, true);
            if (zspan == null) return null;
            Pointer r = functions.tpoint_at_elevation(tptr, zspan);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // TEMPORAL SPATIAL PREDICATES  (TBool hex-WKB out)
    //
    // MEOS: tcontains_geo_tgeo(gs, temp)          meos_geo.h:837
    //       tcovers_tgeo_tgeo(temp1, temp2)        meos_geo.h:842
    //       tdwithin_tgeo_tgeo(temp1, temp2, dist) meos_geo.h:848
    //
    // These return temporal booleans: at each instant, whether the
    // predicate holds.  Result is encoded as hex-WKB for downstream UDFs.
    // ------------------------------------------------------------------

    // tContains("POLYGON(...)", trip) → tbool hex-WKB: true at instants inside polygon
    public static final UDF2<String, String, String> tContains =
        (geomWkt, trip) -> {
            if (geomWkt == null || trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            int srid = tripSrid(tptr);
            Pointer g = functions.geo_from_text(geomWkt, srid);
            if (g == null) return null;
            Pointer r = functions.tcontains_geo_tgeo(g, tptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tCovers(trip1, trip2) → tbool hex-WKB: true at instants where trip1 covers trip2
    public static final UDF2<String, String, String> tCovers =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.tcovers_tgeo_tgeo(p1, p2);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tDwithin(trip1, trip2, 100.0) → tbool hex-WKB: true at instants within 100 units
    public static final UDF3<String, String, Number, String> tDwithin =
        (trip1, trip2, dist) -> {
            if (trip1 == null || trip2 == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.tdwithin_tgeo_tgeo(p1, p2, dist.doubleValue());
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // BEARING ANALYTICS  (TFloat hex-WKB out)
    //
    // MEOS: bearing_tpoint_point(temp, gs, invert)  meos_geo.h
    //       bearing_tpoint_tpoint(temp1, temp2)      meos_geo.h
    //
    // bearing_tpoint_point: invert=false → bearing FROM trip TO point.
    // ------------------------------------------------------------------

    // bearingToPoint(trip, "POINT(lon lat)") → tfloat hex-WKB, bearing in radians
    public static final UDF2<String, String, String> bearingToPoint =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            int srid = tripSrid(tptr);
            Pointer g = functions.geo_from_text(geomWkt, srid);
            if (g == null) return null;
            Pointer r = functions.bearing_tpoint_point(tptr, g, false);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // bearing(trip1, trip2) → tfloat hex-WKB, instantaneous bearing between trips
    public static final UDF2<String, String, String> bearing =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.bearing_tpoint_tpoint(p1, p2);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // SPATIAL AGGREGATES  (geometry WKT out)
    //
    // MEOS: tpoint_twcentroid(temp)  meos_geo.h
    //
    // Returns the time-weighted centroid of the trip as a WKT POINT string.
    // ------------------------------------------------------------------

    // twCentroid(trip) → WKT POINT of the time-weighted centroid
    public static final UDF1<String, String> twCentroid =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer g = functions.tpoint_twcentroid(tptr);
            if (g == null) return null;
            return functions.geo_as_text(g, 15);
        };

    // ------------------------------------------------------------------
    // geoSame(wkt1 STRING, wkt2 STRING) → BOOLEAN
    //
    // Returns true if two geometries are exactly equal (same type, coordinates,
    // and SRID).
    //
    // MEOS: geo_same(const GSERIALIZED *, const GSERIALIZED *) → bool
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> geoSame =
        (wkt1, wkt2) -> {
            if (wkt1 == null || wkt2 == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(wkt1, 0);
            if (g1 == null) return null;
            try {
                Pointer g2 = functions.geo_from_text(wkt2, 0);
                if (g2 == null) return null;
                try {
                    return functions.geo_same(g1, g2);
                } finally {
                    MeosMemory.free(g2);
                }
            } finally {
                MeosMemory.free(g1);
            }
        };

    // ------------------------------------------------------------------
    // tpointConvexHull(trip STRING) → STRING (hex-EWKB geometry)
    //
    // Returns the convex hull of the trajectory of the temporal point.
    //
    // MEOS: tgeo_convex_hull(const Temporal *) → GSERIALIZED *
    //       geo_as_hexewkb(const GSERIALIZED *, const char *endian)
    // ------------------------------------------------------------------
    public static final UDF1<String, String> tpointConvexHull =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer gptr = functions.tgeo_convex_hull(tptr);
                if (gptr == null) return null;
                try {
                    return functions.geo_as_hexewkb(gptr, null);
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // tpointExpandSpace(trip STRING, distance DOUBLE) → STRING (hex-WKB STBOX)
    //
    // Returns the spatiotemporal bounding box of the temporal point expanded
    // by distance in each spatial dimension.
    //
    // MEOS: tspatial_to_stbox(const Temporal *) → STBox *
    //       stbox_expand_space(const STBox *, double d) → STBox *
    //       stbox_as_hexwkb(const STBox *, uint8_t variant, size_t *size)
    // ------------------------------------------------------------------
    public static final UDF2<String, Double, String> tpointExpandSpace =
        (trip, distance) -> {
            if (trip == null || distance == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer stbox = functions.tspatial_to_stbox(tptr);
                if (stbox == null) return null;
                try {
                    Pointer expanded = functions.stbox_expand_space(stbox, distance);
                    if (expanded == null) return null;
                    try {
                        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                        return functions.stbox_as_hexwkb(expanded, (byte) 0, sizeOut);
                    } finally {
                        MeosMemory.free(expanded);
                    }
                } finally {
                    MeosMemory.free(stbox);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // REGISTRATION
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        // geometry operations
        spark.udf().register("geomBuffer",       geomBuffer,       DataTypes.StringType);
        spark.udf().register("geomConvexHull",   geomConvexHull,   DataTypes.StringType);
        spark.udf().register("geomIntersection", geomIntersection, DataTypes.StringType);
        // SRID management
        spark.udf().register("setSRID",          setSRID,          DataTypes.StringType);
        spark.udf().register("transform",        transform,        DataTypes.StringType);
        // text output
        spark.udf().register("asText",           asText,           DataTypes.StringType);
        spark.udf().register("asEWKT",           asEWKT,           DataTypes.StringType);
        // elevation restriction
        spark.udf().register("atElevation",      atElevation,      DataTypes.StringType);
        // temporal predicates
        spark.udf().register("tContains",        tContains,        DataTypes.StringType);
        spark.udf().register("tCovers",          tCovers,          DataTypes.StringType);
        spark.udf().register("tDwithin",         tDwithin,         DataTypes.StringType);
        // bearing
        spark.udf().register("bearingToPoint",   bearingToPoint,   DataTypes.StringType);
        spark.udf().register("bearing",          bearing,          DataTypes.StringType);
        // spatial aggregate
        spark.udf().register("twCentroid",       twCentroid,       DataTypes.StringType);
        // geometry equality
        spark.udf().register("geoSame",          geoSame,          DataTypes.BooleanType);
        // tpoint analytics
        spark.udf().register("tpointConvexHull",  tpointConvexHull,  DataTypes.StringType);
        spark.udf().register("tpointExpandSpace", tpointExpandSpace, DataTypes.StringType);
        // tpoint minus geom + bearing direction
        spark.udf().register("minusGeometry", minusGeometry, DataTypes.StringType);
        spark.udf().register("tdirection",    tdirection,    DataTypes.DoubleType);
        // transformPipeline (PROJ pipeline string)
        spark.udf().register("transformPipeline",      tpointTransformPipeline,  DataTypes.StringType);
        spark.udf().register("stboxTransformPipeline", stboxTransformPipeline,   DataTypes.StringType);
    }

    public static final org.apache.spark.sql.api.java.UDF4<String, String, Integer, Boolean, String>
        tpointTransformPipeline = (trip, pipeline, srid, isForward) -> {
            if (trip == null || pipeline == null) return null;
            org.mobilitydb.spark.MeosThread.ensureReady();
            jnr.ffi.Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            try {
                jnr.ffi.Pointer r = functions.tpoint_transform_pipeline(t, pipeline,
                    srid == null ? 0 : srid, isForward == null ? true : isForward);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { org.mobilitydb.spark.MeosMemory.free(r); }
            } finally { org.mobilitydb.spark.MeosMemory.free(t); }
        };

    public static final org.apache.spark.sql.api.java.UDF4<String, String, Integer, Boolean, String>
        stboxTransformPipeline = (stboxHex, pipeline, srid, isForward) -> {
            if (stboxHex == null || pipeline == null) return null;
            org.mobilitydb.spark.MeosThread.ensureReady();
            jnr.ffi.Pointer s = functions.stbox_from_hexwkb(stboxHex);
            if (s == null) return null;
            try {
                jnr.ffi.Pointer r = functions.stbox_transform_pipeline(s, pipeline,
                    srid == null ? 0 : srid, isForward == null ? true : isForward);
                if (r == null) return null;
                try {
                    jnr.ffi.Pointer sizeOut = jnr.ffi.Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    return functions.stbox_as_hexwkb(r, (byte) 0, sizeOut);
                } finally { org.mobilitydb.spark.MeosMemory.free(r); }
            } finally { org.mobilitydb.spark.MeosMemory.free(s); }
        };

    // minusGeometry(tpoint, geomWkt) → tpoint with geometry subtracted (or null if total)
    public static final org.apache.spark.sql.api.java.UDF2<String, String, String> minusGeometry =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            org.mobilitydb.spark.MeosThread.ensureReady();
            jnr.ffi.Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            jnr.ffi.Pointer g = functions.geo_from_text(geomWkt, 0);
            if (g == null) { org.mobilitydb.spark.MeosMemory.free(t); return null; }
            try {
                jnr.ffi.Pointer r = org.mobilitydb.spark.MeosNative.INSTANCE.tpoint_minus_geom(t, g);
                if (r == null) return null;
                try { return functions.temporal_as_hexwkb(r, (byte) 0); }
                finally { org.mobilitydb.spark.MeosMemory.free(r); }
            } finally { org.mobilitydb.spark.MeosMemory.free(t, g); }
        };

    // tdirection(tpoint) → bearing in radians, or null if not defined
    public static final org.apache.spark.sql.api.java.UDF1<String, Double> tdirection =
        (trip) -> {
            if (trip == null) return null;
            org.mobilitydb.spark.MeosThread.ensureReady();
            jnr.ffi.Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            try {
                jnr.ffi.Pointer outBuf = jnr.ffi.Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                boolean ok = org.mobilitydb.spark.MeosNative.INSTANCE.tpoint_direction(t, outBuf);
                return ok ? outBuf.getDouble(0) : null;
            } finally { org.mobilitydb.spark.MeosMemory.free(t); }
        };
}
