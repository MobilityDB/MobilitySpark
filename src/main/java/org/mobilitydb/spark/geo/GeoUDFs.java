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
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosThread;

/**
 * Spark SQL UDFs for spatial (geometry) operations on tgeompoint.
 *
 * Storage convention:
 *   tgeompoint  → hex-WKB STRING (temporal_as_hexwkb / temporal_from_hexwkb)
 *   geometry    → WKT STRING     (e.g. "POINT(50 0)", parsed via geo_from_text)
 *
 * Storing geometry as WKT keeps it human-readable and avoids the hex-EWKB
 * encoding that DuckDB's spatial extension requires as a workaround for binary
 * interchange.  Parsing overhead is negligible for query parameters.
 *
 * MEOS function authority: meos/include/meos_geo.h
 * JMEOS PR: github.com/MobilityDB/JMEOS/pull/9
 */
public final class GeoUDFs {

    private GeoUDFs() {}

    // ------------------------------------------------------------------
    // eIntersects(trip STRING, geomWkt STRING) → BOOLEAN
    //
    // Returns true if the trip's trajectory ever intersects geomWkt.
    //
    // SRID handling: we extract the trip's SRID from its bounding box and
    // pass it to geo_from_text so that MEOS's ensure_same_srid check passes.
    // BerlinMOD trips use SRID=3857 (Web Mercator); query regions use SRID=0
    // (WKT with no SRID clause).  Without this, all spatial predicates silently
    // return false because ensure_same_srid(3857, 0) fails.
    //
    // Thread safety: MeosThread.ensureReady() initialises MEOS per executor thread.
    //
    // For geodetic trips (tgeogpoint), the geometry is promoted to geography
    // via geom_to_geog() to avoid MEOS "Operation on mixed SRID" errors.
    //
    // MEOS: geo_from_text(const char *, int32_t srid)      meos_geo.h:335
    //       tspatial_to_stbox(const Temporal *)             → STBox
    //       stbox_srid(const STBox *)                       → int32
    //       stbox_isgeodetic(const STBox *)                 → bool
    //       geom_to_geog(const GSERIALIZED *)               → GSERIALIZED *
    //       eintersects_tgeo_geo(const Temporal *,
    //                            const GSERIALIZED *)       meos_geo.h:829
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> eIntersects =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer bbox = functions.tspatial_to_stbox(tptr);
            boolean geodetic = (bbox != null && functions.stbox_isgeodetic(bbox));
            int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
            Pointer gptr = functions.geo_from_text(geomWkt, srid);
            if (gptr == null) return null;
            if (geodetic) {
                gptr = functions.geom_to_geog(gptr);
                if (gptr == null) return null;
            }
            return functions.eintersects_tgeo_geo(tptr, gptr) == 1;
        };

    // ------------------------------------------------------------------
    // nearestApproachDistance(t1 STRING, t2 STRING) → DOUBLE
    //
    // MEOS: nad_tgeo_tgeo(const Temporal *, const Temporal *) → double
    // meos_geo.h line ~864
    // Returns NULL when trips have no overlapping time extent (MEOS: DBL_MAX).
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Double> nearestApproachDistance =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            double dist = functions.nad_tgeo_tgeo(p1, p2);
            return (dist == Double.MAX_VALUE) ? null : dist;
        };

    // ------------------------------------------------------------------
    // eDwithin(t1 STRING, t2 STRING, dist DOUBLE) → BOOLEAN
    //
    // dist accepts Double or BigDecimal (Spark infers decimal(p,s) for
    // numeric literals like 10.0 — use Number.doubleValue() to handle both).
    //
    // MEOS: edwithin_tgeo_tgeo(const Temporal *, const Temporal *, double) → int
    // meos_geo.h line ~828
    // ------------------------------------------------------------------
    public static final UDF3<String, String, Number, Boolean> eDwithin =
        (trip1, trip2, dist) -> {
            if (trip1 == null || trip2 == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            return functions.edwithin_tgeo_tgeo(p1, p2, dist.doubleValue()) == 1;
        };

    // ------------------------------------------------------------------
    // tgeompoint(wkt STRING) → STRING (hex-WKB)
    //
    // Parses a tgeompoint WKT string (e.g. "[POINT(0 0)@2020-01-01, ...]")
    // and returns the MEOS hex-WKB encoding for use in other UDFs.
    //
    // MEOS: tgeompoint_in(const char *str) → Temporal *  meos_geo.h:618
    //       temporal_as_hexwkb(const Temporal *, uint8_t, size_t *) meos.h:1261
    // ------------------------------------------------------------------
    public static final UDF1<String, String> tgeompoint =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tgeompoint_in(wkt);
            if (p == null) return null;
            return functions.temporal_as_hexwkb(p, (byte) 0);
        };

    // ------------------------------------------------------------------
    // trajectory(trip STRING) → STRING (hex WKB geometry)
    //
    // Projects a tgeompoint to its spatial path: a POINT for a single
    // instant, LINESTRING for a linear sequence.  The result is a hex
    // WKB string — the same format that PostgreSQL COPY and DuckDB COPY
    // produce for GEOMETRY columns, making q08 byte-identical across all
    // three platforms.
    //
    // MEOS: tpoint_trajectory(const Temporal *, bool merge) → GSERIALIZED *
    //       geo_as_hexewkb(const GSERIALIZED *, const char *endian)  meos_geo.h
    // ------------------------------------------------------------------
    public static final UDF1<String, String> trajectory =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gptr = functions.tpoint_trajectory(tptr, true);
            if (gptr == null) return null;
            return functions.geo_as_hexewkb(gptr, null);
        };

    // ------------------------------------------------------------------
    // eContains(geomWKT STRING, trip STRING) → BOOLEAN
    //
    // Returns true if the static geometry ever contains the moving object,
    // i.e. at some instant the vehicle's position was strictly inside the
    // geometry's interior.
    //
    // Argument order: eContains(container, contained) — consistent with
    // MobilityDB's eContains(geometry, tgeompoint).
    //
    // MEOS: econtains_geo_tgeo(const GSERIALIZED *, const Temporal *) → int
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> eContains =
        (geomWkt, trip) -> {
            if (geomWkt == null || trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer bbox = functions.tspatial_to_stbox(tptr);
            int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
            Pointer gptr = functions.geo_from_text(geomWkt, srid);
            if (gptr == null) return null;
            return functions.econtains_geo_tgeo(gptr, tptr) == 1;
        };

    // ------------------------------------------------------------------
    // geomFromText(wkt STRING) → STRING (hex-EWKB)
    //
    // Parses a WKT geometry string with SRID 0 and returns the MEOS
    // hex-EWKB encoding.  Useful for pre-converting geometry columns;
    // the scalar UDFs (eIntersects, eDwithin, eContains) accept raw WKT
    // directly so this is typically not needed in query SQL.
    //
    // MEOS: geo_from_text(const char *, int32_t srid) → GSERIALIZED *
    //       geo_as_hexewkb(const GSERIALIZED *, const char *)
    // ------------------------------------------------------------------
    public static final UDF1<String, String> geomFromText =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.geo_from_text(wkt, 0);
            if (p == null) return null;
            return functions.geo_as_hexewkb(p, null);
        };

    // ------------------------------------------------------------------
    // getX / getY / getZ(trip STRING) → STRING (tfloat hex-WKB)
    // cumulativeLength(trip STRING) → STRING (tfloat hex-WKB)
    //
    // MEOS: tpoint_get_x/y/z, tpoint_cumulative_length  meos_geo.h
    // ------------------------------------------------------------------
    public static final UDF1<String, String> getX =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer r = functions.tpoint_get_x(tptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF1<String, String> getY =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer r = functions.tpoint_get_y(tptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF1<String, String> getZ =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            // tpoint_get_z raises a MEOS error (crashing JVM via longjmp) for 2D points;
            // guard with a Z-presence check via the bounding box.
            Pointer bbox = functions.tspatial_to_stbox(tptr);
            if (bbox == null || !functions.stbox_hasz(bbox)) return null;
            Pointer r = functions.tpoint_get_z(tptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF1<String, String> cumulativeLength =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer r = functions.tpoint_cumulative_length(tptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // stops(trip STRING, maxDist DOUBLE, minDuration STRING) → STRING
    //
    // Returns the sub-trajectories where the vehicle stayed within maxDist
    // for at least minDuration.  minDuration is an interval string ("1 second").
    // Returns null when no stops are found.
    //
    // MEOS: temporal_stops(const Temporal *, double, const Interval *)  meos.h
    // ------------------------------------------------------------------
    public static final UDF3<String, Double, String, String> stops =
        (trip, maxDist, minDuration) -> {
            if (trip == null || maxDist == null || minDuration == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer iv = functions.pg_interval_in(minDuration, -1);
            if (iv == null) return null;
            Pointer r = functions.temporal_stops(tptr, maxDist, iv);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // isSimple(trip STRING) → BOOLEAN
    //
    // True when the trip has no self-intersections.
    //
    // MEOS: tpoint_is_simple(const Temporal *)  meos_geo.h
    // ------------------------------------------------------------------
    public static final UDF1<String, Boolean> isSimple =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            return functions.tpoint_is_simple(tptr);
        };

    // ------------------------------------------------------------------
    // shortestLine(trip1 STRING, trip2 STRING) → STRING (WKT geometry)
    //
    // Returns the WKT of the shortest line segment connecting the two trips
    // at their closest point in time and space.
    //
    // MEOS: shortestline_tpoint_tpoint(const Temporal *, const Temporal *)  meos_geo.h
    //       geo_as_text(const GSERIALIZED *, int precision)
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> shortestLine =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            Pointer g = functions.shortestline_tgeo_tgeo(p1, p2);
            if (g == null) return null;
            return functions.geo_as_text(g, 15);
        };

    public static void registerAll(org.apache.spark.sql.SparkSession spark) {
        spark.udf().register("eIntersects",             eIntersects,             DataTypes.BooleanType);
        spark.udf().register("eContains",               eContains,               DataTypes.BooleanType);
        spark.udf().register("nearestApproachDistance", nearestApproachDistance, DataTypes.DoubleType);
        spark.udf().register("eDwithin",                eDwithin,                DataTypes.BooleanType);
        spark.udf().register("tgeompoint",              tgeompoint,              DataTypes.StringType);
        spark.udf().register("trajectory",              trajectory,              DataTypes.StringType);
        spark.udf().register("geomFromText",            geomFromText,            DataTypes.StringType);
        spark.udf().register("getX",                   getX,                   DataTypes.StringType);
        spark.udf().register("getY",                   getY,                   DataTypes.StringType);
        spark.udf().register("getZ",                   getZ,                   DataTypes.StringType);
        spark.udf().register("cumulativeLength",        cumulativeLength,        DataTypes.StringType);
        spark.udf().register("stops",                  stops,                  DataTypes.StringType);
        spark.udf().register("isSimple",               isSimple,               DataTypes.BooleanType);
        spark.udf().register("shortestLine",           shortestLine,           DataTypes.StringType);
    }
}
