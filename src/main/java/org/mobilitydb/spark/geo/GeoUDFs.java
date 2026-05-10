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
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

/**
 * Spark SQL UDFs for spatial (geometry) operations on tgeompoint.
 *
 * Storage convention:
 *   tgeompoint  → hex-WKB STRING (temporal_as_hexwkb / temporal_from_hexwkb)
 *   geometry    → WKT STRING     (e.g. "POINT(50 0)", parsed via geo_from_text)
 *
 * Memory management: every native Pointer allocated by MEOS must be freed via
 * MeosMemory.free() in a finally block.  MEOS standalone mode uses the system
 * malloc/free (palloc/pfree map to malloc/free outside PostgreSQL), so native
 * objects are NOT garbage-collected by the JVM.  Failing to free them causes
 * the native heap to grow without bound across UDF calls (one leaked object per
 * row × millions of rows in cross-join queries like Q2/Q4/Q5/Q6).
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
public final class GeoUDFs {

    private GeoUDFs() {}

    // ------------------------------------------------------------------
    // eIntersects(trip STRING, geomWkt STRING) → BOOLEAN
    //
    // Returns true if the trip's trajectory ever intersects geomWkt.
    //
    // SRID handling: extract the trip's SRID from its bounding box and
    // pass it to geo_from_text so MEOS's ensure_same_srid check passes.
    // BerlinMOD trips use SRID=3857; query regions use SRID=0 (plain WKT).
    //
    // For geodetic trips (tgeogpoint), the geometry is promoted to geography
    // via geom_to_geog() to avoid MEOS "Operation on mixed SRID" errors.
    //
    // MEOS: geo_from_text, tspatial_to_stbox, stbox_srid, stbox_isgeodetic,
    //       geom_to_geog, eintersects_tgeo_geo  (meos_geo.h)
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> eIntersects =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                boolean geodetic = (bbox != null && functions.stbox_isgeodetic(bbox));
                int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
                MeosMemory.free(bbox);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                if (geodetic) {
                    Pointer geog = functions.geom_to_geog(gptr);
                    MeosMemory.free(gptr);
                    gptr = geog;
                    if (gptr == null) return null;
                }
                try {
                    return functions.eintersects_tgeo_geo(tptr, gptr) == 1;
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // nearestApproachDistance(t1 STRING, t2 STRING) → DOUBLE
    //
    // MEOS: nad_tgeo_tgeo(const Temporal *, const Temporal *) → double
    // Returns NULL when trips have no overlapping time extent (MEOS: DBL_MAX).
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Double> nearestApproachDistance =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    double dist = functions.nad_tgeo_tgeo(p1, p2);
                    return (dist == Double.MAX_VALUE) ? null : dist;
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    // ------------------------------------------------------------------
    // eDwithin(t1 STRING, t2 STRING, dist DOUBLE) → BOOLEAN
    //
    // dist accepts Double or BigDecimal (Spark infers decimal(p,s) for
    // numeric literals like 10.0 — use Number.doubleValue() to handle both).
    //
    // MEOS: edwithin_tgeo_tgeo(const Temporal *, const Temporal *, double) → int
    // ------------------------------------------------------------------
    public static final UDF3<String, String, Number, Boolean> eDwithin =
        (trip1, trip2, dist) -> {
            if (trip1 == null || trip2 == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    return functions.edwithin_tgeo_tgeo(p1, p2, dist.doubleValue()) == 1;
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    // ------------------------------------------------------------------
    // tgeompoint(wkt STRING) → STRING (hex-WKB)
    //
    // Parses a tgeompoint WKT string and returns the MEOS hex-WKB encoding.
    //
    // MEOS: tgeompoint_in(const char *str) → Temporal *  meos_geo.h:618
    //       temporal_as_hexwkb(const Temporal *, uint8_t, size_t *) meos.h
    // ------------------------------------------------------------------
    public static final UDF1<String, String> tgeompoint =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tgeompoint_in(wkt);
            if (p == null) return null;
            try {
                return functions.temporal_as_hexwkb(p, (byte) 0);
            } finally {
                MeosMemory.free(p);
            }
        };

    // ------------------------------------------------------------------
    // trajectory(trip STRING) → STRING (hex WKB geometry)
    //
    // Projects a tgeompoint to its spatial path: POINT for a single
    // instant, LINESTRING for a linear sequence.  Returns hex-EWKB.
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
            try {
                Pointer gptr = functions.tpoint_trajectory(tptr, true);
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
    // eDisjoint(trip STRING, geomWkt STRING) → BOOLEAN
    //
    // Returns true if the moving object is ever disjoint from the geometry.
    //
    // MEOS: edisjoint_tgeo_geo(const Temporal *, const GSERIALIZED *) → int
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> eDisjoint =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
                MeosMemory.free(bbox);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return functions.edisjoint_tgeo_geo(tptr, gptr) == 1;
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // eTouches(trip STRING, geomWkt STRING) → BOOLEAN
    //
    // Returns true if the moving object ever touches (shares boundary with)
    // the static geometry.
    //
    // MEOS: etouches_tgeo_geo(const Temporal *, const GSERIALIZED *) → int
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> eTouches =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
                MeosMemory.free(bbox);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return functions.etouches_tgeo_geo(tptr, gptr) == 1;
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // eCovers(trip STRING, geomWkt STRING) → BOOLEAN
    //
    // Returns true if the moving object ever covers the static geometry.
    //
    // MEOS: ecovers_tgeo_geo(const Temporal *, const GSERIALIZED *) → int
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> eCovers =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
                MeosMemory.free(bbox);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return functions.ecovers_tgeo_geo(tptr, gptr) == 1;
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // eDisjointTgeoTgeo(trip1 STRING, trip2 STRING) → BOOLEAN
    // eIntersectsTgeoTgeo(trip1 STRING, trip2 STRING) → BOOLEAN
    //
    // MEOS: edisjoint_tgeo_tgeo, eintersects_tgeo_tgeo
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> eDisjointTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    return functions.edisjoint_tgeo_tgeo(p1, p2) == 1;
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    public static final UDF2<String, String, Boolean> eIntersectsTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    return functions.eintersects_tgeo_tgeo(p1, p2) == 1;
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    // ------------------------------------------------------------------
    // aIntersects(trip STRING, geomWkt STRING) → BOOLEAN
    // aDisjoint(trip STRING, geomWkt STRING) → BOOLEAN
    //
    // Returns true if the moving object always intersects / is always
    // disjoint from the static geometry.
    //
    // MEOS: aintersects_tgeo_geo, adisjoint_tgeo_geo
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> aIntersects =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
                MeosMemory.free(bbox);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return functions.aintersects_tgeo_geo(tptr, gptr) == 1;
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    public static final UDF2<String, String, Boolean> aDisjoint =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
                MeosMemory.free(bbox);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return functions.adisjoint_tgeo_geo(tptr, gptr) == 1;
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // aDwithin(trip1 STRING, trip2 STRING, dist DOUBLE) → BOOLEAN
    // eDwithinGeo(trip STRING, geomWkt STRING, dist DOUBLE) → BOOLEAN
    // aDwithinGeo(trip STRING, geomWkt STRING, dist DOUBLE) → BOOLEAN
    //
    // MEOS: adwithin_tgeo_tgeo, edwithin_tgeo_geo, adwithin_tgeo_geo
    // ------------------------------------------------------------------
    public static final UDF3<String, String, Number, Boolean> aDwithin =
        (trip1, trip2, dist) -> {
            if (trip1 == null || trip2 == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    return functions.adwithin_tgeo_tgeo(p1, p2, dist.doubleValue()) == 1;
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    public static final UDF3<String, String, Number, Boolean> eDwithinGeo =
        (trip, geomWkt, dist) -> {
            if (trip == null || geomWkt == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
                MeosMemory.free(bbox);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return functions.edwithin_tgeo_geo(tptr, gptr, dist.doubleValue()) == 1;
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    public static final UDF3<String, String, Number, Boolean> aDwithinGeo =
        (trip, geomWkt, dist) -> {
            if (trip == null || geomWkt == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
                MeosMemory.free(bbox);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return functions.adwithin_tgeo_geo(tptr, gptr, dist.doubleValue()) == 1;
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // eContains(geomWKT STRING, trip STRING) → BOOLEAN
    //
    // Returns true if the static geometry ever contains the moving object.
    // Argument order: eContains(container, contained).
    //
    // MEOS: econtains_geo_tgeo(const GSERIALIZED *, const Temporal *) → int
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> eContains =
        (geomWkt, trip) -> {
            if (geomWkt == null || trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                int srid = (bbox != null) ? functions.stbox_srid(bbox) : 0;
                MeosMemory.free(bbox);
                Pointer gptr = functions.geo_from_text(geomWkt, srid);
                if (gptr == null) return null;
                try {
                    return functions.econtains_geo_tgeo(gptr, tptr) == 1;
                } finally {
                    MeosMemory.free(gptr);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // geomFromText(wkt STRING) → STRING (hex-EWKB)
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
            try {
                return functions.geo_as_hexewkb(p, null);
            } finally {
                MeosMemory.free(p);
            }
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
            try {
                Pointer r = functions.tpoint_get_x(tptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    public static final UDF1<String, String> getY =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer r = functions.tpoint_get_y(tptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    public static final UDF1<String, String> getZ =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                // tpoint_get_z raises a MEOS error for 2D points; guard with Z-presence check.
                Pointer bbox = functions.tspatial_to_stbox(tptr);
                if (bbox == null || !functions.stbox_hasz(bbox)) {
                    MeosMemory.free(bbox);
                    return null;
                }
                MeosMemory.free(bbox);
                Pointer r = functions.tpoint_get_z(tptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    public static final UDF1<String, String> cumulativeLength =
        (trip) -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer r = functions.tpoint_cumulative_length(tptr);
                if (r == null) return null;
                try {
                    return functions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // stops(trip STRING, maxDist DOUBLE, minDuration STRING) → STRING
    //
    // Returns the sub-trajectories where the vehicle stayed within maxDist
    // for at least minDuration ("1 second").
    //
    // MEOS: temporal_stops(const Temporal *, double, const Interval *)  meos.h
    // ------------------------------------------------------------------
    public static final UDF3<String, Double, String, String> stops =
        (trip, maxDist, minDuration) -> {
            if (trip == null || maxDist == null || minDuration == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer iv = functions.pg_interval_in(minDuration, -1);
                if (iv == null) return null;
                try {
                    Pointer r = functions.temporal_stops(tptr, maxDist, iv);
                    if (r == null) return null;
                    try {
                        return functions.temporal_as_hexwkb(r, (byte) 0);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(iv);
                }
            } finally {
                MeosMemory.free(tptr);
            }
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
            try {
                return functions.tpoint_is_simple(tptr);
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // shortestLine(trip1 STRING, trip2 STRING) → STRING (WKT geometry)
    //
    // MEOS: shortestline_tpoint_tpoint(const Temporal *, const Temporal *)
    //       geo_as_text(const GSERIALIZED *, int precision)
    // ------------------------------------------------------------------
    public static final UDF2<String, String, String> shortestLine =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(trip2);
                if (p2 == null) return null;
                try {
                    Pointer g = functions.shortestline_tgeo_tgeo(p1, p2);
                    if (g == null) return null;
                    try {
                        return functions.geo_as_text(g, 15);
                    } finally {
                        MeosMemory.free(g);
                    }
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    public static void registerAll(org.apache.spark.sql.SparkSession spark) {
        spark.udf().register("eIntersects",             eIntersects,             DataTypes.BooleanType);
        spark.udf().register("eDisjoint",               eDisjoint,               DataTypes.BooleanType);
        spark.udf().register("eTouches",                eTouches,                DataTypes.BooleanType);
        spark.udf().register("eCovers",                 eCovers,                 DataTypes.BooleanType);
        spark.udf().register("eContains",               eContains,               DataTypes.BooleanType);
        spark.udf().register("eDisjointTgeoTgeo",       eDisjointTgeoTgeo,       DataTypes.BooleanType);
        spark.udf().register("eIntersectsTgeoTgeo",     eIntersectsTgeoTgeo,     DataTypes.BooleanType);
        spark.udf().register("nearestApproachDistance", nearestApproachDistance, DataTypes.DoubleType);
        spark.udf().register("eDwithin",                eDwithin,                DataTypes.BooleanType);
        spark.udf().register("eDwithinGeo",             eDwithinGeo,             DataTypes.BooleanType);
        spark.udf().register("aIntersects",             aIntersects,             DataTypes.BooleanType);
        spark.udf().register("aDisjoint",               aDisjoint,               DataTypes.BooleanType);
        spark.udf().register("aDwithin",                aDwithin,                DataTypes.BooleanType);
        spark.udf().register("aDwithinGeo",             aDwithinGeo,             DataTypes.BooleanType);
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
