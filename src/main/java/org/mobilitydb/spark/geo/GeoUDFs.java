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
    // geomWkt is a WKT string (e.g. "POINT(50 0)") with SRID 0 (default).
    // When the trip is geodetic (tgeogpoint, SRID 4326 — e.g. from a
    // TemporalParquet shard written by MobilityDuck), the geometry is
    // automatically promoted to geographic via geom_to_geog() to avoid
    // MEOS "Operation on mixed SRID" errors.
    //
    // MEOS: geo_from_text(const char *, int32_t srid)      meos_geo.h:335
    //       tspatial_to_stbox(const Temporal *)             → STBox
    //       stbox_isgeodetic(const STBox *)                 → bool
    //       geom_to_geog(const GSERIALIZED *)               → GSERIALIZED *
    //       eintersects_tgeo_geo(const Temporal *,
    //                            const GSERIALIZED *)       meos_geo.h:829
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> eIntersects =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            Pointer gptr = functions.geo_from_text(geomWkt, 0);
            if (tptr == null || gptr == null) return null;
            Pointer bbox = functions.tspatial_to_stbox(tptr);
            if (bbox != null && functions.stbox_isgeodetic(bbox)) {
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
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            double dist = functions.nad_tgeo_tgeo(p1, p2);
            return (dist == Double.MAX_VALUE) ? null : dist;
        };

    // ------------------------------------------------------------------
    // eDwithin(t1 STRING, t2 STRING, dist DOUBLE) → BOOLEAN
    //
    // MEOS: edwithin_tgeo_tgeo(const Temporal *, const Temporal *, double) → int
    // meos_geo.h line ~828
    // ------------------------------------------------------------------
    public static final UDF3<String, String, Double, Boolean> eDwithin =
        (trip1, trip2, dist) -> {
            if (trip1 == null || trip2 == null || dist == null) return null;
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            return functions.edwithin_tgeo_tgeo(p1, p2, dist) == 1;
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
            Pointer gptr = functions.geo_from_text(geomWkt, 0);
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (gptr == null || tptr == null) return null;
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
            Pointer p = functions.geo_from_text(wkt, 0);
            if (p == null) return null;
            return functions.geo_as_hexewkb(p, null);
        };

    // ------------------------------------------------------------------
    // length(trip STRING) → DOUBLE
    //
    // Returns the total Euclidean path length of a tgeompoint trip.
    //
    // MEOS: tpoint_length(const Temporal *) → double
    // ------------------------------------------------------------------
    public static final UDF1<String, Double> length =
        (trip) -> {
            if (trip == null) return null;
            Pointer ptr = functions.temporal_from_hexwkb(trip);
            if (ptr == null) return null;
            return functions.tpoint_length(ptr);
        };

    // ------------------------------------------------------------------
    // valueAtTimestamp(trip STRING, instant TIMESTAMP) → STRING (WKT)
    //
    // Returns the WKT geometry of a tgeompoint at a specific instant, or
    // NULL if the instant is outside the trip's time extent.
    //
    // Epoch note: same PG-epoch convention as in TemporalUDFs.atTime.
    //
    // Implementation: temporal_at_timestamptz gives a TInstant; trajectory
    // of a TInstant is the underlying point geometry.
    //
    // MEOS: temporal_at_timestamptz(const Temporal *, TimestampTz) → Temporal *
    //       tpoint_trajectory(const Temporal *, bool merge) → GSERIALIZED *
    //       geo_as_text(const GSERIALIZED *, int) → char *
    // ------------------------------------------------------------------
    public static final UDF2<String, java.sql.Timestamp, String> valueAtTimestamp =
        (trip, instant) -> {
            if (trip == null || instant == null) return null;
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            long pgEpochMicros = (instant.getTime() - 946684800L * 1000L) * 1000L;
            java.time.OffsetDateTime odt = java.time.OffsetDateTime.ofInstant(
                java.time.Instant.ofEpochSecond(pgEpochMicros, 0),
                java.time.ZoneOffset.UTC);
            Pointer instPtr = functions.temporal_at_timestamptz(tptr, odt);
            if (instPtr == null) return null;
            Pointer geomPtr = functions.tpoint_trajectory(instPtr, false);
            if (geomPtr == null) return null;
            return functions.geo_as_text(geomPtr, 0);
        };

    // ------------------------------------------------------------------
    // tDwithin(trip1 STRING, trip2 STRING, dist DOUBLE) → STRING (tbool hex-WKB)
    //
    // Returns a tbool (hex-WKB) representing at which times the two trips
    // were within dist of each other.  Pass the result to whenTrue() to
    // get the tstzspanset of matching intervals.
    //
    // MEOS: tdwithin_tgeo_tgeo(const Temporal *, const Temporal *,
    //           double, bool restr, bool atvalue) → Temporal *
    // ------------------------------------------------------------------
    public static final UDF3<String, String, Double, String> tDwithin =
        (trip1, trip2, dist) -> {
            if (trip1 == null || trip2 == null || dist == null) return null;
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            Pointer result = functions.tdwithin_tgeo_tgeo(p1, p2, dist, false, false);
            if (result == null) return null;
            return functions.temporal_as_hexwkb(result, (byte) 0);
        };

    // ------------------------------------------------------------------
    // whenTrue(tbool STRING) → STRING (tstzspanset text)
    //
    // Returns the set of time spans during which the tbool is TRUE, as a
    // tstzspanset text (e.g. "{[t1, t2], [t3, t4]}").
    //
    // MEOS: tbool_when_true(const Temporal *) → SpanSet *
    //       tstzspanset_out(const SpanSet *) → char *
    // ------------------------------------------------------------------
    public static final UDF1<String, String> whenTrue =
        (tboolHex) -> {
            if (tboolHex == null) return null;
            Pointer ptr = functions.temporal_from_hexwkb(tboolHex);
            if (ptr == null) return null;
            Pointer ss = functions.tbool_when_true(ptr);
            if (ss == null) return null;
            return functions.tstzspanset_out(ss);
        };

    // ------------------------------------------------------------------
    // aDisjoint(trip1 STRING, trip2 STRING) → BOOLEAN
    //
    // Returns true if the two tgeompoints are ALWAYS spatially disjoint
    // (their spatial extents never overlap over the common time span).
    //
    // MEOS: adisjoint_tgeo_tgeo(const Temporal *, const Temporal *) → int
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> aDisjoint =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            Pointer p1 = functions.temporal_from_hexwkb(trip1);
            Pointer p2 = functions.temporal_from_hexwkb(trip2);
            if (p1 == null || p2 == null) return null;
            return functions.adisjoint_tgeo_tgeo(p1, p2) == 1;
        };

    // ------------------------------------------------------------------
    // geomContains(containerWkt STRING, pointWkt STRING) → BOOLEAN
    //
    // Returns true if the first geometry (container) contains the second
    // (typically a POINT).  WKT strings, SRID 0.
    //
    // Portable replacement for ST_Contains(geometry, geometry) when a
    // PostGIS function is unavailable (e.g. in Spark SQL without GeoSpark).
    //
    // MEOS: geo_from_text(const char *, int32_t srid) → GSERIALIZED *
    //       geom_contains(const GSERIALIZED *, const GSERIALIZED *) → bool
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> geomContains =
        (containerWkt, pointWkt) -> {
            if (containerWkt == null || pointWkt == null) return null;
            Pointer g1 = functions.geo_from_text(containerWkt, 0);
            Pointer g2 = functions.geo_from_text(pointWkt, 0);
            if (g1 == null || g2 == null) return null;
            return functions.geom_contains(g1, g2);
        };

    // ------------------------------------------------------------------
    // tgeompointFromBinary(wkb BINARY) → STRING (hex-WKB)
    //
    // Decodes a MEOS-WKB binary column written by MobilityDuck's asBinary()
    // and returns the internal hex-WKB STRING used by all other UDFs.
    // Entry point for reading TemporalParquet files in the edge-to-cloud
    // pipeline — the Parquet BYTE_ARRAY traj column arrives as byte[] here.
    //
    // Compatible with both tgeompoint and tgeogpoint WKB (type preserved
    // in the embedded MEOS flags).  Hex conversion is case-insensitive in
    // MEOS; we produce uppercase to match temporal_as_hexwkb output.
    //
    // MEOS: temporal_from_hexwkb(const char *) → Temporal *
    //       temporal_as_hexwkb(const Temporal *, uint8_t) → char *
    // ------------------------------------------------------------------
    public static final UDF1<byte[], String> tgeompointFromBinary =
        (bytes) -> {
            if (bytes == null) return null;
            String hex = java.util.HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer ptr = functions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            return functions.temporal_as_hexwkb(ptr, (byte) 0);
        };

    // ------------------------------------------------------------------
    // maxSpeed(trip STRING) → DOUBLE
    //
    // Returns the peak instantaneous speed of a tgeompoint trip, in the
    // same spatial units per second as the coordinate reference system
    // (degrees/s for SRID 0, m/s for geodetic tgeogpoint from JMEOS).
    //
    // MEOS: tpoint_speed(const Temporal *) → Temporal * (tfloat)
    //       tfloat_max_value(const Temporal *) → double
    // ------------------------------------------------------------------
    public static final UDF1<String, Double> maxSpeed =
        (trip) -> {
            if (trip == null) return null;
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer speedPtr = functions.tpoint_speed(tptr);
            if (speedPtr == null) return null;
            return functions.tfloat_max_value(speedPtr);
        };

    // ------------------------------------------------------------------
    // duration(trip STRING) → STRING (interval text, e.g. "01:50:00")
    //
    // Returns the total time extent of a tgeompoint trip as an interval
    // text string.  Equivalent to MobilityDB/MobilityDuck duration(traj).
    //
    // MEOS: temporal_duration(const Temporal *, bool) → Interval *
    //       pg_interval_out(const Interval *) → char *
    // ------------------------------------------------------------------
    public static final UDF1<String, String> duration =
        (trip) -> {
            if (trip == null) return null;
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer ivPtr = functions.temporal_duration(tptr, false);
            if (ivPtr == null) return null;
            return functions.pg_interval_out(ivPtr);
        };

    public static void registerAll(org.apache.spark.sql.SparkSession spark) {
        spark.udf().register("eIntersects",             eIntersects,             DataTypes.BooleanType);
        spark.udf().register("eContains",               eContains,               DataTypes.BooleanType);
        spark.udf().register("nearestApproachDistance", nearestApproachDistance, DataTypes.DoubleType);
        spark.udf().register("eDwithin",                eDwithin,                DataTypes.BooleanType);
        spark.udf().register("tgeompoint",              tgeompoint,              DataTypes.StringType);
        spark.udf().register("trajectory",              trajectory,              DataTypes.StringType);
        spark.udf().register("geomFromText",            geomFromText,            DataTypes.StringType);
        spark.udf().register("length",                  length,                  DataTypes.DoubleType);
        spark.udf().register("valueAtTimestamp",        valueAtTimestamp,        DataTypes.StringType);
        spark.udf().register("tDwithin",                tDwithin,                DataTypes.StringType);
        spark.udf().register("whenTrue",                whenTrue,                DataTypes.StringType);
        spark.udf().register("aDisjoint",               aDisjoint,               DataTypes.BooleanType);
        spark.udf().register("geomContains",            geomContains,            DataTypes.BooleanType);
        spark.udf().register("tgeompointFromBinary",    tgeompointFromBinary,    DataTypes.StringType);
        spark.udf().register("maxSpeed",                maxSpeed,                DataTypes.DoubleType);
        spark.udf().register("duration",                duration,                DataTypes.StringType);
    }
}
