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

package org.mobilitydb.spark.demo;

import functions.functions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * BerlinMOD-specific Spark SQL UDFs that extend the base MobilitySpark surface.
 *
 * These UDFs cover operations needed for BerlinMOD Q09-Q16 that are not in the
 * core UDF set: path length, bbox overlap (replacing &&), valueAtTimestamp,
 * geo+time STBox constructors, expandSpace, tDwithin, whenTrue, aDisjoint,
 * and geomContains.
 *
 * BerlinMODBench also preprocesses the portable SQL to rewrite Spark-incompatible
 * syntax before passing to spark.sql():
 *   stbox(geom, t)  →  geoTimeStbox(geom, t)
 *   expr1 && expr2  →  bboxOverlaps(expr1, expr2)   (per line)
 *   ::numeric       →  (removed)
 *   ST_Contains(    →  geomContains(
 */
public final class BerlinMODUDFs {

    private BerlinMODUDFs() {}

    private static final DateTimeFormatter PG_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Convert Spark java.sql.Timestamp to MEOS TimestampTz via pg_timestamptz_in. */
    private static OffsetDateTime toMeosTs(java.sql.Timestamp ts) {
        String s = ts.toInstant().atOffset(ZoneOffset.UTC).format(PG_FMT);
        return functions.pg_timestamptz_in(s, -1);
    }

    /** Convert a String or java.sql.Timestamp to MEOS TimestampTz. */
    private static OffsetDateTime parseTs(Object arg) {
        if (arg instanceof java.sql.Timestamp) return toMeosTs((java.sql.Timestamp) arg);
        return functions.pg_timestamptz_in(arg.toString().trim(), -1);
    }

    // ------------------------------------------------------------------
    // length(trip STRING) → DOUBLE
    // Path length of a tgeompoint trajectory.
    // MEOS: tpoint_length(const Temporal *) meos_geo.h:673
    // ------------------------------------------------------------------
    public static final UDF1<String, Double> length = (trip) -> {
        if (trip == null) return null;
        MeosThread.ensureReady();
        Pointer tptr = functions.temporal_from_hexwkb(trip);
        if (tptr == null) return null;
        try {
            return functions.tpoint_length(tptr);
        } finally {
            MeosMemory.free(tptr);
        }
    };

    // ------------------------------------------------------------------
    // bboxOverlaps(trip STRING, other STRING) → BOOLEAN
    // Replaces the && bounding-box overlap operator in BerlinMOD SQL.
    // other can be:
    //   - a tstzspan literal ("[2020-..., 2020-...]") → temporal overlap
    //   - an STBox hex-WKB string (from geoTimeStbox / expandSpace)    → spatial overlap
    // MEOS: overlaps_temporal_tstzspan / overlaps_tpoint_stbox (JMEOS-1.5 tpoint variant)
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> bboxOverlaps = (trip, other) -> {
        if (trip == null || other == null) return null;
        MeosThread.ensureReady();
        Pointer tptr = functions.temporal_from_hexwkb(trip);
        if (tptr == null) return null;
        try {
            char first = other.isEmpty() ? 0 : other.charAt(0);
            if (first == '[' || first == '(') {
                Pointer sptr = functions.tstzspan_in(other);
                if (sptr == null) return null;
                try {
                    return functions.overlaps_temporal_tstzspan(tptr, sptr);
                } finally {
                    MeosMemory.free(sptr);
                }
            } else {
                Pointer bptr = functions.stbox_from_hexwkb(other);
                if (bptr == null) return null;
                try {
                    return functions.overlaps_tspatial_stbox(tptr, bptr);
                } finally {
                    MeosMemory.free(bptr);
                }
            }
        } finally {
            MeosMemory.free(tptr);
        }
    };

    // ------------------------------------------------------------------
    // valueAtTimestamp(trip STRING, instant) → STRING (WKT geometry)
    // Returns the geometry of a tgeompoint at a given instant, as WKT.
    // instant can be a java.sql.Timestamp (Spark TIMESTAMP) or String.
    // MEOS: temporal_value_at_timestamptz (MEOS 1.4 canonical name)
    // ------------------------------------------------------------------
    public static final UDF2<String, Object, String> valueAtTimestamp = (trip, tsArg) -> {
        if (trip == null || tsArg == null) return null;
        MeosThread.ensureReady();
        Pointer tptr = functions.temporal_from_hexwkb(trip);
        if (tptr == null) return null;
        OffsetDateTime odt = parseTs(tsArg);
        if (odt == null) return null;
        Pointer valueOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        boolean found = functions.tgeo_value_at_timestamptz(tptr, odt, true, valueOut);
        if (!found) return null;
        long addr = valueOut.getLong(0);
        if (addr == 0L) return null;
        Pointer geomPtr = Runtime.getSystemRuntime().getMemoryManager().newPointer(addr);
        return functions.geo_as_text(geomPtr, 15);
    };

    // ------------------------------------------------------------------
    // geoTimeStbox(geomWkt STRING, instant_or_period) → STRING (STBox hex-WKB)
    // Replaces stbox(geom, instant) and stbox(geom, period) in BerlinMOD SQL.
    // Second arg: java.sql.Timestamp → geo_timestamptz_to_stbox
    //             String "[...]"     → geo_tstzspan_to_stbox
    //             String "..."       → treat as timestamp string
    // MEOS: geo_timestamptz_to_stbox / geo_tstzspan_to_stbox meos_geo.h:506-507
    // ------------------------------------------------------------------
    public static final UDF2<String, Object, String> geoTimeStbox = (geomWkt, tsArg) -> {
        if (geomWkt == null || tsArg == null) return null;
        MeosThread.ensureReady();
        Pointer gptr = functions.geo_from_text(geomWkt, 0);
        if (gptr == null) return null;
        try {
            Pointer result;
            if (tsArg instanceof java.sql.Timestamp) {
                OffsetDateTime odt = toMeosTs((java.sql.Timestamp) tsArg);
                if (odt == null) return null;
                result = functions.geo_timestamptz_to_stbox(gptr, odt);
            } else {
                String s = tsArg.toString().trim();
                if (s.isEmpty()) return null;
                if (s.charAt(0) == '[' || s.charAt(0) == '(') {
                    Pointer sptr = functions.tstzspan_in(s);
                    if (sptr == null) return null;
                    try {
                        result = functions.geo_tstzspan_to_stbox(gptr, sptr);
                    } finally {
                        MeosMemory.free(sptr);
                    }
                } else {
                    OffsetDateTime odt = functions.pg_timestamptz_in(s, -1);
                    if (odt == null) return null;
                    result = functions.geo_timestamptz_to_stbox(gptr, odt);
                }
            }
            if (result == null) return null;
            try {
                Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                return functions.stbox_as_hexwkb(result, (byte) 0, sizeOut);
            } finally {
                MeosMemory.free(result);
            }
        } finally {
            MeosMemory.free(gptr);
        }
    };

    // ------------------------------------------------------------------
    // expandSpace(trip STRING, dist NUMBER) → STRING (STBox hex-WKB)
    // Computes the STBox of a trip then expands it spatially by dist.
    // Used in Q10 as a bounding-box pre-filter: bboxOverlaps(trip, expandSpace(...)).
    // MEOS: tspatial_to_stbox + stbox_expand_space meos_geo.h:548
    // ------------------------------------------------------------------
    public static final UDF2<String, Number, String> expandSpace = (trip, dist) -> {
        if (trip == null || dist == null) return null;
        MeosThread.ensureReady();
        Pointer tptr = functions.temporal_from_hexwkb(trip);
        if (tptr == null) return null;
        try {
            Pointer bbox = functions.tspatial_to_stbox(tptr);
            if (bbox == null) return null;
            try {
                Pointer expanded = functions.stbox_expand_space(bbox, dist.doubleValue());
                if (expanded == null) return null;
                try {
                    Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    return functions.stbox_as_hexwkb(expanded, (byte) 0, sizeOut);
                } finally {
                    MeosMemory.free(expanded);
                }
            } finally {
                MeosMemory.free(bbox);
            }
        } finally {
            MeosMemory.free(tptr);
        }
    };

    // ------------------------------------------------------------------
    // tDwithin(t1 STRING, t2 STRING, dist NUMBER) → STRING (tbool hex-WKB)
    // Returns a temporal boolean showing when the two trajectories are within
    // dist of each other (used by Q10 with whenTrue).
    // MEOS: tdwithin_tpoint_tpoint (JMEOS-1.5 tpoint variant)
    // Note: JMEOS wraps with extra (restr, atvalue) booleans; pass false,false
    // to get the full temporal result.
    // ------------------------------------------------------------------
    public static final UDF3<String, String, Number, String> tDwithin = (t1, t2, dist) -> {
        if (t1 == null || t2 == null || dist == null) return null;
        MeosThread.ensureReady();
        Pointer p1 = functions.temporal_from_hexwkb(t1);
        if (p1 == null) return null;
        try {
            Pointer p2 = functions.temporal_from_hexwkb(t2);
            if (p2 == null) return null;
            try {
                Pointer result = functions.tdwithin_tgeo_tgeo(p1, p2, dist.doubleValue());
                if (result == null) return null;
                try {
                    return functions.temporal_as_hexwkb(result, (byte) 0);
                } finally {
                    MeosMemory.free(result);
                }
            } finally {
                MeosMemory.free(p2);
            }
        } finally {
            MeosMemory.free(p1);
        }
    };

    // ------------------------------------------------------------------
    // whenTrue(tbool STRING) → STRING (tstzspanset text)
    // ------------------------------------------------------------------
    public static final UDF1<String, String> whenTrue = (tbool) -> {
        if (tbool == null) return null;
        MeosThread.ensureReady();
        Pointer tptr = functions.temporal_from_hexwkb(tbool);
        if (tptr == null) return null;
        try {
            Pointer sset = functions.tbool_when_true(tptr);
            if (sset == null) return null;
            try {
                return functions.tstzspanset_out(sset);
            } finally {
                MeosMemory.free(sset);
            }
        } finally {
            MeosMemory.free(tptr);
        }
    };

    // ------------------------------------------------------------------
    // aDisjoint(t1 STRING, t2 STRING) → BOOLEAN
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> aDisjoint = (t1, t2) -> {
        if (t1 == null || t2 == null) return null;
        MeosThread.ensureReady();
        Pointer p1 = functions.temporal_from_hexwkb(t1);
        if (p1 == null) return null;
        try {
            Pointer p2 = functions.temporal_from_hexwkb(t2);
            if (p2 == null) return null;
            try {
                return functions.adisjoint_tgeo_tgeo(p1, p2) == 1;
            } finally {
                MeosMemory.free(p2);
            }
        } finally {
            MeosMemory.free(p1);
        }
    };

    // ------------------------------------------------------------------
    // geomContains(outer STRING, inner STRING) → BOOLEAN
    // Replaces ST_Contains in Q14.  Approximated via econtains_geo_tgeo.
    // ------------------------------------------------------------------
    public static final UDF2<String, String, Boolean> geomContains = (outer, inner) -> {
        if (outer == null || inner == null) return null;
        MeosThread.ensureReady();
        Pointer g1 = functions.geo_from_text(outer, 0);
        if (g1 == null) return null;
        try {
            OffsetDateTime epoch = functions.pg_timestamptz_in("2000-01-01", -1);
            Pointer innerGeo = functions.geo_from_text(inner, 0);
            if (innerGeo == null) return null;
            try {
                Pointer tptr = functions.tpointinst_make(innerGeo, epoch);
                if (tptr == null) return null;
                try {
                    return functions.econtains_geo_tgeo(g1, tptr) == 1;
                } finally {
                    MeosMemory.free(tptr);
                }
            } finally {
                MeosMemory.free(innerGeo);
            }
        } finally {
            MeosMemory.free(g1);
        }
    };

    public static void registerAll(SparkSession spark) {
        spark.udf().register("length",           length,           DataTypes.DoubleType);
        spark.udf().register("bboxOverlaps",     bboxOverlaps,     DataTypes.BooleanType);
        spark.udf().register("valueAtTimestamp", valueAtTimestamp, DataTypes.StringType);
        spark.udf().register("geoTimeStbox",     geoTimeStbox,     DataTypes.StringType);
        spark.udf().register("expandSpace",      expandSpace,      DataTypes.StringType);
        spark.udf().register("tDwithin",         tDwithin,         DataTypes.StringType);
        spark.udf().register("whenTrue",         whenTrue,         DataTypes.StringType);
        spark.udf().register("aDisjoint",        aDisjoint,        DataTypes.BooleanType);
        spark.udf().register("geomContains",     geomContains,     DataTypes.BooleanType);
    }
}
