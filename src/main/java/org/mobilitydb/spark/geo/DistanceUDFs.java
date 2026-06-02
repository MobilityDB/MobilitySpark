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

import functions.GeneratedFunctions;
import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosNative;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
            Pointer tptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gsptr = GeneratedFunctions.geo_from_text(geomWkt, 0);
            if (gsptr == null) { MeosMemory.free(tptr); return null; }
            try {
                Pointer r = GeneratedFunctions.tdistance_tgeo_geo(tptr, gsptr);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
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
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = GeneratedFunctions.tdistance_tgeo_tgeo(p1, p2);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
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
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            try {
                Pointer r = GeneratedFunctions.tdistance_tfloat_float(ptr, d);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
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
            Pointer ptr = GeneratedFunctions.temporal_from_hexwkb(hex);
            if (ptr == null) return null;
            try {
                Pointer r = GeneratedFunctions.tdistance_tint_int(ptr, i);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
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
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(hex1);
            if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(hex2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = GeneratedFunctions.tdistance_tnumber_tnumber(p1, p2);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
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
            Pointer tptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gsptr = GeneratedFunctions.geo_from_text(geomWkt, 0);
            if (gsptr == null) { MeosMemory.free(tptr); return null; }
            try {
                double d = MeosNative.INSTANCE.nad_tgeo_geo(tptr, gsptr);
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
            Pointer tptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer sptr = GeneratedFunctions.stbox_from_hexwkb(stboxHex);
            if (sptr == null) { MeosMemory.free(tptr); return null; }
            try {
                double d = MeosNative.INSTANCE.nad_tgeo_stbox(tptr, sptr);
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
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                double d = MeosNative.INSTANCE.nad_tgeo_tgeo(p1, p2);
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
            Pointer tptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gsptr = GeneratedFunctions.geo_from_text(geomWkt, 0);
            if (gsptr == null) { MeosMemory.free(tptr); return null; }
            try {
                Pointer r = MeosNative.INSTANCE.nai_tgeo_geo(tptr, gsptr);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
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
            Pointer tptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gsptr = GeneratedFunctions.geo_from_text(geomWkt, 0);
            if (gsptr == null) { MeosMemory.free(tptr); return null; }
            try {
                Pointer r = MeosNative.INSTANCE.shortestline_tgeo_geo(tptr, gsptr);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.geo_as_text(r, 6);
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
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = GeneratedFunctions.shortestline_tgeo_tgeo(p1, p2);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.geo_as_text(r, 6);
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
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                Pointer r = MeosNative.INSTANCE.nai_tgeo_tgeo(p1, p2);
                if (r == null) return null;
                try {
                    return GeneratedFunctions.temporal_as_hexwkb(r, (byte) 0);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(p1);
                MeosMemory.free(p2);
            }
        };

    // ------------------------------------------------------------------
    // Minimum spatial distance (MobilityDB PR #1007)
    //   minDistance({tgeo,geo},{tgeo,geo}) → double
    // Per-pair scalar.  For the GROUP-BY-over-cross-join shape that the
    // canonical Q5 expresses, wrap with the built-in MIN aggregate:
    //
    //   SELECT MIN(minDistance(t1.trip, t2.trip)) FROM ... GROUP BY ...
    //
    // The (tgeo, geo) overload reuses the NAD kernel — NAD reduces to
    // spatial-min when one argument has no time dimension. The (tgeo,
    // tgeo) overload calls the threshold-aware kernel with DBL_MAX so
    // every call computes the exact per-pair minimum; the kernel still
    // benefits from the outer STBox lower-bound prune.
    // ------------------------------------------------------------------

    // minDistance(trip STRING, geomWkt STRING) → DOUBLE
    public static final UDF2<String, String, Double> minDistanceTgeoGeo =
        (trip, geomWkt) -> {
            if (trip == null || geomWkt == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = GeneratedFunctions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            Pointer gsptr = GeneratedFunctions.geo_from_text(geomWkt, 0);
            if (gsptr == null) { MeosMemory.free(tptr); return null; }
            try {
                double d = MeosNative.INSTANCE.nad_tgeo_geo(tptr, gsptr);
                return d == Double.MAX_VALUE ? null : d;
            } finally {
                MeosMemory.free(tptr);
                MeosMemory.free(gsptr);
            }
        };

    // minDistance(trip1 STRING, trip2 STRING) → DOUBLE
    public static final UDF2<String, String, Double> minDistanceTgeoTgeo =
        (trip1, trip2) -> {
            if (trip1 == null || trip2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.temporal_from_hexwkb(trip1);
            if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.temporal_from_hexwkb(trip2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try {
                double d = MeosNative.INSTANCE.mindistance_tgeo_tgeo(
                    p1, p2, Double.MAX_VALUE);
                return d == Double.MAX_VALUE ? null : d;
            } finally {
                MeosMemory.free(p1);
                MeosMemory.free(p2);
            }
        };

    // minDistance(trips1 ARRAY<STRING>, trips2 ARRAY<STRING>) → DOUBLE
    // The set-set spatial minimum distance: the minimum distance ever reached
    // between any trip in the first set and any trip in the second.  Backed by
    // the MEOS Tgeoarr_tgeoarr_mindist kernel, which prunes far trip pairs by
    // their STBox lower bound, so the N×N is handled inside one call rather than
    // a SQL Cartesian join.  Mirrors MobilityDB's minDistance(tgeompoint[],
    // tgeompoint[]) so the BerlinMOD Q5 SQL is identical across the platforms:
    //   minDistance(array_agg(t1.trip), array_agg(t2.trip)) GROUP BY licence pair
    public static final UDF2<Object, Object, Double> minDistanceTgeoarrTgeoarr =
        (o1, o2) -> {
            if (o1 == null || o2 == null) return null;
            List<String> hex1 = toStringList(o1);
            List<String> hex2 = toStringList(o2);
            if (hex1.isEmpty() || hex2.isEmpty()) return null;
            MeosThread.ensureReady();
            Runtime rt = Runtime.getSystemRuntime();
            List<Pointer> p1 = new ArrayList<>(hex1.size());
            List<Pointer> p2 = new ArrayList<>(hex2.size());
            try {
                for (String h : hex1) {
                    if (h == null) continue;
                    Pointer p = GeneratedFunctions.temporal_from_hexwkb(h);
                    if (p != null) p1.add(p);
                }
                for (String h : hex2) {
                    if (h == null) continue;
                    Pointer p = GeneratedFunctions.temporal_from_hexwkb(h);
                    if (p != null) p2.add(p);
                }
                if (p1.isEmpty() || p2.isEmpty()) return null;
                Pointer arr1 = Memory.allocateDirect(rt, (long) p1.size() * Long.BYTES);
                Pointer arr2 = Memory.allocateDirect(rt, (long) p2.size() * Long.BYTES);
                for (int i = 0; i < p1.size(); i++) arr1.putPointer((long) i * Long.BYTES, p1.get(i));
                for (int i = 0; i < p2.size(); i++) arr2.putPointer((long) i * Long.BYTES, p2.get(i));
                double d = GeneratedFunctions.mindistance_tgeoarr_tgeoarr(
                    arr1, p1.size(), arr2, p2.size());
                // arr1/arr2 are GC-managed Memory.allocateDirect buffers; keep them
                // strongly reachable until the native call returns, or under GC
                // pressure the JIT can reclaim the backing memory mid-call and the
                // kernel reads a freed Temporal*[] (SIGSEGV).
                java.lang.ref.Reference.reachabilityFence(arr1);
                java.lang.ref.Reference.reachabilityFence(arr2);
                return d == Double.MAX_VALUE ? null : d;
            } finally {
                for (Pointer p : p1) MeosMemory.free(p);
                for (Pointer p : p2) MeosMemory.free(p);
            }
        };

    /** Convert a Spark array column argument (Scala Seq / Java List / array) to a List<String>. */
    @SuppressWarnings("unchecked")
    private static List<String> toStringList(Object o) {
        if (o instanceof List) return (List<String>) o;
        if (o instanceof scala.collection.Seq) {
            // Iterate the Scala Seq directly — works on Scala 2.12 and 2.13 alike,
            // unlike the version-specific CollectionConverters classes.
            scala.collection.Iterator<String> it = ((scala.collection.Seq<String>) o).iterator();
            List<String> l = new ArrayList<>();
            while (it.hasNext()) l.add(it.next());
            return l;
        }
        if (o instanceof Object[]) {
            List<String> l = new ArrayList<>();
            for (Object x : (Object[]) o) l.add((String) x);
            return l;
        }
        throw new IllegalArgumentException("minDistance: unexpected array arg type " + o.getClass());
    }

    // ------------------------------------------------------------------
    // Set-set spatial joins (#1148) — array<struct> of qualifying pairs.
    // Both sides are array<string> of hex-WKB temporals.  The kernel resolves
    // the N×M inside one native call (STBox prune before the exact predicate),
    // returning 0-based (i,j) indexes into the input arrays; the SQL maps them
    // back to identities via a parallel array_agg(identity).  This removes the
    // SQL Cartesian + the bbox / th3index / broadcast prefilter tricks.
    // ------------------------------------------------------------------

    /** Marshal a Spark array<string> of hex-WKB temporals into a native Temporal*[]
     *  direct buffer; parsed pointers are added to {@code ptrs} for the caller to free.
     *  Returns the direct buffer, or null when the array is empty. */
    private static Pointer marshalTrips(Object o, List<Pointer> ptrs, Runtime rt) {
        if (o == null) return null;
        for (String h : toStringList(o)) {
            if (h == null) continue;
            Pointer p = GeneratedFunctions.temporal_from_hexwkb(h);
            if (p != null) ptrs.add(p);
        }
        if (ptrs.isEmpty()) return null;
        Pointer buf = Memory.allocateDirect(rt, (long) ptrs.size() * Long.BYTES);
        for (int i = 0; i < ptrs.size(); i++) buf.putPointer((long) i * Long.BYTES, ptrs.get(i));
        return buf;
    }

    // eDwithinPairs(trips1 ARRAY<STRING>, trips2 ARRAY<STRING>, dist DOUBLE)
    //   → ARRAY<STRUCT<i:INT, j:INT>>  (pairs ever within dist)
    public static final UDF3<Object, Object, Object, List<Row>> eDwithinPairs =
        (o1, o2, dist) -> {
            if (o1 == null || o2 == null || dist == null) return Collections.emptyList();
            double d = ((Number) dist).doubleValue();
            MeosThread.ensureReady();
            Runtime rt = Runtime.getSystemRuntime();
            List<Pointer> p1 = new ArrayList<>(), p2 = new ArrayList<>();
            try {
                Pointer arr1 = marshalTrips(o1, p1, rt);
                Pointer arr2 = marshalTrips(o2, p2, rt);
                if (arr1 == null || arr2 == null) return Collections.emptyList();
                Pointer countPtr = Memory.allocateDirect(rt, Integer.BYTES);
                Pointer res = MeosNative.INSTANCE.edwithin_tgeoarr_tgeoarr(
                    arr1, p1.size(), arr2, p2.size(), d, countPtr);
                java.lang.ref.Reference.reachabilityFence(arr1);
                java.lang.ref.Reference.reachabilityFence(arr2);
                int cnt = countPtr.getInt(0);
                if (res == null || cnt == 0) return Collections.emptyList();
                List<Row> out = new ArrayList<>(cnt);
                for (int k = 0; k < cnt; k++)
                    out.add(RowFactory.create(
                        res.getInt((long) (2 * k) * Integer.BYTES),
                        res.getInt((long) (2 * k + 1) * Integer.BYTES)));
                MeosMemory.free(res);
                return out;
            } finally {
                for (Pointer p : p1) MeosMemory.free(p);
                for (Pointer p : p2) MeosMemory.free(p);
            }
        };

    // aDisjointPairs(trips1 ARRAY<STRING>, trips2 ARRAY<STRING>)
    //   → ARRAY<STRUCT<i:INT, j:INT>>  (pairs always spatially disjoint = ¬eIntersects)
    public static final UDF2<Object, Object, List<Row>> aDisjointPairs =
        (o1, o2) -> {
            if (o1 == null || o2 == null) return Collections.emptyList();
            MeosThread.ensureReady();
            Runtime rt = Runtime.getSystemRuntime();
            List<Pointer> p1 = new ArrayList<>(), p2 = new ArrayList<>();
            try {
                Pointer arr1 = marshalTrips(o1, p1, rt);
                Pointer arr2 = marshalTrips(o2, p2, rt);
                if (arr1 == null || arr2 == null) return Collections.emptyList();
                Pointer countPtr = Memory.allocateDirect(rt, Integer.BYTES);
                Pointer res = MeosNative.INSTANCE.adisjoint_tgeoarr_tgeoarr(
                    arr1, p1.size(), arr2, p2.size(), countPtr);
                java.lang.ref.Reference.reachabilityFence(arr1);
                java.lang.ref.Reference.reachabilityFence(arr2);
                int cnt = countPtr.getInt(0);
                if (res == null || cnt == 0) return Collections.emptyList();
                List<Row> out = new ArrayList<>(cnt);
                for (int k = 0; k < cnt; k++)
                    out.add(RowFactory.create(
                        res.getInt((long) (2 * k) * Integer.BYTES),
                        res.getInt((long) (2 * k + 1) * Integer.BYTES)));
                MeosMemory.free(res);
                return out;
            } finally {
                for (Pointer p : p1) MeosMemory.free(p);
                for (Pointer p : p2) MeosMemory.free(p);
            }
        };

    // tDwithinPairs(trips1 ARRAY<STRING>, trips2 ARRAY<STRING>, dist DOUBLE)
    //   → ARRAY<STRUCT<i:INT, j:INT, periods:STRING>>  (whenTrue spanset hex-WKB per pair)
    public static final UDF3<Object, Object, Object, List<Row>> tDwithinPairs =
        (o1, o2, dist) -> {
            if (o1 == null || o2 == null || dist == null) return Collections.emptyList();
            double d = ((Number) dist).doubleValue();
            MeosThread.ensureReady();
            Runtime rt = Runtime.getSystemRuntime();
            List<Pointer> p1 = new ArrayList<>(), p2 = new ArrayList<>();
            try {
                Pointer arr1 = marshalTrips(o1, p1, rt);
                Pointer arr2 = marshalTrips(o2, p2, rt);
                if (arr1 == null || arr2 == null) return Collections.emptyList();
                Pointer countPtr = Memory.allocateDirect(rt, Integer.BYTES);
                Pointer periodsPtr = Memory.allocateDirect(rt, Long.BYTES);
                Pointer res = MeosNative.INSTANCE.tdwithin_tgeoarr_tgeoarr(
                    arr1, p1.size(), arr2, p2.size(), d, countPtr, periodsPtr);
                java.lang.ref.Reference.reachabilityFence(arr1);
                java.lang.ref.Reference.reachabilityFence(arr2);
                int cnt = countPtr.getInt(0);
                if (res == null || cnt == 0) return Collections.emptyList();
                Pointer ssArr = periodsPtr.getPointer(0);
                List<Row> out = new ArrayList<>(cnt);
                for (int k = 0; k < cnt; k++) {
                    int i = res.getInt((long) (2 * k) * Integer.BYTES);
                    int j = res.getInt((long) (2 * k + 1) * Integer.BYTES);
                    Pointer ss = ssArr.getPointer((long) k * Long.BYTES);
                    String periods = ss == null ? null : GeneratedFunctions.spanset_as_hexwkb(ss, (byte) 0);
                    MeosMemory.free(ss);
                    out.add(RowFactory.create(i, j, periods));
                }
                MeosMemory.free(ssArr);
                MeosMemory.free(res);
                return out;
            } finally {
                for (Pointer p : p1) MeosMemory.free(p);
                for (Pointer p : p2) MeosMemory.free(p);
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

        spark.udf().register("minDistanceTgeoGeo",    minDistanceTgeoGeo,    DataTypes.DoubleType);
        spark.udf().register("minDistanceTgeoTgeo",   minDistanceTgeoTgeo,   DataTypes.DoubleType);
        spark.udf().register("minDistanceTgeoarrTgeoarr", minDistanceTgeoarrTgeoarr, DataTypes.DoubleType);

        // MobilityDB SQL bare-name aliases.
        // The portable operator alias `nearestApproachDistance` (|=|) is
        // registered by org.mobilitydb.spark.portable.PortableOperatorAliasUDFs,
        // reusing this same nadTgeoGeo backing field.
        spark.udf().register("nearestApproachInstant",  naiTgeoGeo,  DataTypes.StringType);
        spark.udf().register("shortestLine",            shortestLineTgeoGeo, DataTypes.StringType);
        // Bare `minDistance` resolves to the set-set array form (the BerlinMOD Q5
        // spatial-min over two trip sets); Spark cannot overload by signature, and
        // the scalar forms stay reachable as minDistanceTgeoTgeo / minDistanceTgeoGeo.
        spark.udf().register("minDistance",             minDistanceTgeoarrTgeoarr, DataTypes.DoubleType);

        // Set-set spatial joins (#1148) — array<struct> of qualifying (i,j) pairs.
        StructType pairType = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("i", DataTypes.IntegerType, false),
            DataTypes.createStructField("j", DataTypes.IntegerType, false) });
        StructType pairPeriodsType = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("i", DataTypes.IntegerType, false),
            DataTypes.createStructField("j", DataTypes.IntegerType, false),
            DataTypes.createStructField("periods", DataTypes.StringType, true) });
        spark.udf().register("eDwithinPairs",  eDwithinPairs,  DataTypes.createArrayType(pairType));
        spark.udf().register("aDisjointPairs", aDisjointPairs, DataTypes.createArrayType(pairType));
        spark.udf().register("tDwithinPairs",  tDwithinPairs,  DataTypes.createArrayType(pairPeriodsType));
    }
}
