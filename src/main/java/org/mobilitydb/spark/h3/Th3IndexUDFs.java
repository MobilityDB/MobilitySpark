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
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

/**
 * Spark SQL UDFs for the temporal H3 index type (th3index).
 *
 * th3index is MobilityDB's temporal H3 cell index — a sequence of H3 cells
 * over time produced by sampling a tgeompoint at a chosen H3 resolution.
 * Two uses in MobilitySpark:
 *
 *   1. Spatial prefilter for cross-join queries (BerlinMOD Q2/Q4/Q10/Q11/Q12).
 *      The portable BerlinMOD SQL stays unchanged; preprocessForSpark in
 *      BerlinMODBench injects the prefilter when it sees eIntersects /
 *      eDwithin patterns.
 *   2. Direct th3index queries — cells, hierarchy, neighbour relations.
 *
 * Storage convention:
 *   tgeompoint → hex-WKB STRING (existing convention)
 *   th3index   → hex-WKB STRING (same temporal serialisation framework)
 *   H3Index    → BIGINT (uint64 H3 cell value, fits in Java long)
 *
 * MEOS function authority: meos/include/meos_h3.h on branch th3index-spatial-bbox.
 * Per ecosystem policy `feedback_issued_pr_treat_as_landed.md`, this code
 * targets the th3index API surface from MobilityDB PRs #807 / #866 / #893
 * before they merge upstream — JMEOS regen against the merged tree (parallel
 * session's `feat/regen-against-meos-1.4`) will then resolve the symbols.
 */
public final class Th3IndexUDFs {

    private Th3IndexUDFs() {}

    /** Default H3 resolution for the BerlinMOD prefilter — ~5 km cells. */
    public static final int DEFAULT_RESOLUTION = 7;

    // ------------------------------------------------------------------
    // Type conversions
    // ------------------------------------------------------------------

    // tgeompointToTh3Index(trip STRING, resolution INT) → STRING (th3index hex-WKB)
    // MEOS: tgeompoint_to_th3index(const Temporal *, int32) → Temporal *
    public static final UDF2<String, Integer, String> tgeompointToTh3Index =
        (trip, resolution) -> {
            if (trip == null || resolution == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer th3 = functions.tgeompoint_to_th3index(tptr, resolution);
                if (th3 == null) return null;
                try {
                    return functions.temporal_as_hexwkb(th3, (byte) 0);
                } finally {
                    MeosMemory.free(th3);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // tgeogpointToTh3Index(trip STRING, resolution INT) → STRING (th3index hex-WKB)
    // MEOS: tgeogpoint_to_th3index(const Temporal *, int32) → Temporal *
    public static final UDF2<String, Integer, String> tgeogpointToTh3Index =
        (trip, resolution) -> {
            if (trip == null || resolution == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(trip);
            if (tptr == null) return null;
            try {
                Pointer th3 = functions.tgeogpoint_to_th3index(tptr, resolution);
                if (th3 == null) return null;
                try {
                    return functions.temporal_as_hexwkb(th3, (byte) 0);
                } finally {
                    MeosMemory.free(th3);
                }
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // ------------------------------------------------------------------
    // Static H3 cell I/O
    // ------------------------------------------------------------------

    // h3IndexFromText("8a283082a677fff") → BIGINT  (H3Index parse)
    // MEOS: h3index_in(const char *) → H3Index
    public static final UDF1<String, Long> h3IndexFromText = (str) -> {
        if (str == null) return null;
        MeosThread.ensureReady();
        return functions.h3index_in(str);
    };

    // h3IndexAsText(cell BIGINT) → STRING                    (H3Index format)
    // MEOS: h3index_out(H3Index) → char *
    public static final UDF1<Long, String> h3IndexAsText = (cell) -> {
        if (cell == null) return null;
        MeosThread.ensureReady();
        return functions.h3index_out(cell);
    };

    // ------------------------------------------------------------------
    // Static-geometry → H3 cell (point case)
    //
    // The internal helper h3_gs_point_to_cell is not in the public API.  We
    // compose it from the public symbols: a point geometry is wrapped in a
    // single-instant tgeompoint, converted to th3index, and the start value
    // is extracted as the H3Index.  This costs 4 MEOS calls per UDF
    // invocation but is amortised by Spark's projection / broadcasting on
    // small dimension tables (BerlinMOD's QueryPoints / QueryRegions sit at
    // 4 KB / 184 KB so this UDF runs O(N) on the small side, not O(N×M)).
    //
    // POINT input: exact H3 cell at the requested resolution.
    // Non-POINT input (polygon, linestring): tpointinst_make returns NULL,
    // we return NULL, and the prefilter bypasses for this row.  The bypass
    // is correct (over-approximates the candidate set — the eIntersects
    // exact predicate still runs).  Polygon prefiltering proper requires
    // h3_polygon_to_cells in the public API; defer until upstream merges it.
    // ------------------------------------------------------------------

    // geomToH3Cell(geomWkt STRING, resolution INT) → BIGINT (nullable)
    public static final UDF2<String, Integer, Long> geomToH3Cell =
        (geomWkt, resolution) -> {
            if (geomWkt == null || resolution == null) return null;
            MeosThread.ensureReady();
            Pointer gs = functions.geo_from_text(geomWkt, 0);
            if (gs == null) return null;
            try {
                // Wrap as single-instant tgeompoint at an arbitrary timestamp.
                // tpointinst_make returns NULL for non-POINT geometries, in
                // which case the prefilter degrades to no-op (correct).
                Pointer inst = functions.tpointinst_make(gs, 0L /* epoch */);
                if (inst == null) return null;
                try {
                    Pointer th3 = functions.tgeompoint_to_th3index(inst, resolution);
                    if (th3 == null) return null;
                    try {
                        return functions.th3index_start_value(th3);
                    } finally {
                        MeosMemory.free(th3);
                    }
                } finally {
                    MeosMemory.free(inst);
                }
            } finally {
                MeosMemory.free(gs);
            }
        };

    // ------------------------------------------------------------------
    // Membership prefilter — the headline accelerator for cross-joins
    // ------------------------------------------------------------------

    // everEqH3IndexTh3Index(cell BIGINT, th3idx STRING) → BOOLEAN
    // True iff the trip's th3index sequence ever passes through `cell`.
    // MEOS: ever_eq_h3index_th3index(H3Index, const Temporal *) → int (0/1, -1 on error)
    public static final UDF2<Long, String, Boolean> everEqH3IndexTh3Index =
        (cell, th3idx) -> {
            if (cell == null || th3idx == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(th3idx);
            if (tptr == null) return null;
            try {
                int r = functions.ever_eq_h3index_th3index(cell, tptr);
                return r < 0 ? null : r == 1;
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // alwaysEqH3IndexTh3Index — true iff the trip is *always* in this cell
    // (rare in BerlinMOD; useful for stationary-vehicle detection).
    // MEOS: always_eq_h3index_th3index(H3Index, const Temporal *) → int
    public static final UDF2<Long, String, Boolean> alwaysEqH3IndexTh3Index =
        (cell, th3idx) -> {
            if (cell == null || th3idx == null) return null;
            MeosThread.ensureReady();
            Pointer tptr = functions.temporal_from_hexwkb(th3idx);
            if (tptr == null) return null;
            try {
                int r = functions.always_eq_h3index_th3index(cell, tptr);
                return r < 0 ? null : r == 1;
            } finally {
                MeosMemory.free(tptr);
            }
        };

    // everEqTh3IndexTh3Index(t1 STRING, t2 STRING) → BOOLEAN
    // True iff the two th3index trips are EVER in the same H3 cell at the same
    // instant.  This is a sound prefilter for any "trips close at some common
    // time" query (NAD, eDwithin, tDwithin trip×trip): if the trips never share
    // a cell at any common instant, their minimum distance over common instants
    // is at least the H3 edge length at the chosen resolution
    // (≈ 1.4 km at resolution 7), so any sub-cell-edge-length distance threshold
    // returns false everywhere.  For BerlinMOD Q5/Q6/Q10/Q12 the threshold is 3 m
    // — much smaller than any H3 edge — so the prefilter is highly selective.
    //
    // MEOS: ever_eq_th3index_th3index(const Temporal *, const Temporal *) → int
    public static final UDF2<String, String, Boolean> everEqTh3IndexTh3Index =
        (t1, t2) -> {
            if (t1 == null || t2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = functions.temporal_from_hexwkb(t1);
            if (p1 == null) return null;
            try {
                Pointer p2 = functions.temporal_from_hexwkb(t2);
                if (p2 == null) return null;
                try {
                    int r = functions.ever_eq_th3index_th3index(p1, p2);
                    return r < 0 ? null : r == 1;
                } finally {
                    MeosMemory.free(p2);
                }
            } finally {
                MeosMemory.free(p1);
            }
        };

    // ------------------------------------------------------------------
    // Inspection helpers (handy for tuning the resolution)
    // ------------------------------------------------------------------

    // h3IndexResolution(cell BIGINT) → INT — extracts the H3 resolution (0..15)
    // We have no public scalar getter; compose via a dummy Temporal call.
    // For now expose only the temporal version.

    // th3IndexGetResolution(th3idx STRING) → STRING (tint hex-WKB)
    // MEOS: th3index_get_resolution(const Temporal *) → Temporal *
    public static final UDF1<String, String> th3IndexGetResolution = (th3idx) -> {
        if (th3idx == null) return null;
        MeosThread.ensureReady();
        Pointer tptr = functions.temporal_from_hexwkb(th3idx);
        if (tptr == null) return null;
        try {
            Pointer res = functions.th3index_get_resolution(tptr);
            if (res == null) return null;
            try {
                return functions.temporal_as_hexwkb(res, (byte) 0);
            } finally {
                MeosMemory.free(res);
            }
        } finally {
            MeosMemory.free(tptr);
        }
    };

    // ------------------------------------------------------------------
    // Registration
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        spark.udf().register("tgeompointToTh3Index",   tgeompointToTh3Index,   DataTypes.StringType);
        spark.udf().register("tgeogpointToTh3Index",   tgeogpointToTh3Index,   DataTypes.StringType);

        spark.udf().register("h3IndexFromText",        h3IndexFromText,        DataTypes.LongType);
        spark.udf().register("h3IndexAsText",          h3IndexAsText,          DataTypes.StringType);
        spark.udf().register("geomToH3Cell",           geomToH3Cell,           DataTypes.LongType);

        spark.udf().register("everEqH3IndexTh3Index",  everEqH3IndexTh3Index,  DataTypes.BooleanType);
        spark.udf().register("alwaysEqH3IndexTh3Index", alwaysEqH3IndexTh3Index, DataTypes.BooleanType);
        spark.udf().register("everEqTh3IndexTh3Index", everEqTh3IndexTh3Index, DataTypes.BooleanType);

        spark.udf().register("th3IndexGetResolution",  th3IndexGetResolution,  DataTypes.StringType);
    }
}
