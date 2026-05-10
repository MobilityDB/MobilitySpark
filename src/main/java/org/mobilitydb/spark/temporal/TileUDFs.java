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

package org.mobilitydb.spark.temporal;

import functions.functions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosNative;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.api.java.UDF7;
import org.apache.spark.sql.api.java.UDF8;
import org.apache.spark.sql.api.java.UDF9;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;

/**
 * Spark SQL UDFs for multidimensional tiling — split a temporal value into
 * fixed-size cells (in space, time, value, or combinations) so the resulting
 * cells can be processed in parallel and the per-cell results merged.
 *
 * The "boxes" variants return the bounding STBox/TBox of each cell that the
 * temporal value intersects (lighter-weight, preserves no instants).
 * The "split" variants (in {@link AccessorAliasUDFs}) return an array of
 * sub-temporal values, one per cell.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h,
 *   meos/include/meos_internal.h
 */
public final class TileUDFs {

    private TileUDFs() {}

    private static final int STBOX_SIZE = 80;
    private static final int TBOX_SIZE  = 56;

    private static long pgEpoch(Timestamp ts) {
        return (ts.getTime() - 946684800L * 1000L) * 1000L;
    }

    // ------------------------------------------------------------------
    // Single-tile lookups
    // ------------------------------------------------------------------

    // getTimeTile(timestamptz, intervalStr, originTs) → STBox hex (the cell
    //   containing this timestamp at the given resolution)
    public static final UDF3<Timestamp, String, Timestamp, String> getTimeTile =
        (t, intervalStr, torigin) -> {
            if (t == null || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) return null;
            try {
                Pointer r = functions.MeosLibrary.meos.stbox_get_time_tile(pgEpoch(t), iv, pgEpoch(torigin));
                if (r == null) return null;
                try {
                    Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    return functions.stbox_as_hexwkb(r, (byte) 0, sizeOut);
                } finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(iv); }
        };

    // getSpaceTile(pointWKT, xsize, ysize, zsize, originPointWKT) → STBox hex
    public static final UDF5<String, Double, Double, Double, String, String> getSpaceTile =
        (pointWkt, xsize, ysize, zsize, originWkt) -> {
            if (pointWkt == null || xsize == null || ysize == null || zsize == null) return null;
            MeosThread.ensureReady();
            Pointer pt = functions.geo_from_text(pointWkt, 0);
            if (pt == null) return null;
            Pointer origin = (originWkt == null) ? null : functions.geo_from_text(originWkt, 0);
            try {
                Pointer r = functions.MeosLibrary.meos.stbox_get_space_tile(pt, xsize, ysize, zsize, origin);
                if (r == null) return null;
                try {
                    Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    return functions.stbox_as_hexwkb(r, (byte) 0, sizeOut);
                } finally { MeosMemory.free(r); }
            } finally {
                MeosMemory.free(pt);
                if (origin != null) MeosMemory.free(origin);
            }
        };

    // getSpaceTimeTile(pointWKT, t, xsize, ysize, zsize, intervalStr, originPointWKT, torigin) → STBox hex
    public static final UDF8<String, Timestamp, Double, Double, Double, String, String, Timestamp, String>
        getSpaceTimeTile = (pointWkt, t, xsize, ysize, zsize, intervalStr, originWkt, torigin) -> {
            if (pointWkt == null || t == null || xsize == null || ysize == null || zsize == null
                || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer pt = functions.geo_from_text(pointWkt, 0);
            if (pt == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(pt); return null; }
            Pointer origin = (originWkt == null) ? null : functions.geo_from_text(originWkt, 0);
            try {
                Pointer r = functions.MeosLibrary.meos.stbox_get_space_time_tile(
                    pt, pgEpoch(t), xsize, ysize, zsize, iv, origin, pgEpoch(torigin));
                if (r == null) return null;
                try {
                    Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    return functions.stbox_as_hexwkb(r, (byte) 0, sizeOut);
                } finally { MeosMemory.free(r); }
            } finally {
                MeosMemory.free(pt, iv);
                if (origin != null) MeosMemory.free(origin);
            }
        };

    // ------------------------------------------------------------------
    // Multi-tile bounding-box arrays
    // ------------------------------------------------------------------

    // spaceBoxes(tgeo, xsize, ysize, zsize, originPointWKT, bitmatrix, borderInc) → STBox[]
    public static final UDF7<String, Double, Double, Double, String, Boolean, Boolean, String[]>
        spaceBoxes = (trip, xsize, ysize, zsize, originWkt, bitmatrix, borderInc) -> {
            if (trip == null || xsize == null || ysize == null || zsize == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer origin = (originWkt == null) ? null : functions.geo_from_text(originWkt, 0);
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.tgeo_space_boxes(t, xsize, ysize, zsize, origin,
                    bitmatrix != null && bitmatrix, borderInc == null ? true : borderInc, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++)
                        out[i] = functions.stbox_as_hexwkb(arr.slice(i * STBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally {
                MeosMemory.free(t);
                if (origin != null) MeosMemory.free(origin);
            }
        };

    // spaceTimeBoxes(tgeo, xsize, ysize, zsize, intervalStr, originPointWKT, torigin, bitmatrix, borderInc) → STBox[]
    public static final UDF9<String, Double, Double, Double, String, String, Timestamp, Boolean, Boolean, String[]>
        spaceTimeBoxes = (trip, xsize, ysize, zsize, intervalStr, originWkt, torigin, bitmatrix, borderInc) -> {
            if (trip == null || xsize == null || ysize == null || zsize == null
                || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(t); return null; }
            Pointer origin = (originWkt == null) ? null : functions.geo_from_text(originWkt, 0);
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.MeosLibrary.meos.tgeo_space_time_boxes(
                    t, xsize, ysize, zsize, iv, origin, pgEpoch(torigin),
                    bitmatrix != null && bitmatrix, borderInc == null ? true : borderInc, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++)
                        out[i] = functions.stbox_as_hexwkb(arr.slice(i * STBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally {
                MeosMemory.free(t, iv);
                if (origin != null) MeosMemory.free(origin);
            }
        };

    // valueTimeBoxesTfloat(tfloat, vsize, intervalStr, vorigin, torigin) → TBox[]
    // Datum vsize/vorigin: float Datum is the IEEE 754 bits via doubleToLongBits.
    public static final UDF5<String, Double, String, Double, Timestamp, String[]>
        valueTimeBoxesTfloat = (trip, vsize, intervalStr, vorigin, torigin) -> {
            if (trip == null || vsize == null || intervalStr == null || vorigin == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(t); return null; }
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = MeosNative.INSTANCE.tnumber_value_time_boxes(
                    t, Double.doubleToLongBits(vsize), iv,
                    Double.doubleToLongBits(vorigin), pgEpoch(torigin), countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++)
                        out[i] = functions.tbox_as_hexwkb(arr.slice(i * TBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(t, iv); }
        };

    // valueTimeBoxesTint(tint, vsize, intervalStr, vorigin, torigin) → TBox[]
    public static final UDF5<String, Integer, String, Integer, Timestamp, String[]>
        valueTimeBoxesTint = (trip, vsize, intervalStr, vorigin, torigin) -> {
            if (trip == null || vsize == null || intervalStr == null || vorigin == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(t); return null; }
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = MeosNative.INSTANCE.tnumber_value_time_boxes(
                    t, vsize.longValue(), iv, vorigin.longValue(), pgEpoch(torigin), countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++)
                        out[i] = functions.tbox_as_hexwkb(arr.slice(i * TBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(t, iv); }
        };

    // ------------------------------------------------------------------
    // Splits — return Temporal** array (each element is a pointer to a
    // sub-temporal value).  Iterate by reading 8-byte pointers and
    // dereferencing each.
    // ------------------------------------------------------------------

    // timeSplit(temporal, intervalStr, torigin) → array of temporal hex-WKB
    public static final UDF3<String, String, Timestamp, String[]>
        timeSplit = (trip, intervalStr, torigin) -> {
            if (trip == null || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(t); return null; }
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer binsOut  = rt.getMemoryManager().allocateDirect(8);
                Pointer arr = functions.MeosLibrary.meos.temporal_time_split(t, iv, pgEpoch(torigin), binsOut, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) {
                        Pointer p = arr.getPointer(i * 8L);
                        if (p == null) continue;
                        out[i] = functions.temporal_as_hexwkb(p, (byte) 0);
                        MeosMemory.free(p);
                    }
                    return out;
                } finally {
                    MeosMemory.free(arr);
                    Pointer bins = binsOut.getPointer(0);
                    if (bins != null) MeosMemory.free(bins);
                }
            } finally { MeosMemory.free(t, iv); }
        };

    // spaceSplit(tgeo, xsize, ysize, zsize, originPointWKT, bitmatrix, borderInc) → array of temporal hex-WKB
    public static final UDF7<String, Double, Double, Double, String, Boolean, Boolean, String[]>
        spaceSplit = (trip, xsize, ysize, zsize, originWkt, bitmatrix, borderInc) -> {
            if (trip == null || xsize == null || ysize == null || zsize == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer origin = (originWkt == null) ? null : functions.geo_from_text(originWkt, 0);
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer binsOut  = rt.getMemoryManager().allocateDirect(8);
                Pointer arr = functions.MeosLibrary.meos.tgeo_space_split(t, xsize, ysize, zsize, origin,
                    bitmatrix != null && bitmatrix, borderInc == null ? true : borderInc, binsOut, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) {
                        Pointer p = arr.getPointer(i * 8L);
                        if (p == null) continue;
                        out[i] = functions.temporal_as_hexwkb(p, (byte) 0);
                        MeosMemory.free(p);
                    }
                    return out;
                } finally {
                    MeosMemory.free(arr);
                    Pointer bins = binsOut.getPointer(0);
                    if (bins != null) MeosMemory.free(bins);
                }
            } finally {
                MeosMemory.free(t);
                if (origin != null) MeosMemory.free(origin);
            }
        };

    // spaceTimeSplit(tgeo, xsize, ysize, zsize, intervalStr, originPointWKT, torigin, bitmatrix, borderInc) → array
    public static final UDF9<String, Double, Double, Double, String, String, Timestamp, Boolean, Boolean, String[]>
        spaceTimeSplit = (trip, xsize, ysize, zsize, intervalStr, originWkt, torigin, bitmatrix, borderInc) -> {
            if (trip == null || xsize == null || ysize == null || zsize == null
                || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(t); return null; }
            Pointer origin = (originWkt == null) ? null : functions.geo_from_text(originWkt, 0);
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut    = rt.getMemoryManager().allocateDirect(4);
                Pointer spaceBinsOut = rt.getMemoryManager().allocateDirect(8);
                Pointer timeBinsOut  = rt.getMemoryManager().allocateDirect(8);
                Pointer arr = functions.MeosLibrary.meos.tgeo_space_time_split(
                    t, xsize, ysize, zsize, iv, origin, pgEpoch(torigin),
                    bitmatrix != null && bitmatrix, borderInc == null ? true : borderInc,
                    spaceBinsOut, timeBinsOut, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) {
                        Pointer p = arr.getPointer(i * 8L);
                        if (p == null) continue;
                        out[i] = functions.temporal_as_hexwkb(p, (byte) 0);
                        MeosMemory.free(p);
                    }
                    return out;
                } finally {
                    MeosMemory.free(arr);
                    Pointer sb = spaceBinsOut.getPointer(0);
                    Pointer tb = timeBinsOut.getPointer(0);
                    if (sb != null) MeosMemory.free(sb);
                    if (tb != null) MeosMemory.free(tb);
                }
            } finally {
                MeosMemory.free(t, iv);
                if (origin != null) MeosMemory.free(origin);
            }
        };

    // ------------------------------------------------------------------
    // Bounded tile-set generators — given a bounds box (STBox/TBox) and
    // tile sizes, enumerate every tile in the bounds.  These are the
    // primary parallel-partitioning primitives.
    // ------------------------------------------------------------------

    // spaceTiles(boundsStboxHex, xsize, ysize, zsize, originPointWkt, borderInc) → STBox[]
    public static final UDF6<String, Double, Double, Double, String, Boolean, String[]>
        spaceTiles = (boundsHex, xsize, ysize, zsize, originWkt, borderInc) -> {
            if (boundsHex == null || xsize == null || ysize == null || zsize == null) return null;
            MeosThread.ensureReady();
            Pointer b = functions.stbox_from_hexwkb(boundsHex);
            if (b == null) return null;
            Pointer origin = (originWkt == null) ? null : functions.geo_from_text(originWkt, 0);
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.stbox_space_tiles(b, xsize, ysize, zsize, origin,
                    borderInc == null ? true : borderInc, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++)
                        out[i] = functions.stbox_as_hexwkb(arr.slice(i * STBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally {
                MeosMemory.free(b);
                if (origin != null) MeosMemory.free(origin);
            }
        };

    // timeTiles(stbox, intervalStr, torigin, borderInc) → STBox[]
    public static final UDF4<String, String, Timestamp, Boolean, String[]>
        stboxTimeTiles = (boundsHex, intervalStr, torigin, borderInc) -> {
            if (boundsHex == null || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer b = functions.stbox_from_hexwkb(boundsHex);
            if (b == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(b); return null; }
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.MeosLibrary.meos.stbox_time_tiles(b, iv, pgEpoch(torigin),
                    borderInc == null ? true : borderInc, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++)
                        out[i] = functions.stbox_as_hexwkb(arr.slice(i * STBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(b, iv); }
        };

    // spaceTimeTiles(stbox, xsize, ysize, zsize, intervalStr, originPointWkt, torigin, borderInc) → STBox[]
    public static final UDF8<String, Double, Double, Double, String, String, Timestamp, Boolean, String[]>
        spaceTimeTiles = (boundsHex, xsize, ysize, zsize, intervalStr, originWkt, torigin, borderInc) -> {
            if (boundsHex == null || xsize == null || ysize == null || zsize == null
                || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer b = functions.stbox_from_hexwkb(boundsHex);
            if (b == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(b); return null; }
            Pointer origin = (originWkt == null) ? null : functions.geo_from_text(originWkt, 0);
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer countOut = rt.getMemoryManager().allocateDirect(4);
                Pointer arr = functions.MeosLibrary.meos.stbox_space_time_tiles(b, xsize, ysize, zsize, iv,
                    origin, pgEpoch(torigin), borderInc == null ? true : borderInc, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++)
                        out[i] = functions.stbox_as_hexwkb(arr.slice(i * STBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally {
                MeosMemory.free(b, iv);
                if (origin != null) MeosMemory.free(origin);
            }
        };

    // tboxTimeTiles(tbox, intervalStr, torigin) → TBox[] — value-bounded time tiling
    public static final UDF3<String, String, Timestamp, String[]>
        tintboxTimeTiles = (boundsHex, intervalStr, torigin) -> tboxTimeTilesImpl(boundsHex, intervalStr, torigin, false);

    public static final UDF3<String, String, Timestamp, String[]>
        tfloatboxTimeTiles = (boundsHex, intervalStr, torigin) -> tboxTimeTilesImpl(boundsHex, intervalStr, torigin, true);

    private static String[] tboxTimeTilesImpl(String boundsHex, String intervalStr, Timestamp torigin, boolean isFloat) {
        if (boundsHex == null || intervalStr == null || torigin == null) return null;
        MeosThread.ensureReady();
        Pointer b = functions.tbox_from_hexwkb(boundsHex);
        if (b == null) return null;
        Pointer iv = functions.pg_interval_in(intervalStr, -1);
        if (iv == null) { MeosMemory.free(b); return null; }
        try {
            jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
            Pointer countOut = rt.getMemoryManager().allocateDirect(4);
            Pointer arr = isFloat
                ? functions.MeosLibrary.meos.tfloatbox_time_tiles(b, iv, pgEpoch(torigin), countOut)
                : functions.MeosLibrary.meos.tintbox_time_tiles(b, iv, pgEpoch(torigin), countOut);
            if (arr == null) return null;
            try {
                int n = countOut.getInt(0);
                String[] out = new String[n];
                Pointer sizeOut = rt.getMemoryManager().allocateDirect(8);
                for (int i = 0; i < n; i++)
                    out[i] = functions.tbox_as_hexwkb(arr.slice(i * TBOX_SIZE), (byte) 0, sizeOut);
                return out;
            } finally { MeosMemory.free(arr); }
        } finally { MeosMemory.free(b, iv); }
    }

    // makeSimple(tpoint) — returns array of simple sub-tpoints
    public static final UDF1<String, String[]> makeSimple =
        trip -> {
            if (trip == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            try {
                Pointer countOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4);
                Pointer arr = functions.tpoint_make_simple(t, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    for (int i = 0; i < n; i++) {
                        Pointer p = arr.getPointer(i * 8L);
                        if (p == null) continue;
                        out[i] = functions.temporal_as_hexwkb(p, (byte) 0);
                        MeosMemory.free(p);
                    }
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(t); }
        };

    // timeBoxes(tnumber) → TBox[] — go through tnumber_to_tbox then tile by time.
    // For tfloat default; tint variant uses tintbox.
    public static final UDF3<String, String, Timestamp, String[]>
        tfloatTimeBoxes = (trip, intervalStr, torigin) -> {
            if (trip == null || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(t); return null; }
            try {
                Pointer box = functions.tnumber_to_tbox(t);
                if (box == null) return null;
                try {
                    Pointer countOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4);
                    Pointer arr = functions.MeosLibrary.meos.tfloatbox_time_tiles(box, iv, pgEpoch(torigin), countOut);
                    if (arr == null) return null;
                    try {
                        int n = countOut.getInt(0);
                        String[] out = new String[n];
                        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                        for (int i = 0; i < n; i++)
                            out[i] = functions.tbox_as_hexwkb(arr.slice(i * TBOX_SIZE), (byte) 0, sizeOut);
                        return out;
                    } finally { MeosMemory.free(arr); }
                } finally { MeosMemory.free(box); }
            } finally { MeosMemory.free(t, iv); }
        };

    public static final UDF3<String, String, Timestamp, String[]>
        tintTimeBoxes = (trip, intervalStr, torigin) -> {
            if (trip == null || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(t); return null; }
            try {
                Pointer box = functions.tnumber_to_tbox(t);
                if (box == null) return null;
                try {
                    Pointer countOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4);
                    Pointer arr = functions.MeosLibrary.meos.tintbox_time_tiles(box, iv, pgEpoch(torigin), countOut);
                    if (arr == null) return null;
                    try {
                        int n = countOut.getInt(0);
                        String[] out = new String[n];
                        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                        for (int i = 0; i < n; i++)
                            out[i] = functions.tbox_as_hexwkb(arr.slice(i * TBOX_SIZE), (byte) 0, sizeOut);
                        return out;
                    } finally { MeosMemory.free(arr); }
                } finally { MeosMemory.free(box); }
            } finally { MeosMemory.free(t, iv); }
        };

    // timeBoxes(tgeo, intervalStr, torigin, ...) → STBox[] — extract STBox first
    public static final UDF3<String, String, Timestamp, String[]>
        tgeoTimeBoxes = (trip, intervalStr, torigin) -> {
            if (trip == null || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer t = functions.temporal_from_hexwkb(trip);
            if (t == null) return null;
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) { MeosMemory.free(t); return null; }
            try {
                Pointer box = functions.tspatial_to_stbox(t);
                if (box == null) return null;
                try {
                    Pointer countOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4);
                    Pointer arr = functions.MeosLibrary.meos.stbox_time_tiles(box, iv, pgEpoch(torigin), true, countOut);
                    if (arr == null) return null;
                    try {
                        int n = countOut.getInt(0);
                        String[] out = new String[n];
                        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                        for (int i = 0; i < n; i++)
                            out[i] = functions.stbox_as_hexwkb(arr.slice(i * STBOX_SIZE), (byte) 0, sizeOut);
                        return out;
                    } finally { MeosMemory.free(arr); }
                } finally { MeosMemory.free(box); }
            } finally { MeosMemory.free(t, iv); }
        };

    // tfloatValueTiles / tintValueTiles — value-only TBox tiling
    public static final UDF3<String, Double, Double, String[]>
        tfloatValueTiles = (boxHex, vsize, vorigin) -> {
            if (boxHex == null || vsize == null || vorigin == null) return null;
            MeosThread.ensureReady();
            Pointer b = functions.tbox_from_hexwkb(boxHex);
            if (b == null) return null;
            try {
                Pointer countOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4);
                Pointer arr = functions.tfloatbox_value_tiles(b, vsize, vorigin, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++)
                        out[i] = functions.tbox_as_hexwkb(arr.slice(i * TBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(b); }
        };

    public static final UDF3<String, Integer, Integer, String[]>
        tintValueTiles = (boxHex, vsize, vorigin) -> {
            if (boxHex == null || vsize == null || vorigin == null) return null;
            MeosThread.ensureReady();
            Pointer b = functions.tbox_from_hexwkb(boxHex);
            if (b == null) return null;
            try {
                Pointer countOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(4);
                Pointer arr = functions.tintbox_value_tiles(b, vsize, vorigin, countOut);
                if (arr == null) return null;
                try {
                    int n = countOut.getInt(0);
                    String[] out = new String[n];
                    Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    for (int i = 0; i < n; i++)
                        out[i] = functions.tbox_as_hexwkb(arr.slice(i * TBOX_SIZE), (byte) 0, sizeOut);
                    return out;
                } finally { MeosMemory.free(arr); }
            } finally { MeosMemory.free(b); }
        };

    // tfloatValueSplit / tintValueSplit — Temporal** array of value-tiled sub-tnumbers.
    // Datum vsize/vorigin: tfloat passes IEEE bits via doubleToLongBits; tint just .longValue().
    public static final UDF3<String, Double, Double, String[]>
        tfloatValueSplit = (trip, vsize, vorigin) -> tnumberValueSplitImpl(trip,
            Double.doubleToLongBits(vsize == null ? 0 : vsize),
            Double.doubleToLongBits(vorigin == null ? 0 : vorigin),
            vsize == null || vorigin == null);

    public static final UDF3<String, Integer, Integer, String[]>
        tintValueSplit = (trip, vsize, vorigin) -> tnumberValueSplitImpl(trip,
            vsize == null ? 0 : vsize.longValue(),
            vorigin == null ? 0 : vorigin.longValue(),
            vsize == null || vorigin == null);

    // tfloatValueTimeSplit / tintValueTimeSplit — Temporal** array of value+time-tiled sub-tnumbers
    public static final org.apache.spark.sql.api.java.UDF5<String, Double, String, Double, Timestamp, String[]>
        tfloatValueTimeSplit = (trip, vsize, intervalStr, vorigin, torigin) -> tnumberValueTimeSplitImpl(trip,
            Double.doubleToLongBits(vsize == null ? 0 : vsize), intervalStr,
            Double.doubleToLongBits(vorigin == null ? 0 : vorigin), torigin,
            vsize == null || intervalStr == null || vorigin == null || torigin == null);

    public static final org.apache.spark.sql.api.java.UDF5<String, Integer, String, Integer, Timestamp, String[]>
        tintValueTimeSplit = (trip, vsize, intervalStr, vorigin, torigin) -> tnumberValueTimeSplitImpl(trip,
            vsize == null ? 0 : vsize.longValue(), intervalStr,
            vorigin == null ? 0 : vorigin.longValue(), torigin,
            vsize == null || intervalStr == null || vorigin == null || torigin == null);

    private static String[] tnumberValueTimeSplitImpl(String trip, long vsize, String intervalStr,
            long vorigin, Timestamp torigin, boolean nullArgs) {
        if (trip == null || nullArgs) return null;
        MeosThread.ensureReady();
        Pointer t = functions.temporal_from_hexwkb(trip);
        if (t == null) return null;
        Pointer iv = functions.pg_interval_in(intervalStr, -1);
        if (iv == null) { MeosMemory.free(t); return null; }
        try {
            jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
            Pointer countOut = rt.getMemoryManager().allocateDirect(4);
            Pointer vBinsOut = rt.getMemoryManager().allocateDirect(8);
            Pointer tBinsOut = rt.getMemoryManager().allocateDirect(8);
            Pointer arr = MeosNative.INSTANCE.tnumber_value_time_split(t, vsize, iv, vorigin, pgEpoch(torigin),
                vBinsOut, tBinsOut, countOut);
            if (arr == null) return null;
            try {
                int n = countOut.getInt(0);
                String[] out = new String[n];
                for (int i = 0; i < n; i++) {
                    Pointer p = arr.getPointer(i * 8L);
                    if (p == null) continue;
                    out[i] = functions.temporal_as_hexwkb(p, (byte) 0);
                    MeosMemory.free(p);
                }
                return out;
            } finally {
                MeosMemory.free(arr);
                Pointer vb = vBinsOut.getPointer(0);
                Pointer tb = tBinsOut.getPointer(0);
                if (vb != null) MeosMemory.free(vb);
                if (tb != null) MeosMemory.free(tb);
            }
        } finally { MeosMemory.free(t, iv); }
    }

    private static String[] tnumberValueSplitImpl(String trip, long vsize, long vorigin, boolean nullArgs) {
        if (trip == null || nullArgs) return null;
        MeosThread.ensureReady();
        Pointer t = functions.temporal_from_hexwkb(trip);
        if (t == null) return null;
        try {
            jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
            Pointer countOut = rt.getMemoryManager().allocateDirect(4);
            Pointer binsOut  = rt.getMemoryManager().allocateDirect(8);
            Pointer arr = MeosNative.INSTANCE.tnumber_value_split(t, vsize, vorigin, binsOut, countOut);
            if (arr == null) return null;
            try {
                int n = countOut.getInt(0);
                String[] out = new String[n];
                for (int i = 0; i < n; i++) {
                    Pointer p = arr.getPointer(i * 8L);
                    if (p == null) continue;
                    out[i] = functions.temporal_as_hexwkb(p, (byte) 0);
                    MeosMemory.free(p);
                }
                return out;
            } finally {
                MeosMemory.free(arr);
                Pointer bins = binsOut.getPointer(0);
                if (bins != null) MeosMemory.free(bins);
            }
        } finally { MeosMemory.free(t); }
    }

    // ------------------------------------------------------------------
    // Single-tile lookups via tbox_get_value_time_tile
    //
    // MeosType enum values used: T_FLOAT8=11, T_FLOATSPAN=13,
    //                            T_INT4=15,   T_INTSPAN=19.
    // Datum vsize/vorigin: tfloat → IEEE bits via doubleToLongBits;
    //                      tint   → just longValue().
    // ------------------------------------------------------------------

    private static final int T_FLOAT8 = 11, T_FLOATSPAN = 13, T_INT4 = 15, T_INTSPAN = 19;

    // getValueTile(v float, vsize float, vorigin float) → tbox hex
    public static final UDF3<Double, Double, Double, String>
        getValueTileFloat = (v, vsize, vorigin) -> {
            if (v == null || vsize == null || vorigin == null) return null;
            MeosThread.ensureReady();
            Pointer r = MeosNative.INSTANCE.tbox_get_value_time_tile(
                Double.doubleToLongBits(v), 0L,
                Double.doubleToLongBits(vsize), null,
                Double.doubleToLongBits(vorigin), 0L,
                T_FLOAT8, T_FLOATSPAN);
            if (r == null) return null;
            try {
                Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                return functions.tbox_as_hexwkb(r, (byte) 0, sizeOut);
            } finally { MeosMemory.free(r); }
        };

    // getTBoxTimeTile(t timestamptz, duration interval, torigin timestamptz) → tbox hex
    public static final UDF3<Timestamp, String, Timestamp, String>
        getTBoxTimeTile = (t, intervalStr, torigin) -> {
            if (t == null || intervalStr == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) return null;
            try {
                Pointer r = MeosNative.INSTANCE.tbox_get_value_time_tile(
                    0L, pgEpoch(t),
                    0L, iv,
                    0L, pgEpoch(torigin),
                    T_FLOAT8, T_FLOATSPAN);
                if (r == null) return null;
                try {
                    Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    return functions.tbox_as_hexwkb(r, (byte) 0, sizeOut);
                } finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(iv); }
        };

    // getValueTimeTile(v float, t timestamptz, vsize float, duration interval,
    //                  vorigin float, torigin timestamptz) → tbox hex
    public static final org.apache.spark.sql.api.java.UDF6<Double, Timestamp, Double, String, Double, Timestamp, String>
        getValueTimeTileFloat = (v, t, vsize, intervalStr, vorigin, torigin) -> {
            if (v == null || t == null || vsize == null || intervalStr == null
                || vorigin == null || torigin == null) return null;
            MeosThread.ensureReady();
            Pointer iv = functions.pg_interval_in(intervalStr, -1);
            if (iv == null) return null;
            try {
                Pointer r = MeosNative.INSTANCE.tbox_get_value_time_tile(
                    Double.doubleToLongBits(v), pgEpoch(t),
                    Double.doubleToLongBits(vsize), iv,
                    Double.doubleToLongBits(vorigin), pgEpoch(torigin),
                    T_FLOAT8, T_FLOATSPAN);
                if (r == null) return null;
                try {
                    Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                    return functions.tbox_as_hexwkb(r, (byte) 0, sizeOut);
                } finally { MeosMemory.free(r); }
            } finally { MeosMemory.free(iv); }
        };

    // ------------------------------------------------------------------
    // Analytics — geoMeasure + asMVTGeom
    // ------------------------------------------------------------------

    // geoMeasure(tpoint, measure, segmentize) → geometry hex (geomeasure encoding)
    public static final org.apache.spark.sql.api.java.UDF3<String, String, Boolean, String>
        geoMeasure = (tpointHex, measureHex, segmentize) -> {
            if (tpointHex == null || measureHex == null) return null;
            MeosThread.ensureReady();
            Pointer tp = functions.temporal_from_hexwkb(tpointHex);
            if (tp == null) return null;
            Pointer m  = functions.temporal_from_hexwkb(measureHex);
            if (m == null) { MeosMemory.free(tp); return null; }
            try {
                Pointer geo = functions.tpoint_tfloat_to_geomeas(
                    tp, m, segmentize != null && segmentize);
                if (geo == null) return null;
                try { return functions.geo_as_hexewkb(geo, "NDR"); }
                finally { MeosMemory.free(geo); }
            } finally { MeosMemory.free(tp, m); }
        };

    // asMVTGeom(tpoint, bounds, extent, buffer, clip_geom) → array of WKT geometries
    // (the per-tile clipped tpoint trajectories)
    public static final org.apache.spark.sql.api.java.UDF5<String, String, Integer, Integer, Boolean, String[]>
        asMVTGeom = (tpointHex, boundsHex, extent, buffer, clipGeom) -> {
            if (tpointHex == null || boundsHex == null) return null;
            MeosThread.ensureReady();
            Pointer tp = functions.temporal_from_hexwkb(tpointHex);
            if (tp == null) return null;
            Pointer b = functions.stbox_from_hexwkb(boundsHex);
            if (b == null) { MeosMemory.free(tp); return null; }
            try {
                jnr.ffi.Runtime rt = Runtime.getSystemRuntime();
                Pointer gsArrOut    = rt.getMemoryManager().allocateDirect(8);
                Pointer timesArrOut = rt.getMemoryManager().allocateDirect(8);
                Pointer countOut    = rt.getMemoryManager().allocateDirect(4);
                boolean ok = functions.tpoint_as_mvtgeom(tp, b,
                    extent == null ? 4096 : extent, buffer == null ? 256 : buffer,
                    clipGeom == null || clipGeom, gsArrOut, timesArrOut, countOut);
                if (!ok) return null;
                Pointer gsArr = gsArrOut.getPointer(0);
                Pointer timesArr = timesArrOut.getPointer(0);
                if (gsArr == null) return null;
                int n = countOut.getInt(0);
                String[] out = new String[n];
                try {
                    for (int i = 0; i < n; i++) {
                        Pointer gs = gsArr.getPointer(i * 8L);
                        if (gs == null) continue;
                        out[i] = functions.geo_as_text(gs, 6);
                        // GSERIALIZED ownership: gsArr is allocated by MEOS, each
                        // entry is owned by the array; do not free entries individually.
                    }
                    return out;
                } finally {
                    MeosMemory.free(gsArr);
                    if (timesArr != null) MeosMemory.free(timesArr);
                }
            } finally { MeosMemory.free(tp, b); }
        };

    public static void registerAll(SparkSession spark) {
        // Single-tile lookups
        spark.udf().register("getTimeTile",      getTimeTile,      DataTypes.StringType);
        spark.udf().register("getSpaceTile",     getSpaceTile,     DataTypes.StringType);
        spark.udf().register("getSpaceTimeTile", getSpaceTimeTile, DataTypes.StringType);
        // getStboxTimeTile alias for getTimeTile (covers MobilityDB SQL bare name)
        spark.udf().register("getStboxTimeTile", getTimeTile,      DataTypes.StringType);
        // Multi-tile bounding boxes
        spark.udf().register("spaceBoxes",         spaceBoxes,           DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("spaceTimeBoxes",     spaceTimeBoxes,       DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("valueTimeBoxesTfloat", valueTimeBoxesTfloat, DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("valueTimeBoxesTint",   valueTimeBoxesTint,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("valueTimeBoxes",     valueTimeBoxesTfloat, DataTypes.createArrayType(DataTypes.StringType));
        // Splits (return arrays of sub-temporal values)
        spark.udf().register("timeSplit",      timeSplit,      DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("spaceSplit",     spaceSplit,     DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("spaceTimeSplit", spaceTimeSplit, DataTypes.createArrayType(DataTypes.StringType));
        // Bounded tile-set generators
        spark.udf().register("spaceTiles",       spaceTiles,         DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("spaceTimeTiles",   spaceTimeTiles,     DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("stboxTimeTiles",   stboxTimeTiles,     DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tintboxTimeTiles", tintboxTimeTiles,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tfloatboxTimeTiles", tfloatboxTimeTiles, DataTypes.createArrayType(DataTypes.StringType));
        // Bare-name aliases (timeTiles defaults to STBox; users with TBox use stboxTimeTiles vs tboxTimeTiles explicitly)
        spark.udf().register("timeTiles",        stboxTimeTiles,     DataTypes.createArrayType(DataTypes.StringType));
        // makeSimple + timeBoxes (typed dispatch via tnumber/tgeo)
        spark.udf().register("makeSimple",       makeSimple,         DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tfloatTimeBoxes",  tfloatTimeBoxes,    DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tintTimeBoxes",    tintTimeBoxes,      DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tgeoTimeBoxes",    tgeoTimeBoxes,      DataTypes.createArrayType(DataTypes.StringType));
        // timeBoxes default = tgeo (returns STBox[]); for TBox callers use tfloatTimeBoxes/tintTimeBoxes
        spark.udf().register("timeBoxes",        tgeoTimeBoxes,      DataTypes.createArrayType(DataTypes.StringType));
        // Value-only tile generators
        spark.udf().register("tfloatValueTiles", tfloatValueTiles,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tintValueTiles",   tintValueTiles,     DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("valueTiles",       tfloatValueTiles,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("valueBoxes",       tfloatValueTiles,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("valueTimeTiles",   valueTimeBoxesTfloat, DataTypes.createArrayType(DataTypes.StringType));
        // Value splits (typed Temporal** arrays)
        spark.udf().register("tfloatValueSplit", tfloatValueSplit,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tintValueSplit",   tintValueSplit,     DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("valueSplit",       tfloatValueSplit,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tfloatValueTimeSplit", tfloatValueTimeSplit, DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("tintValueTimeSplit",   tintValueTimeSplit,   DataTypes.createArrayType(DataTypes.StringType));
        spark.udf().register("valueTimeSplit",       tfloatValueTimeSplit, DataTypes.createArrayType(DataTypes.StringType));
        // Single-tile lookups (defaults to float; tint variants would call with T_INT4/T_INTSPAN)
        spark.udf().register("getValueTile",     getValueTileFloat,     DataTypes.StringType);
        spark.udf().register("getTBoxTimeTile",  getTBoxTimeTile,       DataTypes.StringType);
        spark.udf().register("getValueTimeTile", getValueTimeTileFloat, DataTypes.StringType);
        // Analytics
        spark.udf().register("geoMeasure",       geoMeasure,            DataTypes.StringType);
        spark.udf().register("asMVTGeom",        asMVTGeom,             DataTypes.createArrayType(DataTypes.StringType));
    }
}
