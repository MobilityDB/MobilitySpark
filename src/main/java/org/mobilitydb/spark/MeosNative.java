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

package org.mobilitydb.spark;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;

/**
 * Supplementary JNR-FFI interface for libmeos symbols not yet declared in
 * JMEOS-1.4.  JMEOS-1.4 was generated from an older API snapshot and still
 * uses {@code _tpoint_} naming for functions that MEOS 1.4 has renamed to
 * {@code _tspatial_} or {@code _tgeo_}.
 *
 * Loading the same "meos" library twice is safe: JNR-FFI caches native
 * library handles by name so the OS shared-library is opened only once.
 */
public final class MeosNative {

    private MeosNative() {}

    public interface Lib {

        // ----------------------------------------------------------------
        // Nearest approach distance (NAD) — returns double, DBL_MAX on fail
        // ----------------------------------------------------------------

        double nad_tgeo_geo(Pointer temporal, Pointer geo);
        double nad_tgeo_stbox(Pointer temporal, Pointer stbox);
        double nad_tgeo_tgeo(Pointer temporal1, Pointer temporal2);

        // ----------------------------------------------------------------
        // Nearest approach instant (NAI) — returns TInstant *
        // ----------------------------------------------------------------

        Pointer nai_tgeo_geo(Pointer temporal, Pointer geo);
        Pointer nai_tgeo_tgeo(Pointer temporal1, Pointer temporal2);

        // ----------------------------------------------------------------
        // Shortest line — returns GSERIALIZED *
        // ----------------------------------------------------------------

        Pointer shortestline_tgeo_geo(Pointer temporal, Pointer geo);

        // ----------------------------------------------------------------
        // Scalar value-to-bin (renamed from float_bucket / int_bucket)
        // ----------------------------------------------------------------

        double float_get_bin(double value, double size, double origin);
        int    int_get_bin(int value, int size, int origin);

        // ----------------------------------------------------------------
        // TBox expand (renamed from tbox_expand_float / tbox_expand_int)
        // ----------------------------------------------------------------

        Pointer tfloatbox_expand(Pointer tbox, double value);
        Pointer tintbox_expand(Pointer tbox, int value);

        // ----------------------------------------------------------------
        // tgeometry / tgeography MFJSON constructors (not in JMEOS-1.4)
        // ----------------------------------------------------------------

        Pointer tgeometry_from_mfjson(String mfjson);
        Pointer tgeography_from_mfjson(String mfjson);

        // ----------------------------------------------------------------
        // tgeometry / tgeography text constructors (not in JMEOS-1.4)
        // ----------------------------------------------------------------

        Pointer tgeometry_in(String wkt);
        Pointer tgeography_in(String wkt);

        // ----------------------------------------------------------------
        // Temporal accessor (not in JMEOS-1.4)
        // ----------------------------------------------------------------

        int temporal_mem_size(Pointer temporal);
        Pointer tgeompoint_to_tgeometry(Pointer p);
        Pointer tgeogpoint_to_tgeography(Pointer p);
        Pointer tgeometry_to_tgeompoint(Pointer p);
        Pointer tgeography_to_tgeogpoint(Pointer p);
        Pointer tgeometry_to_tgeography(Pointer p);
        Pointer tgeography_to_tgeometry(Pointer p);

        // Time-restriction (TimestampTz = int64 microseconds since J2000)
        Pointer temporal_before_timestamptz(Pointer temporal, long pgEpochMicros);
        Pointer temporal_after_timestamptz(Pointer temporal, long pgEpochMicros);

        // ttext concatenation (not in JMEOS-1.4)
        Pointer textcat_ttext_text(Pointer ttext, Pointer text);
        Pointer textcat_text_ttext(Pointer text, Pointer ttext);
        Pointer textcat_ttext_ttext(Pointer ttext1, Pointer ttext2);

        // MobilityDB extension introspection
        String mobilitydb_version();
        String mobilitydb_full_version();

        // Typed set element accessors (return bool, fill out-param)
        boolean intset_value_n(Pointer set, int n, Pointer result);
        boolean bigintset_value_n(Pointer set, int n, Pointer result);
        boolean floatset_value_n(Pointer set, int n, Pointer result);

        // Aggregate-as-scalar
        double tnumber_avg_value(Pointer temporal);

        // tgeometry/tgeography instant constructor (geometry-typed, timestamp via long)
        Pointer tgeoinst_make(Pointer geo, long pgEpochMicros);

        // Array-returning bbox accessors (count via out-param, returned
        // pointer is contiguous TBox[]/STBox[] respectively)
        Pointer tnumber_tboxes(Pointer temporal, Pointer count);
        Pointer tgeo_stboxes(Pointer temporal, Pointer count);

        // Similarity paths (Match[] = pairs of {int i, int j}, 8 bytes each)
        Pointer temporal_dyntimewarp_path(Pointer p1, Pointer p2, Pointer count);
        Pointer temporal_frechet_path(Pointer p1, Pointer p2, Pointer count);

        // Affine transformation (AFFINE = 12 doubles, 96 bytes)
        Pointer tgeo_affine(Pointer temporal, Pointer affine);

        // Span tiling — returns Span[] with count via out-param
        Pointer intspan_bins(Pointer span, int vsize, int vorigin, Pointer count);
        Pointer bigintspan_bins(Pointer span, long vsize, long vorigin, Pointer count);
        Pointer floatspan_bins(Pointer span, double vsize, double vorigin, Pointer count);

        // Span expand (returns expanded Span*)
        Pointer intspan_expand(Pointer span, int value);
        Pointer bigintspan_expand(Pointer span, long value);
        Pointer floatspan_expand(Pointer span, double value);

        // tpoint minus geometry, direction (instantaneous bearing in radians)
        Pointer tpoint_minus_geom(Pointer temporal, Pointer geo);
        boolean tpoint_direction(Pointer temporal, Pointer result);

        // Time-tiling: Span[] of consecutive time bins
        Pointer temporal_time_bins(Pointer temporal, Pointer interval, long origin, Pointer count);
        Pointer tstzspan_bins(Pointer span, Pointer interval, long origin, Pointer count);

        // Value-tiling: Span[] of consecutive value bins for tnumber
        Pointer tint_value_bins(Pointer temporal, int vsize, int vorigin, Pointer count);
        Pointer tfloat_value_bins(Pointer temporal, double vsize, double vorigin, Pointer count);

        // STBox quad-split: returns STBox[] of 4 quadrants (or 8 if Z, 16 if T)
        Pointer stbox_quad_split(Pointer stbox, Pointer count);

        // Scalar timestamptz_get_bin: TimestampTz value, Interval, TimestampTz origin → TimestampTz
        long timestamptz_get_bin(long ts, Pointer interval, long origin);

        // Single-point space tile: STBox of the cell containing point
        Pointer stbox_get_space_tile(Pointer point, double xsize, double ysize, double zsize, Pointer sorigin);
        Pointer stbox_get_time_tile(long t, Pointer duration, long torigin);
        Pointer stbox_get_space_time_tile(Pointer point, long t, double xsize, double ysize, double zsize, Pointer duration, Pointer sorigin, long torigin);

        // Space + space-time bounding boxes (multi-tile, contiguous STBox[])
        Pointer tgeo_space_boxes(Pointer temporal, double xsize, double ysize, double zsize, Pointer sorigin, boolean bitmatrix, boolean border_inc, Pointer count);
        Pointer tgeo_space_time_boxes(Pointer temporal, double xsize, double ysize, double zsize, Pointer duration, Pointer sorigin, long torigin, boolean bitmatrix, boolean border_inc, Pointer count);

        // tnumber value-time boxes (Datum vsize/vorigin passed as long)
        Pointer tnumber_value_time_boxes(Pointer temporal, long vsize, Pointer duration, long vorigin, long torigin, Pointer count);

        // Splits — return Temporal** (array of pointers) + various out-bin arrays
        Pointer temporal_time_split(Pointer temporal, Pointer duration, long torigin, Pointer time_bins_out, Pointer count);
        Pointer tgeo_space_split(Pointer temporal, double xsize, double ysize, double zsize, Pointer sorigin, boolean bitmatrix, boolean border_inc, Pointer space_bins_out, Pointer count);
        Pointer tgeo_space_time_split(Pointer temporal, double xsize, double ysize, double zsize, Pointer duration, Pointer sorigin, long torigin, boolean bitmatrix, boolean border_inc, Pointer space_bins_out, Pointer time_bins_out, Pointer count);

        // valueSet support: temporal_values_p returns Datum*, set_make_free
        // packs them into a typed Set; temptype_basetype maps temporal type
        // (T_TINT/T_TFLOAT/etc.) to its base value type (T_INT4/T_FLOAT8/etc.).
        Pointer temporal_values_p(Pointer temporal, Pointer count);
        int     temptype_basetype(int temptype);
        Pointer set_make_free(Pointer values, int count, int basetype, boolean order);

        // segmentMin/MaxDuration — temporal_segm_duration with atleast flag
        Pointer temporal_segm_duration(Pointer temporal, Pointer duration, boolean atleast, boolean strict);

        // STBox → BOX3D / GBOX + text serialization (PostGIS embedded in MEOS)
        Pointer stbox_to_box3d(Pointer stbox);
        String  box3d_out(Pointer box3d, int maxdd);
        Pointer stbox_to_gbox(Pointer stbox);
        String  gbox_out(Pointer gbox, int maxdd);

        // Tile-set generators (return arrays of bounding boxes)
        Pointer stbox_space_tiles(Pointer bounds, double xsize, double ysize, double zsize, Pointer sorigin, boolean border_inc, Pointer count);
        Pointer stbox_time_tiles(Pointer bounds, Pointer duration, long torigin, boolean border_inc, Pointer count);
        Pointer stbox_space_time_tiles(Pointer bounds, double xsize, double ysize, double zsize, Pointer duration, Pointer sorigin, long torigin, boolean border_inc, Pointer count);
        Pointer tintbox_time_tiles(Pointer box, Pointer duration, long torigin, Pointer count);
        Pointer tfloatbox_time_tiles(Pointer box, Pointer duration, long torigin, Pointer count);
        Pointer tintbox_value_time_tiles(Pointer box, int xsize, Pointer duration, int xorigin, long torigin, Pointer count);
        Pointer tfloatbox_value_time_tiles(Pointer box, double vsize, Pointer duration, double vorigin, long torigin, Pointer count);

        // tpoint → array of simple sub-tpoints (no self-intersections)
        Pointer tpoint_make_simple(Pointer temporal, Pointer count);

        // Type-converters for timeBoxes intermediate step
        Pointer tnumber_to_tbox(Pointer temporal);

        // Value-only tile generators (TBox[])
        Pointer tintbox_value_tiles(Pointer box, int xsize, int xorigin, Pointer count);
        Pointer tfloatbox_value_tiles(Pointer box, double vsize, double vorigin, Pointer count);

        // Value/value-time splits returning Temporal** + bin out-arrays (Datum vsize/vorigin)
        Pointer tnumber_value_split(Pointer temporal, long vsize, long vorigin, Pointer bins_out, Pointer count);
        Pointer tnumber_value_time_split(Pointer temporal, long size, Pointer duration, long vorigin, long torigin, Pointer value_bins_out, Pointer time_bins_out, Pointer count);

        // Single-tile lookup: takes Datum value/origin + MeosType basetype/spantype
        Pointer tbox_get_value_time_tile(long value, long t, long vsize, Pointer duration, long vorigin, long torigin, int basetype, int spantype);

        // tpoint analytics
        boolean tpoint_tfloat_to_geomeas(Pointer tpoint, Pointer measure, boolean segmentize, Pointer result_out);
        boolean tpoint_as_mvtgeom(Pointer temporal, Pointer bounds, int extent, int buffer, boolean clip_geom, Pointer gsarr_out, Pointer timesarr_out, Pointer count_out);

        // Split-by-N functions (count via out-param, returned pointer is contiguous array)
        Pointer temporal_split_n_spans(Pointer temporal, int n, Pointer count);
        Pointer temporal_split_each_n_spans(Pointer temporal, int n, Pointer count);
        Pointer tnumber_split_n_tboxes(Pointer temporal, int n, Pointer count);
        Pointer tnumber_split_each_n_tboxes(Pointer temporal, int n, Pointer count);
        Pointer tgeo_split_n_stboxes(Pointer temporal, int n, Pointer count);
        Pointer tgeo_split_each_n_stboxes(Pointer temporal, int n, Pointer count);

        // ----------------------------------------------------------------
        // Cross-type: STBox × TSpatial (spatial direction)
        // ----------------------------------------------------------------

        boolean left_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean right_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean overleft_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean overright_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean above_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean below_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean overabove_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean overbelow_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean front_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean back_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean overfront_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean overback_stbox_tspatial(Pointer stbox, Pointer tspatial);

        // ----------------------------------------------------------------
        // Cross-type: STBox × TSpatial (temporal direction)
        // ----------------------------------------------------------------

        boolean before_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean after_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean overbefore_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean overafter_stbox_tspatial(Pointer stbox, Pointer tspatial);

        // ----------------------------------------------------------------
        // Cross-type: STBox × TSpatial (topological)
        // ----------------------------------------------------------------

        boolean adjacent_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean contains_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean contained_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean overlaps_stbox_tspatial(Pointer stbox, Pointer tspatial);
        boolean same_stbox_tspatial(Pointer stbox, Pointer tspatial);

        // ----------------------------------------------------------------
        // Cross-type: TSpatial × STBox (spatial direction)
        // ----------------------------------------------------------------

        boolean left_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean right_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean overleft_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean overright_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean above_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean below_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean overabove_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean overbelow_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean front_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean back_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean overfront_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean overback_tspatial_stbox(Pointer tspatial, Pointer stbox);

        // ----------------------------------------------------------------
        // Cross-type: TSpatial × STBox (temporal direction)
        // ----------------------------------------------------------------

        boolean before_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean after_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean overbefore_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean overafter_tspatial_stbox(Pointer tspatial, Pointer stbox);

        // ----------------------------------------------------------------
        // Cross-type: TSpatial × STBox (topological)
        // ----------------------------------------------------------------

        boolean adjacent_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean contains_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean contained_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean overlaps_tspatial_stbox(Pointer tspatial, Pointer stbox);
        boolean same_tspatial_stbox(Pointer tspatial, Pointer stbox);
    }

    public static final Lib INSTANCE;
    static {
        LibraryLoader<Lib> loader = LibraryLoader.create(Lib.class);
        String libPath = System.getProperty("java.library.path");
        if (libPath != null) {
            for (String p : libPath.split(":")) {
                if (!p.isEmpty()) loader.search(p);
            }
        }
        INSTANCE = loader.load("meos");
    }
}
