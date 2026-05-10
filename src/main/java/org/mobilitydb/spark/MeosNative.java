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
 * Supplementary JNR-FFI interface for libmeos symbols not declared in any
 * MEOS public header.  These all live in MEOS private headers
 * (meos_internal.h, meos_internal_geo.h, temporal/temporal.h,
 * temporal/meos_catalog.h) and use Datum / MeosType parameters that the
 * JMEOS generator does not currently lower.  The symbols are exported by
 * libmeos.so so JNR-FFI can bind them directly.
 *
 * Loading the same "meos" library twice is safe: JNR-FFI caches native
 * library handles by name so the OS shared-library is opened only once.
 */
public final class MeosNative {

    private MeosNative() {}

    public interface Lib {

        // mobilitydb_version / mobilitydb_full_version live in
        // include/temporal/temporal.h
        String mobilitydb_version();
        String mobilitydb_full_version();

        // temporal_mem_size is in include/temporal/temporal.h
        int temporal_mem_size(Pointer temporal);

        // valueSet support — all three symbols live in private headers and
        // use Datum / MeosType parameters:
        //   temporal_values_p     -> meos_internal.h        (Datum*)
        //   set_make_free         -> meos_internal.h        (Datum*, MeosType)
        //   temptype_basetype     -> temporal/meos_catalog.h (MeosType)
        Pointer temporal_values_p(Pointer temporal, Pointer count);
        Pointer set_make_free(Pointer values, int count, int basetype, boolean ordered);
        int     temptype_basetype(int temptype);

        // Multidimensional value-time tile / split — meos_internal.h
        // (Datum-typed value parameters; not lowered by JMEOS generator).
        Pointer tnumber_value_split(Pointer temporal, long vsize, long vorigin,
                                    Pointer bins_out, Pointer count);
        Pointer tnumber_value_time_split(Pointer temporal, long size, Pointer duration,
                                         long vorigin, long torigin,
                                         Pointer value_bins_out, Pointer time_bins_out,
                                         Pointer count);
        Pointer tnumber_value_time_boxes(Pointer temporal, long vsize, Pointer duration,
                                         long vorigin, long torigin, Pointer count);
        Pointer tbox_get_value_time_tile(long value, long t, long vsize, Pointer duration,
                                         long vorigin, long torigin,
                                         int basetype, int spantype);
    }

    public static final Lib INSTANCE = LibraryLoader.create(Lib.class).load("meos");
}
