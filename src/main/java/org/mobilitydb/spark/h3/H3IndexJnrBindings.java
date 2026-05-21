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

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;

/**
 * Direct JNR-FFI bindings for the MEOS H3 prefilter surface used by
 * the BerlinMOD th3index variant queries.
 *
 * The mainline JMEOS jar shipped with MobilitySpark does not include
 * the H3 family because the JMEOS function generator does not yet
 * support the H3Index typedef.  Until JMEOS regenerates against the
 * H3-aware MEOS headers, these bindings expose the four MEOS symbols
 * the bench harness actually needs:
 *
 *   tgeompoint_to_th3index  — TH3 from a tgeompoint trajectory + H3 resolution.
 *   geo_to_h3index_set      — Set<H3Index> from a static geometry (EPSG:4326)
 *                             at a given H3 resolution.
 *   ever_eq_th3index_th3index
 *                           — trip x trip prefilter: do two trip H3 cell
 *                             sequences ever share a cell at the same instant.
 *   ever_eq_anyof_h3indexset_th3index
 *                           — trip x static prefilter: does the trip H3
 *                             sequence ever pass through any cell of a static
 *                             H3 set.
 */
public final class H3IndexJnrBindings {

    private H3IndexJnrBindings() {}

    /** JNR view of the four MEOS H3 symbols.  All return MEOS pointers or int. */
    public interface LibMeosH3 {
        Pointer tgeompoint_to_th3index(Pointer temp, int resolution);
        Pointer geo_to_h3index_set(Pointer gs, int resolution);
        int     ever_eq_th3index_th3index(Pointer t1, Pointer t2);
        int     ever_eq_anyof_h3indexset_th3index(Pointer set, Pointer t);
    }

    public static final LibMeosH3 LIB =
        LibraryLoader.create(LibMeosH3.class).load("meos");
}
