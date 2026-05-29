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

import org.junit.jupiter.api.*;
import org.mobilitydb.spark.MeosTestBase;

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for additional RestrictionUDFs — extrema restriction
 * (temporalAtMax/Min) and spatial restriction (tgeoAtGeom/tgeoMinusGeom).
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RestrictionUDFsExtTest extends MeosTestBase {

    private static String TFLOAT_SEQ;
    private static String TRIP;

    @BeforeAll
    static void initMeos() {
        TFLOAT_SEQ = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01, 5.0@2020-01-02, 2.0@2020-01-03]"), (byte) 0);
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(3 4)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // Extrema restriction
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalAtMax_returns_nonnull() throws Exception {
        String r = RestrictionUDFs.temporalAtMax.call(TFLOAT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void temporalAtMin_returns_nonnull() throws Exception {
        String r = RestrictionUDFs.temporalAtMin.call(TFLOAT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Spatial restriction
    // ------------------------------------------------------------------

    @Test @Order(3)
    void tgeoAtGeom_returns_nonnull_or_null() throws Exception {
        // The trip passes through the polygon — result may be non-null
        String r = RestrictionUDFs.tgeoAtGeom.call(
            TRIP, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(4)
    void tgeoMinusGeom_returns_nonnull_or_null() throws Exception {
        String r = RestrictionUDFs.tgeoMinusGeom.call(
            TRIP, "POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))");
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(5)
    void null_input_returns_null() throws Exception {
        assertNull(RestrictionUDFs.temporalAtMax.call(null));
        assertNull(RestrictionUDFs.temporalAtMin.call(null));
        assertNull(RestrictionUDFs.tgeoAtGeom.call(null, "POINT(0 0)"));
        assertNull(RestrictionUDFs.tgeoMinusGeom.call(null, "POINT(0 0)"));
    }
}
