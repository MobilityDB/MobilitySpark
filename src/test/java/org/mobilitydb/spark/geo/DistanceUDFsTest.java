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

import org.junit.jupiter.api.*;
import org.mobilitydb.spark.MeosTestBase;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DistanceUDFs — temporal distance between tgeo/tnumber and
 * fixed or temporal counterparts.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DistanceUDFsTest extends MeosTestBase {

    private static String TRIP;
    private static String TRIP2;
    private static String TFLOAT_SEQ;
    private static String TINT_SEQ;

    @BeforeAll
    static void initMeos() {
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(3 4)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TRIP2 = temporal_as_hexwkb(
            tgeompoint_in("[POINT(1 1)@2020-01-01 00:00:00+00, POINT(4 5)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01, 5.0@2020-01-03]"), (byte) 0);
        TINT_SEQ = temporal_as_hexwkb(
            tint_in("[2@2020-01-01, 6@2020-01-03]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // Spatial distance
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tdistanceTgeoGeo_returns_tfloat() throws Exception {
        String r = DistanceUDFs.tdistanceTgeoGeo.call(TRIP, "POINT(0 0)");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tdistanceTgeoTgeo_returns_tfloat() throws Exception {
        String r = DistanceUDFs.tdistanceTgeoTgeo.call(TRIP, TRIP2);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Number distance
    // ------------------------------------------------------------------

    @Test @Order(3)
    void tdistanceTfloatFloat_returns_tfloat() throws Exception {
        String r = DistanceUDFs.tdistanceTfloatFloat.call(TFLOAT_SEQ, 3.0);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void tdistanceTintInt_returns_tint() throws Exception {
        String r = DistanceUDFs.tdistanceTintInt.call(TINT_SEQ, 4);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(5)
    void tdistanceTnumberTnumber_returns_tfloat() throws Exception {
        String r = DistanceUDFs.tdistanceTnumberTnumber.call(TFLOAT_SEQ, TFLOAT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(6)
    void null_input_returns_null() throws Exception {
        assertNull(DistanceUDFs.tdistanceTgeoGeo.call(null, "POINT(0 0)"));
        assertNull(DistanceUDFs.tdistanceTgeoTgeo.call(null, TRIP2));
        assertNull(DistanceUDFs.tdistanceTfloatFloat.call(null, 1.0));
    }
}
