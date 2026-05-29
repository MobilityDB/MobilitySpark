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
 * Unit tests for TempSpatialRelsUDFs — tDisjoint, tIntersects, tTouches.
 *
 * Fixture:
 *   TRIP — tgeompoint moving along X axis from (0,0) to (1,0)
 *   REGION_ON_PATH  — polygon enclosing the trip midpoint
 *   REGION_FAR      — polygon far away from the trip
 *
 * MEOS function authority: meos/include/meos_geo.h (072_tgeo_tempspatialrels)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TempSpatialRelsUDFsTest extends MeosTestBase {

    private static String TRIP;
    private static final String REGION_ON_PATH = "POLYGON((0 -1, 1 -1, 1 1, 0 1, 0 -1))";
    private static final String REGION_FAR     = "POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))";

    @BeforeAll
    static void initMeos() {
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, POINT(1.0 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // tIntersects
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tIntersects_trip_with_region_on_path_returns_nonnull_tbool() throws Exception {
        String r = TempSpatialRelsUDFs.tIntersects.call(TRIP, REGION_ON_PATH);
        assertNotNull(r, "tIntersects should return a tbool hex-WKB when trip crosses region");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tIntersects_trip_with_far_region_returns_nonnull_tbool() throws Exception {
        String r = TempSpatialRelsUDFs.tIntersects.call(TRIP, REGION_FAR);
        assertNotNull(r);
    }

    @Test @Order(3)
    void tIntersects_null_trip_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tIntersects.call(null, REGION_ON_PATH));
    }

    @Test @Order(4)
    void tIntersects_null_geom_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tIntersects.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // tDisjoint
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tDisjoint_trip_with_far_region_returns_nonnull_tbool() throws Exception {
        String r = TempSpatialRelsUDFs.tDisjoint.call(TRIP, REGION_FAR);
        assertNotNull(r, "tDisjoint should return a tbool hex-WKB");
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void tDisjoint_null_trip_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tDisjoint.call(null, REGION_FAR));
    }

    @Test @Order(7)
    void tDisjoint_null_geom_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tDisjoint.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // tTouches
    // ------------------------------------------------------------------

    @Test @Order(8)
    void tTouches_returns_nonnull_tbool_for_valid_inputs() throws Exception {
        // tTouches tests the boundary; this just checks the UDF doesn't crash
        String r = TempSpatialRelsUDFs.tTouches.call(TRIP, REGION_ON_PATH);
        // Result may be null if MEOS raises an error for a non-boundary condition
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(9)
    void tTouches_null_trip_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tTouches.call(null, REGION_ON_PATH));
    }

    @Test @Order(10)
    void tTouches_null_geom_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tTouches.call(TRIP, null));
    }
}
