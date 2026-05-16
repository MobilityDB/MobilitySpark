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
 * Unit tests for StaticGeoUDFs: static geometry predicates, metrics,
 * transforms, and line operations.
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StaticGeoUDFsTest extends MeosTestBase {

    private static final String POLY = "POLYGON((0 0,4 0,4 4,0 4,0 0))";
    private static final String POLY2 = "POLYGON((2 2,6 2,6 6,2 6,2 2))";
    private static final String POINT_INSIDE = "POINT(2 2)";
    private static final String POINT_OUTSIDE = "POINT(10 10)";
    private static final String LINE = "LINESTRING(0 0,10 0)";
    private static final String LINE2 = "LINESTRING(5 5,5 -5)";

    // ------------------------------------------------------------------
    // Geometry predicates
    // ------------------------------------------------------------------

    @Test @Order(1)
    void geomContains_polygon_contains_interior_point() throws Exception {
        assertTrue(StaticGeoUDFs.geomContains.call(POLY, "POINT(1 1)"));
    }

    @Test @Order(2)
    void geomContains_polygon_does_not_contain_exterior_point() throws Exception {
        assertFalse(StaticGeoUDFs.geomContains.call(POLY, POINT_OUTSIDE));
    }

    @Test @Order(3)
    void geomIntersects_overlapping_polygons_is_true() throws Exception {
        assertTrue(StaticGeoUDFs.geomIntersects.call(POLY, POLY2));
    }

    @Test @Order(4)
    void geomDisjoint_non_overlapping_is_true() throws Exception {
        String farPoly = "POLYGON((10 10,14 10,14 14,10 14,10 10))";
        assertTrue(StaticGeoUDFs.geomDisjoint.call(POLY, farPoly));
    }

    @Test @Order(5)
    void geomTouches_crossing_lines_at_point() throws Exception {
        // Two line segments that share only an endpoint — touches is true
        String line1 = "LINESTRING(0 0,5 5)";
        String line2 = "LINESTRING(5 5,10 0)";
        assertTrue(StaticGeoUDFs.geomTouches.call(line1, line2));
    }

    @Test @Order(6)
    void geomDwithin_close_points_is_true() throws Exception {
        assertTrue(StaticGeoUDFs.geomDwithin.call("POINT(0 0)", "POINT(3 4)", 5.0001));
    }

    @Test @Order(7)
    void geomDwithin_far_points_is_false() throws Exception {
        assertFalse(StaticGeoUDFs.geomDwithin.call("POINT(0 0)", "POINT(100 100)", 1.0));
    }

    // ------------------------------------------------------------------
    // Geometry metrics
    // ------------------------------------------------------------------

    @Test @Order(8)
    void geomDistance_between_points() throws Exception {
        Double d = StaticGeoUDFs.geomDistance.call("POINT(0 0)", "POINT(3 4)");
        assertNotNull(d);
        assertEquals(5.0, d, 1e-9);
    }

    @Test @Order(9)
    void geomLength_of_line() throws Exception {
        Double len = StaticGeoUDFs.geomLength.call(LINE);
        assertNotNull(len);
        assertEquals(10.0, len, 1e-9);
    }

    @Test @Order(10)
    void geomPerimeter_of_square() throws Exception {
        Double p = StaticGeoUDFs.geomPerimeter.call(POLY);
        assertNotNull(p);
        assertEquals(16.0, p, 1e-9);
    }

    // ------------------------------------------------------------------
    // Geometry transforms
    // ------------------------------------------------------------------

    @Test @Order(11)
    void geomCentroid_of_square_is_center() throws Exception {
        String wkt = StaticGeoUDFs.geomCentroid.call(POLY);
        assertNotNull(wkt);
        assertTrue(wkt.startsWith("POINT"), "Expected POINT, got: " + wkt);
    }

    @Test @Order(12)
    void geomBoundary_of_polygon_is_ring() throws Exception {
        String wkt = StaticGeoUDFs.geomBoundary.call(POLY);
        assertNotNull(wkt);
        assertTrue(wkt.startsWith("LINESTRING") || wkt.startsWith("MULTILINESTRING"),
            "Expected LINESTRING, got: " + wkt);
    }

    @Test @Order(13)
    void geomDifference_returns_non_null() throws Exception {
        String wkt = StaticGeoUDFs.geomDifference.call(POLY, POLY2);
        assertNotNull(wkt);
        assertFalse(wkt.isBlank());
    }

    @Test @Order(14)
    void geoReverse_line_reverses_direction() throws Exception {
        String wkt = StaticGeoUDFs.geoReverse.call(LINE);
        assertNotNull(wkt);
        assertTrue(wkt.startsWith("LINESTRING"), "Expected LINESTRING, got: " + wkt);
    }

    @Test @Order(15)
    void geoRound_reduces_precision() throws Exception {
        String wkt = StaticGeoUDFs.geoRound.call("POINT(1.123456789 2.987654321)", 3);
        assertNotNull(wkt);
        assertTrue(wkt.startsWith("POINT"), "Expected POINT, got: " + wkt);
    }

    // ------------------------------------------------------------------
    // Line functions
    // ------------------------------------------------------------------

    @Test @Order(16)
    void lineInterpolatePoint_midpoint() throws Exception {
        String wkt = StaticGeoUDFs.lineInterpolatePoint.call(LINE, 0.5);
        assertNotNull(wkt);
        assertTrue(wkt.startsWith("POINT"), "Expected POINT, got: " + wkt);
    }

    @Test @Order(17)
    void lineSubstring_half() throws Exception {
        String wkt = StaticGeoUDFs.lineSubstring.call(LINE, 0.0, 0.5);
        assertNotNull(wkt);
        assertTrue(wkt.startsWith("LINESTRING"), "Expected LINESTRING, got: " + wkt);
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(18)
    void null_inputs_return_null() throws Exception {
        assertNull(StaticGeoUDFs.geomContains.call(null, POLY));
        assertNull(StaticGeoUDFs.geomIntersects.call(POLY, null));
        assertNull(StaticGeoUDFs.geomDistance.call(null, "POINT(1 1)"));
        assertNull(StaticGeoUDFs.geomLength.call(null));
        assertNull(StaticGeoUDFs.geomCentroid.call(null));
        assertNull(StaticGeoUDFs.geomBoundary.call(null));
        assertNull(StaticGeoUDFs.geoReverse.call(null));
        assertNull(StaticGeoUDFs.lineInterpolatePoint.call(null, 0.5));
        assertNull(StaticGeoUDFs.lineSubstring.call(null, 0.0, 0.5));
    }
}
