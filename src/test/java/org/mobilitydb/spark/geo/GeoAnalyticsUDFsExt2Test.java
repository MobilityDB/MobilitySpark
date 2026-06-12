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

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GeoAnalyticsUDFs.geoSame.
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GeoAnalyticsUDFsExt2Test {

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");
    }

    @Test @Order(1)
    void geoSame_identical_points_returns_true() throws Exception {
        Boolean r = GeoAnalyticsUDFs.geoSame.call("POINT(1 2)", "POINT(1 2)");
        assertNotNull(r, "geoSame must return non-null for identical points");
        assertTrue(r, "identical geometries must be the same");
    }

    @Test @Order(2)
    void geoSame_different_points_returns_false() throws Exception {
        Boolean r = GeoAnalyticsUDFs.geoSame.call("POINT(1 2)", "POINT(3 4)");
        assertNotNull(r);
        assertFalse(r, "different geometries must not be the same");
    }

    @Test @Order(3)
    void geoSame_identical_polygons_returns_true() throws Exception {
        String poly = "POLYGON((0 0,1 0,1 1,0 1,0 0))";
        Boolean r = GeoAnalyticsUDFs.geoSame.call(poly, poly);
        assertNotNull(r);
        assertTrue(r, "identical polygons must be the same");
    }

    @Test @Order(4)
    void geoSame_null_first_returns_null() throws Exception {
        assertNull(GeoAnalyticsUDFs.geoSame.call(null, "POINT(1 2)"));
    }

    @Test @Order(5)
    void geoSame_null_second_returns_null() throws Exception {
        assertNull(GeoAnalyticsUDFs.geoSame.call("POINT(1 2)", null));
    }
}
