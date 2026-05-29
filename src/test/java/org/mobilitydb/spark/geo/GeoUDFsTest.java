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

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GeoUDFs — spatial relations and distance on tgeompoint.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GeoUDFsTest extends MeosTestBase {

    private static String TRIP_HEX;
    private static String TRIP2_HEX;
    // Geometry passed as WKT text — eIntersects parses via geo_from_text internally
    private static final String POINT_ON_PATH = "POINT(0.05 0.0)";
    private static final String POINT_FAR     = "POINT(10.0 10.0)";

    @BeforeAll
    static void initMeos() {
        TRIP_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, POINT(0.1 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TRIP2_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.05 0.001)@2020-01-01 00:00:00+00, POINT(0.15 0.001)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    @AfterAll
    static void finalizeMeos() {
        meos_finalize();
    }

    @Test @Order(1)
    void eIntersects_point_on_path_returns_true() throws Exception {
        assertTrue(GeoUDFs.eIntersects.call(TRIP_HEX, POINT_ON_PATH));
    }

    @Test @Order(2)
    void eIntersects_far_point_returns_false() throws Exception {
        assertFalse(GeoUDFs.eIntersects.call(TRIP_HEX, POINT_FAR));
    }

    @Test @Order(3)
    void eIntersects_null_trip_returns_null() throws Exception {
        assertNull(GeoUDFs.eIntersects.call(null, POINT_ON_PATH));
    }

    @Test @Order(4)
    void nearestApproachDistance_same_trip_is_zero() throws Exception {
        Double d = GeoUDFs.nearestApproachDistance.call(TRIP_HEX, TRIP_HEX);
        assertNotNull(d);
        assertEquals(0.0, d, 1e-9);
    }

    @Test @Order(5)
    void nearestApproachDistance_parallel_trips() throws Exception {
        Double d = GeoUDFs.nearestApproachDistance.call(TRIP_HEX, TRIP2_HEX);
        assertNotNull(d);
        assertTrue(d > 0.0 && d < 0.1, "Expected positive distance < 0.1, got " + d);
    }

    @Test @Order(6)
    void nearestApproachDistance_null_returns_null() throws Exception {
        assertNull(GeoUDFs.nearestApproachDistance.call(null, TRIP_HEX));
    }

    @Test @Order(7)
    void eDwithin_within_large_distance() throws Exception {
        assertTrue(GeoUDFs.eDwithin.call(TRIP_HEX, TRIP2_HEX, 1.0));
    }

    @Test @Order(8)
    void eDwithin_outside_tiny_distance() throws Exception {
        assertFalse(GeoUDFs.eDwithin.call(TRIP_HEX, TRIP2_HEX, 1e-10));
    }

    @Test @Order(9)
    void eDwithin_null_returns_null() throws Exception {
        assertNull(GeoUDFs.eDwithin.call(null, TRIP_HEX, 1.0));
    }
}
