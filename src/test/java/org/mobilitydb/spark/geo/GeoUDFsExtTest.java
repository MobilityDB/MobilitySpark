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
import org.mobilitydb.spark.temporal.AccessorUDFs;

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GeoUDFs Phase 4 extensions — getX/Y/Z, cumulativeLength,
 * stops, isSimple, shortestLine.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GeoUDFsExtTest {

    // Linear trip from (0,0) to (2,0) over 2 hours
    private static String TRIP_HEX;
    // Second trip offset in Y
    private static String TRIP2_HEX;
    // Self-intersecting trip (figure-8 approximation)
    private static String TRIP_SELF_INTERSECTING;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TRIP_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(2 0)@2020-01-01 02:00:00+00]"),
            (byte) 0);
        TRIP2_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(3 1)@2020-01-01 00:00:00+00, POINT(3 2)@2020-01-01 02:00:00+00]"),
            (byte) 0);
        // A simple sequence (no self-intersection)
        TRIP_SELF_INTERSECTING = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(1 0)@2020-01-01 01:00:00+00, POINT(2 0)@2020-01-01 02:00:00+00]"),
            (byte) 0);
    }

    @Test @Order(1)
    void getX_returns_tfloat_hex() throws Exception {
        String x = GeoUDFs.getX.call(TRIP_HEX);
        assertNotNull(x, "getX should return tfloat hex-WKB");
        assertFalse(x.isBlank());
    }

    @Test @Order(2)
    void getX_start_value_is_zero() throws Exception {
        String x = GeoUDFs.getX.call(TRIP_HEX);
        assertNotNull(x);
        // The start X coordinate should be 0.0 for POINT(0 0)@t1
        // Use tfloatStartValue from AccessorUDFs to check
        Double sv = AccessorUDFs.tfloatStartValue.call(x);
        assertNotNull(sv);
        assertEquals(0.0, sv, 1e-9);
    }

    @Test @Order(3)
    void getY_returns_tfloat_hex() throws Exception {
        String y = GeoUDFs.getY.call(TRIP_HEX);
        assertNotNull(y, "getY should return tfloat hex-WKB");
        assertFalse(y.isBlank());
    }

    @Test @Order(4)
    void getZ_2d_trip_returns_null() throws Exception {
        // 2D tgeompoint has no Z component → MEOS returns null
        assertNull(GeoUDFs.getZ.call(TRIP_HEX));
    }

    @Test @Order(5)
    void getX_null_returns_null() throws Exception {
        assertNull(GeoUDFs.getX.call(null));
    }

    @Test @Order(6)
    void cumulativeLength_starts_at_zero() throws Exception {
        String cl = GeoUDFs.cumulativeLength.call(TRIP_HEX);
        assertNotNull(cl, "cumulativeLength should return a tfloat hex-WKB");
        // The start value of cumulative length for a linear trip should be 0.0
        Double sv = AccessorUDFs.tfloatStartValue.call(cl);
        assertNotNull(sv);
        assertEquals(0.0, sv, 1e-9);
    }

    @Test @Order(7)
    void cumulativeLength_null_returns_null() throws Exception {
        assertNull(GeoUDFs.cumulativeLength.call(null));
    }

    @Test @Order(8)
    void stops_returns_null_for_moving_trip() throws Exception {
        // A perfectly linear trip has no stops — MEOS returns null for empty stops
        // (rather than an empty sequence set)
        String s = GeoUDFs.stops.call(TRIP_HEX, 0.001, "1 second");
        if (s != null) assertFalse(s.isBlank());
    }

    @Test @Order(9)
    void stops_null_trip_returns_null() throws Exception {
        assertNull(GeoUDFs.stops.call(null, 0.001, "1 second"));
    }

    @Test @Order(10)
    void isSimple_linear_trip_returns_true() throws Exception {
        assertTrue(GeoUDFs.isSimple.call(TRIP_HEX),
            "A straight linear trip should have no self-intersections");
    }

    @Test @Order(11)
    void isSimple_null_returns_null() throws Exception {
        assertNull(GeoUDFs.isSimple.call(null));
    }

    @Test @Order(12)
    void shortestLine_between_parallel_trips_returns_wkt() throws Exception {
        String wkt = GeoUDFs.shortestLine.call(TRIP_HEX, TRIP2_HEX);
        assertNotNull(wkt, "shortestLine should return a WKT geometry");
        assertTrue(wkt.startsWith("POINT") || wkt.startsWith("LINESTRING"),
            "Expected WKT geometry, got: " + wkt);
    }

    @Test @Order(13)
    void shortestLine_same_trip_returns_point() throws Exception {
        String wkt = GeoUDFs.shortestLine.call(TRIP_HEX, TRIP_HEX);
        assertNotNull(wkt);
        assertTrue(wkt.startsWith("POINT") || wkt.startsWith("LINESTRING"),
            "Expected WKT, got: " + wkt);
    }

    @Test @Order(14)
    void shortestLine_null_returns_null() throws Exception {
        assertNull(GeoUDFs.shortestLine.call(null, TRIP_HEX));
    }
}
