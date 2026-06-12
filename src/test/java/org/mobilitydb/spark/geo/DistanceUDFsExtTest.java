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
import org.mobilitydb.spark.temporal.ConstructorUDFs;

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for nearest approach distance (nadTgeoGeo, nadTgeoTgeo,
 * nadTgeoStbox) and nearest approach instant (naiTgeoGeo, naiTgeoTgeo).
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DistanceUDFsExtTest {

    private static String TRIP;
    private static String STBOX;

    @BeforeAll
    static void initMeos() throws Exception {
        meos_initialize();
        meos_initialize_timezone("UTC");

        // Simple 2-instant trip: (0,0)→(4,0)
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, "
                        + "POINT(4.0 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);

        // STBox covering (0,0)→(4,0) with a time span
        STBOX = ConstructorUDFs.stbox.call(
            "STBOX XT(((0,0),(4,0)),[2020-01-01 00:00:00+00,2020-01-01 01:00:00+00])");
    }

    // ------------------------------------------------------------------
    // nadTgeoGeo
    // ------------------------------------------------------------------

    @Test @Order(1)
    void nadTgeoGeo_collocated_returns_zero() throws Exception {
        // Trip lies on x-axis; POINT(2,0) is on the trajectory at mid-time.
        Double d = DistanceUDFs.nadTgeoGeo.call(TRIP, "POINT(2 0)");
        assertNotNull(d, "NAD must be non-null");
        assertEquals(0.0, d, 1e-9, "NAD to collocated point must be 0");
    }

    @Test @Order(2)
    void nadTgeoGeo_perpendicular_offset() throws Exception {
        // POINT(2,3) is 3 units above the trajectory.
        Double d = DistanceUDFs.nadTgeoGeo.call(TRIP, "POINT(2 3)");
        assertNotNull(d);
        assertEquals(3.0, d, 1e-6, "NAD to point 3 units above must be 3");
    }

    @Test @Order(3)
    void nadTgeoGeo_null_trip_returns_null() throws Exception {
        assertNull(DistanceUDFs.nadTgeoGeo.call(null, "POINT(0 0)"));
    }

    @Test @Order(4)
    void nadTgeoGeo_null_geom_returns_null() throws Exception {
        assertNull(DistanceUDFs.nadTgeoGeo.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // nadTgeoStbox
    // ------------------------------------------------------------------

    @Test @Order(5)
    void nadTgeoStbox_overlapping_returns_zero() throws Exception {
        Double d = DistanceUDFs.nadTgeoStbox.call(TRIP, STBOX);
        assertNotNull(d, "NAD tgeo×stbox must be non-null");
        assertEquals(0.0, d, 1e-6, "overlapping trip/stbox must give NAD 0");
    }

    @Test @Order(6)
    void nadTgeoStbox_null_trip_returns_null() throws Exception {
        assertNull(DistanceUDFs.nadTgeoStbox.call(null, STBOX));
    }

    @Test @Order(7)
    void nadTgeoStbox_null_stbox_returns_null() throws Exception {
        assertNull(DistanceUDFs.nadTgeoStbox.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // nadTgeoTgeo
    // ------------------------------------------------------------------

    @Test @Order(8)
    void nadTgeoTgeo_same_trip_returns_zero() throws Exception {
        Double d = DistanceUDFs.nadTgeoTgeo.call(TRIP, TRIP);
        assertNotNull(d);
        assertEquals(0.0, d, 1e-9, "NAD of a trip to itself must be 0");
    }

    @Test @Order(9)
    void nadTgeoTgeo_null_trip1_returns_null() throws Exception {
        assertNull(DistanceUDFs.nadTgeoTgeo.call(null, TRIP));
    }

    @Test @Order(10)
    void nadTgeoTgeo_null_trip2_returns_null() throws Exception {
        assertNull(DistanceUDFs.nadTgeoTgeo.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // naiTgeoGeo
    // ------------------------------------------------------------------

    @Test @Order(11)
    void naiTgeoGeo_returns_nonnull_hex() throws Exception {
        String r = DistanceUDFs.naiTgeoGeo.call(TRIP, "POINT(2 3)");
        assertNotNull(r, "NAI must return non-null hex-WKB TInstant");
        assertFalse(r.isBlank());
        assertTrue(r.matches("[0-9A-Fa-f]+"), "result must be hex-WKB");
    }

    @Test @Order(12)
    void naiTgeoGeo_null_trip_returns_null() throws Exception {
        assertNull(DistanceUDFs.naiTgeoGeo.call(null, "POINT(0 0)"));
    }

    @Test @Order(13)
    void naiTgeoGeo_null_geom_returns_null() throws Exception {
        assertNull(DistanceUDFs.naiTgeoGeo.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // naiTgeoTgeo
    // ------------------------------------------------------------------

    @Test @Order(14)
    void naiTgeoTgeo_returns_nonnull_hex() throws Exception {
        String r = DistanceUDFs.naiTgeoTgeo.call(TRIP, TRIP);
        assertNotNull(r, "NAI tgeo×tgeo must return non-null hex-WKB TInstant");
        assertFalse(r.isBlank());
        assertTrue(r.matches("[0-9A-Fa-f]+"), "result must be hex-WKB");
    }

    @Test @Order(15)
    void naiTgeoTgeo_null_trip1_returns_null() throws Exception {
        assertNull(DistanceUDFs.naiTgeoTgeo.call(null, TRIP));
    }

    @Test @Order(16)
    void naiTgeoTgeo_null_trip2_returns_null() throws Exception {
        assertNull(DistanceUDFs.naiTgeoTgeo.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // shortestLineTgeoGeo
    // ------------------------------------------------------------------

    @Test @Order(17)
    void shortestLineTgeoGeo_returns_wkt_geometry() throws Exception {
        String r = DistanceUDFs.shortestLineTgeoGeo.call(TRIP, "POINT(2 3)");
        assertNotNull(r, "shortestLine must return non-null WKT");
        assertFalse(r.isBlank());
        assertTrue(r.toUpperCase().startsWith("LINESTRING") || r.toUpperCase().startsWith("POINT"),
            "result must be WKT geometry");
    }

    @Test @Order(18)
    void shortestLineTgeoGeo_null_trip_returns_null() throws Exception {
        assertNull(DistanceUDFs.shortestLineTgeoGeo.call(null, "POINT(0 0)"));
    }

    @Test @Order(19)
    void shortestLineTgeoGeo_null_geom_returns_null() throws Exception {
        assertNull(DistanceUDFs.shortestLineTgeoGeo.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // shortestLineTgeoTgeo
    // ------------------------------------------------------------------

    @Test @Order(20)
    void shortestLineTgeoTgeo_returns_wkt_geometry() throws Exception {
        String r = DistanceUDFs.shortestLineTgeoTgeo.call(TRIP, TRIP);
        assertNotNull(r, "shortestLine tgeo×tgeo must return non-null WKT");
        assertFalse(r.isBlank());
        assertTrue(r.toUpperCase().startsWith("LINESTRING") || r.toUpperCase().startsWith("POINT"),
            "result must be WKT geometry");
    }

    @Test @Order(21)
    void shortestLineTgeoTgeo_null_trip1_returns_null() throws Exception {
        assertNull(DistanceUDFs.shortestLineTgeoTgeo.call(null, TRIP));
    }

    @Test @Order(22)
    void shortestLineTgeoTgeo_null_trip2_returns_null() throws Exception {
        assertNull(DistanceUDFs.shortestLineTgeoTgeo.call(TRIP, null));
    }
}
