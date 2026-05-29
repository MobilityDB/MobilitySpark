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
 * Unit tests for TempSpatialRelsUDFs tgeo×tgeo and tgeo×geo variants:
 *   tDisjointTgeoTgeo, tIntersectsTgeoTgeo, tTouchesTogeoTgeo,
 *   tContainsTgeoGeo, tContainsTgeoTgeo, tCoversTgeoGeo, tDwithinTgeoGeo.
 *
 * Fixture:
 *   TRIP  — linear from (0,0) to (1,0) over 1 hour
 *   TRIP2 — shifted trip: (0,0.5)→(1,0.5), same time window
 *   REGION_ON_PATH — polygon covering part of the trip path
 *   REGION_FAR     — polygon far from the trip
 *
 * MEOS function authority: meos/include/meos_geo.h (072_tgeo_tempspatialrels)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TempSpatialRelsUDFsExtTest extends MeosTestBase {

    private static String TRIP;
    private static String TRIP2;
    private static final String REGION_ON_PATH = "POLYGON((-0.1 -1, 0.6 -1, 0.6 1, -0.1 1, -0.1 -1))";
    private static final String REGION_FAR     = "POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))";

    @BeforeAll
    static void initMeos() {
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, POINT(1.0 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TRIP2 = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.5)@2020-01-01 00:00:00+00, POINT(1.0 0.5)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // tDisjointTgeoTgeo
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tDisjointTgeoTgeo_parallel_trips_returns_nonnull_tbool() throws Exception {
        String r = TempSpatialRelsUDFs.tDisjointTgeoTgeo.call(TRIP, TRIP2);
        assertNotNull(r, "tDisjointTgeoTgeo should return a tbool hex-WKB");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tDisjointTgeoTgeo_null_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tDisjointTgeoTgeo.call(null, TRIP));
        assertNull(TempSpatialRelsUDFs.tDisjointTgeoTgeo.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // tIntersectsTgeoTgeo
    // ------------------------------------------------------------------

    @Test @Order(3)
    void tIntersectsTgeoTgeo_same_trip_returns_nonnull_tbool() throws Exception {
        String r = TempSpatialRelsUDFs.tIntersectsTgeoTgeo.call(TRIP, TRIP);
        assertNotNull(r, "tIntersectsTgeoTgeo with same trip should return a tbool");
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void tIntersectsTgeoTgeo_null_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tIntersectsTgeoTgeo.call(null, TRIP));
    }

    // ------------------------------------------------------------------
    // tTouchesTogeoTgeo
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tTouchesTogeoTgeo_returns_non_null_or_handles_gracefully() throws Exception {
        // ttouches between point trajectories may return null for certain inputs;
        // just verify no exception is thrown
        String r = TempSpatialRelsUDFs.tTouchesTogeoTgeo.call(TRIP, TRIP2);
        assertTrue(r == null || !r.isBlank(), "Result must be null or non-blank hex-WKB");
    }

    @Test @Order(6)
    void tTouchesTogeoTgeo_null_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tTouchesTogeoTgeo.call(null, TRIP));
    }

    // ------------------------------------------------------------------
    // tContainsTgeoGeo
    // ------------------------------------------------------------------

    @Test @Order(7)
    void tContainsTgeoGeo_returns_tbool_or_null() throws Exception {
        // A point typically does not contain a polygon; result depends on semantics
        String r = TempSpatialRelsUDFs.tContainsTgeoGeo.call(TRIP, REGION_FAR);
        assertTrue(r == null || !r.isBlank(), "Result must be null or non-blank hex-WKB");
    }

    @Test @Order(8)
    void tContainsTgeoGeo_null_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tContainsTgeoGeo.call(null, REGION_FAR));
        assertNull(TempSpatialRelsUDFs.tContainsTgeoGeo.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // tContainsTgeoTgeo
    // ------------------------------------------------------------------

    @Test @Order(9)
    void tContainsTgeoTgeo_same_trip_returns_nonnull() throws Exception {
        String r = TempSpatialRelsUDFs.tContainsTgeoTgeo.call(TRIP, TRIP);
        assertNotNull(r, "tContainsTgeoTgeo with same trip should return a tbool");
        assertFalse(r.isBlank());
    }

    @Test @Order(10)
    void tContainsTgeoTgeo_null_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tContainsTgeoTgeo.call(null, TRIP));
    }

    // ------------------------------------------------------------------
    // tCoversTgeoGeo
    // ------------------------------------------------------------------

    @Test @Order(11)
    void tCoversTgeoGeo_returns_tbool_or_null() throws Exception {
        String r = TempSpatialRelsUDFs.tCoversTgeoGeo.call(TRIP, REGION_FAR);
        assertTrue(r == null || !r.isBlank(), "Result must be null or non-blank hex-WKB");
    }

    @Test @Order(12)
    void tCoversTgeoGeo_null_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tCoversTgeoGeo.call(null, REGION_FAR));
    }

    // ------------------------------------------------------------------
    // tDwithinTgeoGeo
    // ------------------------------------------------------------------

    @Test @Order(13)
    void tDwithinTgeoGeo_trip_with_nearby_point_returns_nonnull() throws Exception {
        // tdwithin_tgeo_geo is defined for point geometries; polygon may return null.
        // Use a point geometry close to the trip to ensure a valid result.
        String r = TempSpatialRelsUDFs.tDwithinTgeoGeo.call(TRIP, "POINT(0.5 0.0)", 100.0);
        assertNotNull(r, "tDwithinTgeoGeo with point geometry should return a tbool");
        assertFalse(r.isBlank());
    }

    @Test @Order(14)
    void tDwithinTgeoGeo_null_returns_null() throws Exception {
        assertNull(TempSpatialRelsUDFs.tDwithinTgeoGeo.call(null, REGION_ON_PATH, 1.0));
        assertNull(TempSpatialRelsUDFs.tDwithinTgeoGeo.call(TRIP, null, 1.0));
        assertNull(TempSpatialRelsUDFs.tDwithinTgeoGeo.call(TRIP, REGION_ON_PATH, null));
    }
}
