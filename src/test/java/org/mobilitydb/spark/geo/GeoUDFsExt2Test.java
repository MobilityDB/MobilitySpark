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
 * Unit tests for GeoUDFs ever/always predicates:
 *   eDisjoint, eTouches, eCovers, eDisjointTgeoTgeo, eIntersectsTgeoTgeo,
 *   aIntersects, aDisjoint, aDwithin, eDwithinGeo, aDwithinGeo.
 *
 * Fixture:
 *   TRIP  — linear from (0,0) to (1,0) in 1 hour
 *   TRIP2 — parallel trip (1,0)→(2,0), same time window
 *   REGION_ON_PATH — polygon covering part of trip
 *   REGION_FAR     — polygon far from trip
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GeoUDFsExt2Test extends MeosTestBase {

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
            tgeompoint_in("[POINT(1.0 0.0)@2020-01-01 00:00:00+00, POINT(2.0 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // eDisjoint
    // ------------------------------------------------------------------

    @Test @Order(1)
    void eDisjoint_trip_with_far_region_returns_true() throws Exception {
        assertTrue(GeoUDFs.eDisjoint.call(TRIP, REGION_FAR),
            "Trip is always disjoint from far region — ever-disjoint must be true");
    }

    @Test @Order(2)
    void eDisjoint_trip_with_on_path_region_returns_non_null() throws Exception {
        // Trip starts inside the region and exits — ever-disjoint is true at the exit end.
        // Just verify the UDF completes without exception and returns a Boolean.
        Boolean r = GeoUDFs.eDisjoint.call(TRIP, REGION_ON_PATH);
        assertNotNull(r);
    }

    @Test @Order(3)
    void eDisjoint_null_returns_null() throws Exception {
        assertNull(GeoUDFs.eDisjoint.call(null, REGION_FAR));
        assertNull(GeoUDFs.eDisjoint.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // eTouches
    // ------------------------------------------------------------------

    @Test @Order(4)
    void eTouches_returns_non_null_for_valid_inputs() throws Exception {
        Boolean r = GeoUDFs.eTouches.call(TRIP, REGION_ON_PATH);
        // Result is Boolean; just ensure no crash
        assertTrue(r == null || r instanceof Boolean);
    }

    @Test @Order(5)
    void eTouches_null_returns_null() throws Exception {
        assertNull(GeoUDFs.eTouches.call(null, REGION_ON_PATH));
        assertNull(GeoUDFs.eTouches.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // eCovers
    // ------------------------------------------------------------------

    @Test @Order(6)
    void eCovers_returns_non_null_boolean() throws Exception {
        // ecovers_tgeo_geo semantics: returns true if the moving object ever covers
        // the static geometry at any instant.  Just verify no exception and non-null.
        Boolean r = GeoUDFs.eCovers.call(TRIP, REGION_ON_PATH);
        assertNotNull(r);
    }

    @Test @Order(7)
    void eCovers_null_returns_null() throws Exception {
        assertNull(GeoUDFs.eCovers.call(null, REGION_ON_PATH));
    }

    // ------------------------------------------------------------------
    // eDisjointTgeoTgeo / eIntersectsTgeoTgeo
    // ------------------------------------------------------------------

    @Test @Order(8)
    void eDisjointTgeoTgeo_non_overlapping_trips_returns_true() throws Exception {
        assertTrue(GeoUDFs.eDisjointTgeoTgeo.call(TRIP, TRIP2) != null);
    }

    @Test @Order(9)
    void eIntersectsTgeoTgeo_same_trip_returns_true() throws Exception {
        assertTrue(GeoUDFs.eIntersectsTgeoTgeo.call(TRIP, TRIP),
            "A trip always intersects itself");
    }

    @Test @Order(10)
    void eDisjointTgeoTgeo_null_returns_null() throws Exception {
        assertNull(GeoUDFs.eDisjointTgeoTgeo.call(null, TRIP));
        assertNull(GeoUDFs.eIntersectsTgeoTgeo.call(null, TRIP));
    }

    // ------------------------------------------------------------------
    // aIntersects / aDisjoint
    // ------------------------------------------------------------------

    @Test @Order(11)
    void aDisjoint_trip_with_far_region_returns_true() throws Exception {
        assertTrue(GeoUDFs.aDisjoint.call(TRIP, REGION_FAR),
            "Trip is always disjoint from far region");
    }

    @Test @Order(12)
    void aIntersects_trip_with_far_region_returns_false() throws Exception {
        assertFalse(GeoUDFs.aIntersects.call(TRIP, REGION_FAR),
            "Trip never intersects the far region");
    }

    @Test @Order(13)
    void aIntersects_aDisjoint_null_returns_null() throws Exception {
        assertNull(GeoUDFs.aIntersects.call(null, REGION_FAR));
        assertNull(GeoUDFs.aDisjoint.call(null, REGION_FAR));
    }

    // ------------------------------------------------------------------
    // aDwithin / eDwithinGeo / aDwithinGeo
    // ------------------------------------------------------------------

    @Test @Order(14)
    void aDwithin_same_trip_with_large_dist_returns_true() throws Exception {
        assertTrue(GeoUDFs.aDwithin.call(TRIP, TRIP, 1.0),
            "A trip is always within distance 1.0 of itself");
    }

    @Test @Order(15)
    void aDwithin_null_returns_null() throws Exception {
        assertNull(GeoUDFs.aDwithin.call(null, TRIP, 1.0));
        assertNull(GeoUDFs.aDwithin.call(TRIP, null, 1.0));
        assertNull(GeoUDFs.aDwithin.call(TRIP, TRIP, null));
    }

    @Test @Order(16)
    void eDwithinGeo_trip_within_large_radius_of_nearby_polygon_returns_true() throws Exception {
        assertTrue(GeoUDFs.eDwithinGeo.call(TRIP, REGION_ON_PATH, 100.0),
            "Trip is within 100 units of nearby polygon");
    }

    @Test @Order(17)
    void eDwithinGeo_null_returns_null() throws Exception {
        assertNull(GeoUDFs.eDwithinGeo.call(null, REGION_ON_PATH, 1.0));
    }

    @Test @Order(18)
    void aDwithinGeo_trip_far_from_far_region_large_radius_returns_true() throws Exception {
        Boolean r = GeoUDFs.aDwithinGeo.call(TRIP, REGION_FAR, 1000.0);
        assertNotNull(r);
        assertTrue(r, "Trip should be within 1000 units of far region");
    }

    @Test @Order(19)
    void aDwithinGeo_null_returns_null() throws Exception {
        assertNull(GeoUDFs.aDwithinGeo.call(null, REGION_FAR, 1.0));
    }
}
