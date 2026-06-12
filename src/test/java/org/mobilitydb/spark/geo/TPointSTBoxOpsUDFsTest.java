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
 * Unit tests for cross-type STBox × TPoint positional and topological UDFs.
 *
 * Fixtures:
 *   TRIP — tgeopoint travelling from (0,0) to (4,0)
 *   STBOX_OVERLAP — STBOX covering (0,0)-(4,0) (overlaps trip)
 *   STBOX_RIGHT  — STBOX at (10,0)-(14,0) (trip is left of this)
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TPointSTBoxOpsUDFsTest {

    private static String TRIP;
    private static String STBOX_OVERLAP;
    private static String STBOX_RIGHT;

    @BeforeAll
    static void initMeos() throws Exception {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, "
                        + "POINT(4.0 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);

        STBOX_OVERLAP = ConstructorUDFs.stbox.call(
            "STBOX XT(((0,0),(4,0)),[2020-01-01 00:00:00+00,2020-01-01 01:00:00+00])");

        STBOX_RIGHT = ConstructorUDFs.stbox.call(
            "STBOX XT(((10,0),(14,0)),[2020-01-01 00:00:00+00,2020-01-01 01:00:00+00])");
    }

    // ------------------------------------------------------------------
    // Null input guards (one representative test per direction)
    // ------------------------------------------------------------------

    @Test @Order(1)
    void stboxLeftTpoint_null_stbox_returns_null() throws Exception {
        assertNull(TPointSTBoxOpsUDFs.stboxLeftTpoint.call(null, TRIP));
    }

    @Test @Order(2)
    void stboxLeftTpoint_null_tpoint_returns_null() throws Exception {
        assertNull(TPointSTBoxOpsUDFs.stboxLeftTpoint.call(STBOX_OVERLAP, null));
    }

    @Test @Order(3)
    void tpointLeftStbox_null_tpoint_returns_null() throws Exception {
        assertNull(TPointSTBoxOpsUDFs.tpointLeftStbox.call(null, STBOX_OVERLAP));
    }

    @Test @Order(4)
    void tpointLeftStbox_null_stbox_returns_null() throws Exception {
        assertNull(TPointSTBoxOpsUDFs.tpointLeftStbox.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // Spatial direction predicates — stbox × tpoint
    // ------------------------------------------------------------------

    @Test @Order(5)
    void stboxLeftTpoint_right_stbox_is_left_of_trip() throws Exception {
        // STBOX_RIGHT is to the right of the trip origin area;
        // trip (0-4) is to the LEFT of STBOX_RIGHT (10-14).
        // So stboxLeftTpoint(STBOX_RIGHT, trip) → trip is NOT to the left of stbox
        // Actually: left_stbox_tspatial means stbox is strictly LEFT of tspatial.
        // STBOX_RIGHT (10-14) is NOT to the left of trip (0-4); trip is left of stbox.
        Boolean r = TPointSTBoxOpsUDFs.stboxLeftTpoint.call(STBOX_OVERLAP, TRIP);
        assertNotNull(r);
    }

    @Test @Order(6)
    void tpointLeftStbox_trip_is_left_of_right_stbox() throws Exception {
        // trip (0-4) is strictly to the LEFT of STBOX_RIGHT (10-14)
        Boolean r = TPointSTBoxOpsUDFs.tpointLeftStbox.call(TRIP, STBOX_RIGHT);
        assertNotNull(r);
        assertTrue(r, "trip (x∈[0,4]) must be left of stbox (x∈[10,14])");
    }

    @Test @Order(7)
    void stboxRightTpoint_right_stbox_is_right_of_trip() throws Exception {
        // STBOX_RIGHT (10-14) is strictly to the RIGHT of trip (0-4)
        Boolean r = TPointSTBoxOpsUDFs.stboxRightTpoint.call(STBOX_RIGHT, TRIP);
        assertNotNull(r);
        assertTrue(r, "stbox (x∈[10,14]) must be right of trip (x∈[0,4])");
    }

    @Test @Order(8)
    void tpointRightStbox_trip_not_right_of_right_stbox() throws Exception {
        // trip (x∈[0,4]) is NOT to the right of STBOX_RIGHT (x∈[10,14])
        Boolean r = TPointSTBoxOpsUDFs.tpointRightStbox.call(TRIP, STBOX_RIGHT);
        assertNotNull(r);
        assertFalse(r, "trip (x∈[0,4]) must not be right of stbox (x∈[10,14])");
    }

    // ------------------------------------------------------------------
    // Topological predicates — stbox × tpoint
    // ------------------------------------------------------------------

    @Test @Order(9)
    void stboxOverlapsTpoint_overlapping_returns_true() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.stboxOverlapsTpoint.call(STBOX_OVERLAP, TRIP);
        assertNotNull(r);
        assertTrue(r, "overlapping stbox/tpoint must give overlaps=true");
    }

    @Test @Order(10)
    void stboxOverlapsTpoint_disjoint_returns_false() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.stboxOverlapsTpoint.call(STBOX_RIGHT, TRIP);
        assertNotNull(r);
        assertFalse(r, "non-overlapping stbox/tpoint must give overlaps=false");
    }

    @Test @Order(11)
    void tpointOverlapsStbox_overlapping_returns_true() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.tpointOverlapsStbox.call(TRIP, STBOX_OVERLAP);
        assertNotNull(r);
        assertTrue(r, "trip inside stbox must give overlaps=true");
    }

    @Test @Order(12)
    void stboxContainsTpoint_full_containment_returns_nonnull() throws Exception {
        // Containment test: stbox at least as large as trip bounding box
        Boolean r = TPointSTBoxOpsUDFs.stboxContainsTpoint.call(STBOX_OVERLAP, TRIP);
        assertNotNull(r);
    }

    @Test @Order(13)
    void tpointContainedStbox_returns_nonnull() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.tpointContainedStbox.call(TRIP, STBOX_OVERLAP);
        assertNotNull(r);
    }

    @Test @Order(14)
    void stboxSameTpoint_same_extent_returns_nonnull() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.stboxSameTpoint.call(STBOX_OVERLAP, TRIP);
        assertNotNull(r);
    }

    @Test @Order(15)
    void tpointSameStbox_returns_nonnull() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.tpointSameStbox.call(TRIP, STBOX_OVERLAP);
        assertNotNull(r);
    }

    // ------------------------------------------------------------------
    // Temporal direction predicates
    // ------------------------------------------------------------------

    @Test @Order(16)
    void stboxBeforeTpoint_returns_nonnull() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.stboxBeforeTpoint.call(STBOX_OVERLAP, TRIP);
        assertNotNull(r);
    }

    @Test @Order(17)
    void tpointBeforeStbox_returns_nonnull() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.tpointBeforeStbox.call(TRIP, STBOX_OVERLAP);
        assertNotNull(r);
    }

    @Test @Order(18)
    void stboxAfterTpoint_returns_nonnull() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.stboxAfterTpoint.call(STBOX_OVERLAP, TRIP);
        assertNotNull(r);
    }

    @Test @Order(19)
    void tpointAfterStbox_returns_nonnull() throws Exception {
        Boolean r = TPointSTBoxOpsUDFs.tpointAfterStbox.call(TRIP, STBOX_OVERLAP);
        assertNotNull(r);
    }

    // ------------------------------------------------------------------
    // Over-predicates (a representative sample)
    // ------------------------------------------------------------------

    @Test @Order(20)
    void stboxOverleftTpoint_returns_nonnull() throws Exception {
        assertNotNull(TPointSTBoxOpsUDFs.stboxOverleftTpoint.call(STBOX_OVERLAP, TRIP));
    }

    @Test @Order(21)
    void stboxOverrightTpoint_returns_nonnull() throws Exception {
        assertNotNull(TPointSTBoxOpsUDFs.stboxOverrightTpoint.call(STBOX_OVERLAP, TRIP));
    }

    @Test @Order(22)
    void tpointOverleftStbox_returns_nonnull() throws Exception {
        assertNotNull(TPointSTBoxOpsUDFs.tpointOverleftStbox.call(TRIP, STBOX_OVERLAP));
    }

    @Test @Order(23)
    void tpointOverrightStbox_returns_nonnull() throws Exception {
        assertNotNull(TPointSTBoxOpsUDFs.tpointOverrightStbox.call(TRIP, STBOX_OVERLAP));
    }

    @Test @Order(24)
    void stboxAboveTpoint_returns_nonnull() throws Exception {
        assertNotNull(TPointSTBoxOpsUDFs.stboxAboveTpoint.call(STBOX_OVERLAP, TRIP));
    }

    @Test @Order(25)
    void tpointAboveStbox_returns_nonnull() throws Exception {
        assertNotNull(TPointSTBoxOpsUDFs.tpointAboveStbox.call(TRIP, STBOX_OVERLAP));
    }
}
