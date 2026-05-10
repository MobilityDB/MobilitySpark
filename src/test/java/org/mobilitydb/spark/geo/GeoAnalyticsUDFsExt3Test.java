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

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for tpoint spatial analytics UDFs:
 *   tpointConvexHull, tpointExpandSpace.
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GeoAnalyticsUDFsExt3Test {

    private static String TRIP;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, "
                        + "POINT(2.0 2.0)@2020-01-01 01:00:00+00, "
                        + "POINT(4.0 0.0)@2020-01-01 02:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // tpointConvexHull
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tpointConvexHull_returns_nonnull() throws Exception {
        String r = GeoAnalyticsUDFs.tpointConvexHull.call(TRIP);
        assertNotNull(r, "tpointConvexHull must return non-null hex-EWKB for valid trip");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tpointConvexHull_result_is_parseable_hex() throws Exception {
        String r = GeoAnalyticsUDFs.tpointConvexHull.call(TRIP);
        assertNotNull(r);
        // All hex characters — a valid hex-EWKB string has only hex digits.
        assertTrue(r.matches("[0-9A-Fa-f]+"), "result must be a hex string");
    }

    @Test @Order(3)
    void tpointConvexHull_null_trip_returns_null() throws Exception {
        assertNull(GeoAnalyticsUDFs.tpointConvexHull.call(null));
    }

    // ------------------------------------------------------------------
    // tpointExpandSpace
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tpointExpandSpace_returns_nonnull() throws Exception {
        String r = GeoAnalyticsUDFs.tpointExpandSpace.call(TRIP, 1.0);
        assertNotNull(r, "tpointExpandSpace must return non-null hex-WKB STBOX");
        assertFalse(r.isBlank());
    }

    @Test @Order(5)
    void tpointExpandSpace_result_is_parseable_hex() throws Exception {
        String r = GeoAnalyticsUDFs.tpointExpandSpace.call(TRIP, 0.5);
        assertNotNull(r);
        assertTrue(r.matches("[0-9A-Fa-f]+"), "result must be a hex string");
    }

    @Test @Order(6)
    void tpointExpandSpace_null_trip_returns_null() throws Exception {
        assertNull(GeoAnalyticsUDFs.tpointExpandSpace.call(null, 1.0));
    }

    @Test @Order(7)
    void tpointExpandSpace_null_distance_returns_null() throws Exception {
        assertNull(GeoAnalyticsUDFs.tpointExpandSpace.call(TRIP, null));
    }

    @Test @Order(8)
    void tpointExpandSpace_zero_distance_returns_nonnull() throws Exception {
        String r = GeoAnalyticsUDFs.tpointExpandSpace.call(TRIP, 0.0);
        assertNotNull(r, "zero expansion distance must still produce a valid STBOX");
        assertFalse(r.isBlank());
    }
}
