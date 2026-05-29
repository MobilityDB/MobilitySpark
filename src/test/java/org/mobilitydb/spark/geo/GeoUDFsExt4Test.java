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
 * Unit tests for tpoint I/O and transformation UDFs:
 *   tpointAsText, tpointAsEWKT, tpointSRID, tpointSetSRID, tpointRound,
 *   tgeomToTgeog, tgeogToTgeom, tpointToStbox.
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GeoUDFsExt4Test extends MeosTestBase {

    private static String TRIP;

    @BeforeAll
    static void initMeos() {
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(1.0 2.0)@2020-01-01 00:00:00+00, "
                        + "POINT(3.0 4.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // tpointAsText
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tpointAsText_returns_nonnull() throws Exception {
        String r = GeoUDFs.tpointAsText.call(TRIP, 6);
        assertNotNull(r, "tpointAsText must return non-null for valid trip");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tpointAsText_contains_point_keyword() throws Exception {
        String r = GeoUDFs.tpointAsText.call(TRIP, 6);
        assertNotNull(r);
        assertTrue(r.toUpperCase().contains("POINT"), "output must contain POINT keyword");
    }

    @Test @Order(3)
    void tpointAsText_null_trip_returns_null() throws Exception {
        assertNull(GeoUDFs.tpointAsText.call(null, 6));
    }

    @Test @Order(4)
    void tpointAsText_null_precision_uses_default() throws Exception {
        String r = GeoUDFs.tpointAsText.call(TRIP, null);
        assertNotNull(r, "null precision must fall back to 15");
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // tpointAsEWKT
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tpointAsEWKT_returns_nonnull() throws Exception {
        String r = GeoUDFs.tpointAsEWKT.call(TRIP, 6);
        assertNotNull(r, "tpointAsEWKT must return non-null for valid trip");
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void tpointAsEWKT_contains_point_keyword() throws Exception {
        String r = GeoUDFs.tpointAsEWKT.call(TRIP, 6);
        assertNotNull(r);
        assertTrue(r.toUpperCase().contains("POINT"), "EWKT must contain POINT keyword");
    }

    @Test @Order(7)
    void tpointAsEWKT_null_trip_returns_null() throws Exception {
        assertNull(GeoUDFs.tpointAsEWKT.call(null, 6));
    }

    @Test @Order(8)
    void tpointAsEWKT_null_precision_uses_default() throws Exception {
        String r = GeoUDFs.tpointAsEWKT.call(TRIP, null);
        assertNotNull(r, "null precision must fall back to 15");
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // tpointSRID
    // ------------------------------------------------------------------

    @Test @Order(9)
    void tpointSRID_returns_integer() throws Exception {
        Integer r = GeoUDFs.tpointSRID.call(TRIP);
        assertNotNull(r, "tpointSRID must return non-null for valid trip");
    }

    @Test @Order(10)
    void tpointSRID_null_trip_returns_null() throws Exception {
        assertNull(GeoUDFs.tpointSRID.call(null));
    }

    // ------------------------------------------------------------------
    // tpointSetSRID
    // ------------------------------------------------------------------

    @Test @Order(11)
    void tpointSetSRID_returns_nonnull() throws Exception {
        String r = GeoUDFs.tpointSetSRID.call(TRIP, 4326);
        assertNotNull(r, "tpointSetSRID must return non-null hex-WKB");
        assertFalse(r.isBlank());
    }

    @Test @Order(12)
    void tpointSetSRID_result_is_valid_temporal() throws Exception {
        // ISO WKB (variant 0) does not embed SRID, so a hex-WKB round-trip resets
        // SRID to 0. This test verifies the function completes without error and
        // the result can be deserialized as a valid temporal.
        String r = GeoUDFs.tpointSetSRID.call(TRIP, 4326);
        assertNotNull(r);
        String text = GeoUDFs.tpointAsText.call(r, 6);
        assertNotNull(text, "result of tpointSetSRID must be a valid temporal");
    }

    @Test @Order(13)
    void tpointSetSRID_null_trip_returns_null() throws Exception {
        assertNull(GeoUDFs.tpointSetSRID.call(null, 4326));
    }

    @Test @Order(14)
    void tpointSetSRID_null_srid_returns_null() throws Exception {
        assertNull(GeoUDFs.tpointSetSRID.call(TRIP, null));
    }

    // ------------------------------------------------------------------
    // tpointRound
    // ------------------------------------------------------------------

    @Test @Order(15)
    void tpointRound_returns_nonnull() throws Exception {
        String r = GeoUDFs.tpointRound.call(TRIP, 2);
        assertNotNull(r, "tpointRound must return non-null hex-WKB");
        assertFalse(r.isBlank());
    }

    @Test @Order(16)
    void tpointRound_null_trip_returns_null() throws Exception {
        assertNull(GeoUDFs.tpointRound.call(null, 2));
    }

    @Test @Order(17)
    void tpointRound_null_decimals_uses_default() throws Exception {
        String r = GeoUDFs.tpointRound.call(TRIP, null);
        assertNotNull(r, "null decimals must fall back to 6");
        assertFalse(r.isBlank());
    }

    @Test @Order(18)
    void tpointRound_preserves_type() throws Exception {
        String r = GeoUDFs.tpointRound.call(TRIP, 3);
        assertNotNull(r);
        // The result must be a valid temporal: tpointSRID must succeed.
        Integer srid = GeoUDFs.tpointSRID.call(r);
        assertNotNull(srid, "rounded trip must still be a valid tgeompoint");
    }

    // ------------------------------------------------------------------
    // tpointToStbox
    // ------------------------------------------------------------------

    @Test @Order(19)
    void tpointToStbox_returns_nonnull() throws Exception {
        String r = GeoUDFs.tpointToStbox.call(TRIP);
        assertNotNull(r, "tpointToStbox must return non-null hex-WKB STBOX");
        assertFalse(r.isBlank());
    }

    @Test @Order(20)
    void tpointToStbox_null_trip_returns_null() throws Exception {
        assertNull(GeoUDFs.tpointToStbox.call(null));
    }
}
