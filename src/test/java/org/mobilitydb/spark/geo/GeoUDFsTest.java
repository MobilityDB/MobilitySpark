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
 * Unit tests for GeoUDFs — spatial relations and distance on tgeompoint.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GeoUDFsTest {

    private static String TRIP_HEX;
    private static String TRIP2_HEX;
    private static String TRIP_FAR_HEX;
    // Geometry passed as WKT text — eIntersects parses via geo_from_text internally
    private static final String POINT_ON_PATH = "POINT(0.05 0.0)";
    private static final String POINT_FAR     = "POINT(10.0 10.0)";

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TRIP_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, POINT(0.1 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TRIP2_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.05 0.001)@2020-01-01 00:00:00+00, POINT(0.15 0.001)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TRIP_FAR_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(1000.0 1000.0)@2020-01-01 00:00:00+00, POINT(2000.0 1000.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
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

    @Test @Order(10)
    void length_returns_positive() throws Exception {
        Double len = GeoUDFs.length.call(TRIP_HEX);
        assertNotNull(len);
        assertTrue(len > 0.0, "length should be positive for a moving trip, got " + len);
    }

    @Test @Order(11)
    void length_null_returns_null() throws Exception {
        assertNull(GeoUDFs.length.call(null));
    }

    @Test @Order(12)
    void valueAtTimestamp_inside_interval_returns_wkt() throws Exception {
        // 2020-01-01 00:30:00 UTC — inside TRIP_HEX interval [00:00, 01:00] UTC
        java.sql.Timestamp ts = new java.sql.Timestamp(
            java.time.Instant.parse("2020-01-01T00:30:00Z").toEpochMilli());
        String wkt = GeoUDFs.valueAtTimestamp.call(TRIP_HEX, ts);
        assertNotNull(wkt, "valueAtTimestamp should return WKT inside trip interval");
        assertTrue(wkt.startsWith("POINT("), "Expected WKT POINT, got: " + wkt);
    }

    @Test @Order(13)
    void valueAtTimestamp_outside_interval_returns_null() throws Exception {
        // 2020-06-01 00:00:00 UTC — well outside TRIP_HEX interval
        java.sql.Timestamp ts = new java.sql.Timestamp(
            java.time.Instant.parse("2020-06-01T00:00:00Z").toEpochMilli());
        assertNull(GeoUDFs.valueAtTimestamp.call(TRIP_HEX, ts),
            "valueAtTimestamp should return null outside trip interval");
    }

    @Test @Order(14)
    void tDwithin_returns_tbool_hex() throws Exception {
        String tboolHex = GeoUDFs.tDwithin.call(TRIP_HEX, TRIP2_HEX, 1.0);
        assertNotNull(tboolHex);
        assertFalse(tboolHex.isBlank(), "tDwithin should return non-empty tbool hex");
    }

    @Test @Order(15)
    void whenTrue_returns_spanset_text() throws Exception {
        String tboolHex = GeoUDFs.tDwithin.call(TRIP_HEX, TRIP2_HEX, 1.0);
        assertNotNull(tboolHex);
        String spans = GeoUDFs.whenTrue.call(tboolHex);
        assertNotNull(spans, "whenTrue should return a tstzspanset text");
        assertTrue(spans.contains("["), "Expected tstzspanset with brackets, got: " + spans);
    }

    @Test @Order(16)
    void aDisjoint_far_trips_returns_true() throws Exception {
        assertTrue(GeoUDFs.aDisjoint.call(TRIP_HEX, TRIP_FAR_HEX),
            "Trips far apart should be always disjoint");
    }

    @Test @Order(17)
    void aDisjoint_overlapping_trips_returns_false() throws Exception {
        assertFalse(GeoUDFs.aDisjoint.call(TRIP_HEX, TRIP_HEX),
            "Identical trips should not be always disjoint");
    }

    @Test @Order(18)
    void geomContains_point_inside_polygon() throws Exception {
        String polygon = "POLYGON((40 -1,60 -1,60 6,40 6,40 -1))";
        String inside  = "POINT(50 2)";
        assertTrue(GeoUDFs.geomContains.call(polygon, inside));
    }

    @Test @Order(19)
    void geomContains_point_outside_polygon() throws Exception {
        String polygon = "POLYGON((40 -1,60 -1,60 6,40 6,40 -1))";
        String outside = "POINT(100 100)";
        assertFalse(GeoUDFs.geomContains.call(polygon, outside));
    }

    @Test @Order(20)
    void tgeompointFromBinary_roundtrip_matches_hex() throws Exception {
        // Convert TRIP_HEX to bytes, decode via tgeompointFromBinary, and
        // verify we get an equivalent hex-WKB back (round-trip identity).
        byte[] bytes = java.util.HexFormat.of().parseHex(TRIP_HEX);
        String decoded = GeoUDFs.tgeompointFromBinary.call(bytes);
        assertNotNull(decoded, "tgeompointFromBinary should decode valid MEOS-WKB bytes");
        // Both represent the same trip — cross-check via length().
        Double lenFromHex  = GeoUDFs.length.call(TRIP_HEX);
        Double lenFromBin  = GeoUDFs.length.call(decoded);
        assertNotNull(lenFromBin);
        assertEquals(lenFromHex, lenFromBin, 1e-9,
            "length after binary round-trip should equal length from original hex-WKB");
    }

    @Test @Order(21)
    void tgeompointFromBinary_null_input_returns_null() throws Exception {
        assertNull(GeoUDFs.tgeompointFromBinary.call(null));
    }

    @Test @Order(22)
    void maxSpeed_returns_positive_for_moving_trip() throws Exception {
        Double spd = GeoUDFs.maxSpeed.call(TRIP_HEX);
        assertNotNull(spd, "maxSpeed should return a value for a moving trip");
        assertTrue(spd > 0, "maxSpeed of a moving trip should be positive");
    }

    @Test @Order(23)
    void duration_returns_expected_interval() throws Exception {
        // TRIP_HEX spans [2020-01-01 00:00:00, 2020-01-01 01:00:00] → 1 hour
        String dur = GeoUDFs.duration.call(TRIP_HEX);
        assertNotNull(dur, "duration should return an interval string");
        assertTrue(dur.contains("01:00:00"),
            "duration of a 1-hour trip should contain '01:00:00', got: " + dur);
    }
}
