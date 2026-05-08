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

package org.mobilitydb.spark.udfs;

import org.junit.jupiter.api.*;
import org.mobilitydb.spark.temporal.TemporalUDFs;

import java.util.HexFormat;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for temporal (time-axis) UDFs — runs without a Spark session.
 *
 * Geo-specific UDFs (eIntersects, nearestApproachDistance, eDwithin) are
 * covered in {@link org.mobilitydb.spark.geo.GeoUDFsTest}.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TemporalUDFsTest {

    private static String TRIP_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");
        TRIP_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, POINT(0.1 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    @Test @Order(1)
    void atTime_instant_inside_interval_returns_nonnull() throws Exception {
        String result = TemporalUDFs.atTime.call(TRIP_HEX, "2020-01-01 00:30:00+00");
        assertNotNull(result, "atTime should return a value inside the trip interval");
        assertFalse(result.isBlank());
    }

    @Test @Order(2)
    void atTime_instant_outside_interval_returns_null() throws Exception {
        assertNull(TemporalUDFs.atTime.call(TRIP_HEX, "2020-06-01 00:00:00+00"),
            "atTime should return null outside the trip interval");
    }

    @Test @Order(3)
    void atTime_null_trip_returns_null() throws Exception {
        assertNull(TemporalUDFs.atTime.call(null, "2020-01-01 00:30:00+00"));
    }

    @Test @Order(4)
    void atTime_period_inside_interval_returns_nonnull() throws Exception {
        String result = TemporalUDFs.atTime.call(TRIP_HEX, "[2020-01-01 00:00:00+00,2020-01-01 00:30:00+00]");
        assertNotNull(result, "atTime with period should return a value when trip overlaps the period");
        assertFalse(result.isBlank());
    }

    @Test @Order(5)
    void atTime_period_outside_interval_returns_null() throws Exception {
        assertNull(TemporalUDFs.atTime.call(TRIP_HEX, "[2020-06-01 00:00:00+00,2020-06-01 01:00:00+00]"),
            "atTime with period should return null when trip does not overlap the period");
    }

    @Test @Order(6)
    void asHexWKB_is_identity_on_hexwkb() throws Exception {
        String result = TemporalUDFs.asHexWKB.call(TRIP_HEX);
        assertEquals(TRIP_HEX, result,
            "asHexWKB(hexwkb) must be a lossless identity: parse then re-serialize");
    }

    @Test @Order(7)
    void asHexWKB_null_returns_null() throws Exception {
        assertNull(TemporalUDFs.asHexWKB.call(null));
    }

    // TemporalParquet round-trip tests -----------------------------------

    @Test @Order(8)
    void tintFromBinary_round_trips() throws Exception {
        String hex = temporal_as_hexwkb(tint_in("[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00]"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        String result = TemporalUDFs.tintFromBinary.call(bytes);
        assertEquals(hex, result, "tintFromBinary must round-trip through MEOS-WKB");
    }

    @Test @Order(9)
    void tfloatFromBinary_round_trips() throws Exception {
        String hex = temporal_as_hexwkb(tfloat_in("[1.5@2020-01-01 00:00:00+00, 2.5@2020-01-02 00:00:00+00]"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        String result = TemporalUDFs.tfloatFromBinary.call(bytes);
        assertEquals(hex, result, "tfloatFromBinary must round-trip through MEOS-WKB");
    }

    @Test @Order(10)
    void tboolFromBinary_round_trips() throws Exception {
        String hex = temporal_as_hexwkb(tbool_in("[true@2020-01-01 00:00:00+00, false@2020-01-02 00:00:00+00]"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        String result = TemporalUDFs.tboolFromBinary.call(bytes);
        assertEquals(hex, result, "tboolFromBinary must round-trip through MEOS-WKB");
    }

    @Test @Order(11)
    void ttextFromBinary_round_trips() throws Exception {
        String hex = temporal_as_hexwkb(ttext_in("[hello@2020-01-01 00:00:00+00, world@2020-01-02 00:00:00+00]"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        String result = TemporalUDFs.ttextFromBinary.call(bytes);
        assertEquals(hex, result, "ttextFromBinary must round-trip through MEOS-WKB");
    }

    @Test @Order(12)
    void asBinary_round_trips_with_tgeompoint() throws Exception {
        byte[] bytes = TemporalUDFs.asBinary.call(TRIP_HEX);
        assertNotNull(bytes, "asBinary must return non-null for valid hex-WKB");
        String back = HexFormat.of().formatHex(bytes).toUpperCase();
        assertEquals(TRIP_HEX, back, "asBinary then hex-encode must recover the original hex-WKB");
    }

    @Test @Order(13)
    void asBinary_null_returns_null() throws Exception {
        assertNull(TemporalUDFs.asBinary.call(null));
    }

    @Test @Order(14)
    void tgeompointFromBinary_round_trips() throws Exception {
        byte[] bytes = HexFormat.of().parseHex(TRIP_HEX.toLowerCase());
        String result = TemporalUDFs.tgeompointFromBinary.call(bytes);
        assertEquals(TRIP_HEX, result, "tgeompointFromBinary must round-trip through MEOS-WKB");
    }

    @Test @Order(15)
    void tgeogpointFromBinary_round_trips() throws Exception {
        String hex = temporal_as_hexwkb(
            tgeogpoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, POINT(0.1 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        String result = TemporalUDFs.tgeogpointFromBinary.call(bytes);
        assertEquals(hex, result, "tgeogpointFromBinary must round-trip through MEOS-WKB");
    }

    @Test @Order(16)
    void fromBinary_null_returns_null() throws Exception {
        assertNull(TemporalUDFs.tgeompointFromBinary.call(null));
        assertNull(TemporalUDFs.tgeogpointFromBinary.call(null));
        assertNull(TemporalUDFs.tintFromBinary.call(null));
        assertNull(TemporalUDFs.tfloatFromBinary.call(null));
        assertNull(TemporalUDFs.tboolFromBinary.call(null));
        assertNull(TemporalUDFs.ttextFromBinary.call(null));
    }

    @Test @Order(17)
    void asHexWKB_matches_mbdb_expected() throws Exception {
        // Known hex-WKB for [POINT(0 0)@2020-01-01 00:00:00+00, POINT(100 0)@2020-01-01 00:10:00+00]
        // Generated from MobilityDB: SELECT asHexWKB(trip) FROM Trips WHERE tripId = 1;
        String wkt = "[POINT(0 0)@2020-01-01 00:00:00+00, POINT(100 0)@2020-01-01 00:10:00+00]";
        String hexwkb = temporal_as_hexwkb(tgeompoint_in(wkt), (byte) 0);
        String expected = "012E000E02000000030101000000000000000000000000000000000000000060C286073E020001010000000000000000005940000000000000000000A685AA073E0200";
        assertEquals(expected, hexwkb,
            "MEOS hex-WKB must match MobilityDB asHexWKB() byte-for-byte");
        // asHexWKB UDF returns the same value
        assertEquals(expected, TemporalUDFs.asHexWKB.call(hexwkb));
    }
}
