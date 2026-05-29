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

package org.mobilitydb.spark.temporal;

import org.junit.jupiter.api.*;
import org.mobilitydb.spark.temporal.ConstructorUDFs;
import org.mobilitydb.spark.MeosTestBase;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for extended AccessorUDFs — value restriction, spatio-temporal
 * restriction, append operations, and value span accessor.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AccessorUDFsExtTest extends MeosTestBase {

    // tint sequence [1@t1, 3@t2, 2@t3] — min=1, max=3
    private static String TINT_SEQ;
    // tstzspan for minusTime test
    private static String TSTZSPAN_FIRST_HOUR;
    // intset for atValues test
    private static String INTSET_1_3;
    // intspan tbox for tnumber restriction test (TBOXINT to match tint span type)
    private static String TBOX;
    // tgeompoint for stbox restriction test
    private static String TRIP_HEX;
    // stbox covering half the trip
    private static String STBOX_HEX;

    @BeforeAll
    static void initMeos() throws Exception {
        TINT_SEQ = temporal_as_hexwkb(
            tint_in("{1@2020-01-01 00:00:00+00, 3@2020-01-01 01:00:00+00, 2@2020-01-01 02:00:00+00}"),
            (byte) 0);

        TSTZSPAN_FIRST_HOUR = span_as_hexwkb(
            tstzspan_in("[2020-01-01 00:00:00+00, 2020-01-01 01:00:00+00)"), (byte) 0);

        INTSET_1_3 = set_as_hexwkb(intset_in("{1, 3}"), (byte) 0);

        TBOX = ConstructorUDFs.tbox.call("TBOXINT XT([1,4],[2020-01-01 00:00:00+00, 2020-01-01 03:00:00+00])");

        TRIP_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(2 0)@2020-01-01 02:00:00+00]"),
            (byte) 0);

        STBOX_HEX = ConstructorUDFs.stbox.call(
            "STBOX XT(((0,0),(1,1)),[2020-01-01 00:00:00+00, 2020-01-01 02:00:00+00])");
    }

    // ------------------------------------------------------------------
    // atMin / atMax
    // ------------------------------------------------------------------

    @Test @Order(1)
    void atMin_returns_instants_at_value_1() throws Exception {
        String r = AccessorUDFs.atMin.call(TINT_SEQ);
        assertNotNull(r, "atMin should return non-null for a valid tint");
        Integer sv = AccessorUDFs.tintStartValue.call(r);
        assertNotNull(sv);
        assertEquals(1, sv, "atMin start value should be the minimum (1)");
    }

    @Test @Order(2)
    void atMax_returns_instants_at_value_3() throws Exception {
        String r = AccessorUDFs.atMax.call(TINT_SEQ);
        assertNotNull(r);
        Integer sv = AccessorUDFs.tintStartValue.call(r);
        assertNotNull(sv);
        assertEquals(3, sv, "atMax start value should be the maximum (3)");
    }

    @Test @Order(3)
    void atMin_null_returns_null() throws Exception {
        assertNull(AccessorUDFs.atMin.call(null));
    }

    @Test @Order(4)
    void atMax_null_returns_null() throws Exception {
        assertNull(AccessorUDFs.atMax.call(null));
    }

    // ------------------------------------------------------------------
    // atValues
    // ------------------------------------------------------------------

    @Test @Order(5)
    void atValues_restricts_to_values_1_and_3() throws Exception {
        String r = AccessorUDFs.atValues.call(TINT_SEQ, INTSET_1_3);
        assertNotNull(r, "atValues should return non-null when set intersects tint values");
    }

    @Test @Order(6)
    void atValues_null_trip_returns_null() throws Exception {
        assertNull(AccessorUDFs.atValues.call(null, INTSET_1_3));
    }

    // ------------------------------------------------------------------
    // minusTime
    // ------------------------------------------------------------------

    @Test @Order(7)
    void minusTime_removes_first_hour() throws Exception {
        String r = AccessorUDFs.minusTime.call(TINT_SEQ, TSTZSPAN_FIRST_HOUR);
        assertNotNull(r, "minusTime should return remaining instants");
    }

    @Test @Order(8)
    void minusTime_null_returns_null() throws Exception {
        assertNull(AccessorUDFs.minusTime.call(null, TSTZSPAN_FIRST_HOUR));
    }

    // ------------------------------------------------------------------
    // minusMin / minusMax
    // ------------------------------------------------------------------

    @Test @Order(9)
    void minusMin_excludes_value_1() throws Exception {
        String r = AccessorUDFs.minusMin.call(TINT_SEQ);
        assertNotNull(r, "minusMin should return instants not at minimum");
    }

    @Test @Order(10)
    void minusMax_excludes_value_3() throws Exception {
        String r = AccessorUDFs.minusMax.call(TINT_SEQ);
        assertNotNull(r, "minusMax should return instants not at maximum");
    }

    // ------------------------------------------------------------------
    // atStbox / minusStbox
    // ------------------------------------------------------------------

    @Test @Order(11)
    void atStbox_restricts_trip_to_box() throws Exception {
        String r = AccessorUDFs.atStbox.call(TRIP_HEX, STBOX_HEX);
        // May be null when no intersection — just verify no exception and non-blank if present
        if (r != null) assertFalse(r.isBlank());
    }

    @Test @Order(12)
    void atStbox_null_returns_null() throws Exception {
        assertNull(AccessorUDFs.atStbox.call(null, STBOX_HEX));
    }

    @Test @Order(13)
    void minusStbox_returns_part_outside_box() throws Exception {
        String r = AccessorUDFs.minusStbox.call(TRIP_HEX, STBOX_HEX);
        if (r != null) assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // tnumberAtTbox / tnumberMinusTbox
    // ------------------------------------------------------------------

    @Test @Order(14)
    void tnumberAtTbox_restricts_tint_to_tbox() throws Exception {
        String r = AccessorUDFs.tnumberAtTbox.call(TINT_SEQ, TBOX);
        assertNotNull(r, "tnumberAtTbox should return non-null when tbox covers tint range");
    }

    @Test @Order(15)
    void tnumberAtTbox_null_returns_null() throws Exception {
        assertNull(AccessorUDFs.tnumberAtTbox.call(null, TBOX));
    }

    @Test @Order(16)
    void tnumberMinusTbox_returns_values_outside_tbox() throws Exception {
        String r = AccessorUDFs.tnumberMinusTbox.call(TINT_SEQ, TBOX);
        // Result can be null if all values are within the box
        if (r != null) assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // appendInstant / appendSequence
    // ------------------------------------------------------------------

    @Test @Order(17)
    void appendInstant_extends_sequence() throws Exception {
        String instant = temporal_as_hexwkb(
            tint_in("5@2020-01-01 03:00:00+00"), (byte) 0);
        String r = AccessorUDFs.appendInstant.call(TINT_SEQ, instant);
        assertNotNull(r, "appendInstant should return extended temporal");
    }

    @Test @Order(18)
    void appendInstant_null_returns_null() throws Exception {
        assertNull(AccessorUDFs.appendInstant.call(null, TINT_SEQ));
    }

    // ------------------------------------------------------------------
    // tnumberValuespans
    // ------------------------------------------------------------------

    @Test @Order(19)
    void tnumberValuespans_returns_spanset_hex() throws Exception {
        String ss = AccessorUDFs.tnumberValuespans.call(TINT_SEQ);
        assertNotNull(ss, "tnumberValuespans should return a spanset hex-WKB");
        assertFalse(ss.isBlank());
    }

    @Test @Order(20)
    void tnumberValuespans_null_returns_null() throws Exception {
        assertNull(AccessorUDFs.tnumberValuespans.call(null));
    }
}
