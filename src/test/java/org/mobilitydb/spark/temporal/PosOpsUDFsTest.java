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
import org.mobilitydb.spark.MeosTestBase;

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PosOpsUDFs — temporal and spatial positional operators.
 *
 * Fixtures:
 *   TRIP_EARLY  — tgeompoint January 2020
 *   TRIP_LATE   — tgeompoint July 2020
 *   TINT_LOW    — tint with values [1,3]
 *   TINT_HIGH   — tint with values [10,20]
 *   TRIP_LEFT   — tgeompoint with X in [0,1]
 *   TRIP_RIGHT  — tgeompoint with X in [5,6]
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PosOpsUDFsTest extends MeosTestBase {

    private static String TRIP_EARLY;
    private static String TRIP_LATE;
    private static String TINT_LOW;
    private static String TINT_HIGH;
    private static String TRIP_LEFT;
    private static String TRIP_RIGHT;
    private static String SPAN_EARLY;
    private static String SPAN_LATE;

    @BeforeAll
    static void initMeos() {
        TRIP_EARLY = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01, POINT(1 0)@2020-01-02]"),
            (byte) 0);
        TRIP_LATE = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-07-01, POINT(1 0)@2020-07-02]"),
            (byte) 0);
        TINT_LOW = temporal_as_hexwkb(
            tint_in("[1@2020-01-01, 3@2020-01-02]"),
            (byte) 0);
        TINT_HIGH = temporal_as_hexwkb(
            tint_in("[10@2020-01-01, 20@2020-01-02]"),
            (byte) 0);
        TRIP_LEFT = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01, POINT(1 0)@2020-01-02]"),
            (byte) 0);
        TRIP_RIGHT = temporal_as_hexwkb(
            tgeompoint_in("[POINT(5 0)@2020-01-01, POINT(6 0)@2020-01-02]"),
            (byte) 0);
        SPAN_EARLY = span_as_hexwkb(tstzspan_in("[2020-01-01, 2020-02-01]"), (byte) 0);
        SPAN_LATE  = span_as_hexwkb(tstzspan_in("[2020-07-01, 2020-08-01]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // Time-direction: temporal ↔ temporal
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalBefore_early_before_late_is_true() throws Exception {
        assertTrue(PosOpsUDFs.temporalBefore.call(TRIP_EARLY, TRIP_LATE));
    }

    @Test @Order(2)
    void temporalBefore_late_before_early_is_false() throws Exception {
        assertFalse(PosOpsUDFs.temporalBefore.call(TRIP_LATE, TRIP_EARLY));
    }

    @Test @Order(3)
    void temporalAfter_late_after_early_is_true() throws Exception {
        assertTrue(PosOpsUDFs.temporalAfter.call(TRIP_LATE, TRIP_EARLY));
    }

    @Test @Order(4)
    void temporalOverbefore_overlapping_start_is_true() throws Exception {
        // TRIP_EARLY ends before TRIP_LATE starts → overbefore is true
        assertTrue(PosOpsUDFs.temporalOverbefore.call(TRIP_EARLY, TRIP_LATE));
    }

    @Test @Order(5)
    void temporalOverafter_late_overafter_early_is_true() throws Exception {
        assertTrue(PosOpsUDFs.temporalOverafter.call(TRIP_LATE, TRIP_EARLY));
    }

    @Test @Order(6)
    void temporalBefore_null_input_returns_null() throws Exception {
        assertNull(PosOpsUDFs.temporalBefore.call(null, TRIP_LATE));
        assertNull(PosOpsUDFs.temporalBefore.call(TRIP_EARLY, null));
    }

    // ------------------------------------------------------------------
    // Time-direction: temporal ↔ tstzspan
    // ------------------------------------------------------------------

    @Test @Order(7)
    void temporalBeforeSpan_early_before_late_span_is_true() throws Exception {
        assertTrue(PosOpsUDFs.temporalBeforeSpan.call(TRIP_EARLY, SPAN_LATE));
    }

    @Test @Order(8)
    void temporalAfterSpan_late_after_early_span_is_true() throws Exception {
        assertTrue(PosOpsUDFs.temporalAfterSpan.call(TRIP_LATE, SPAN_EARLY));
    }

    @Test @Order(9)
    void temporalOverbeforeSpan_null_returns_null() throws Exception {
        assertNull(PosOpsUDFs.temporalOverbeforeSpan.call(null, SPAN_LATE));
    }

    @Test @Order(10)
    void temporalOverafterSpan_null_returns_null() throws Exception {
        assertNull(PosOpsUDFs.temporalOverafterSpan.call(TRIP_LATE, null));
    }

    // ------------------------------------------------------------------
    // Value-direction: tnumber ↔ tnumber
    // ------------------------------------------------------------------

    @Test @Order(11)
    void tnumberLeft_low_left_of_high_is_true() throws Exception {
        assertTrue(PosOpsUDFs.tnumberLeft.call(TINT_LOW, TINT_HIGH));
    }

    @Test @Order(12)
    void tnumberRight_high_right_of_low_is_true() throws Exception {
        assertTrue(PosOpsUDFs.tnumberRight.call(TINT_HIGH, TINT_LOW));
    }

    @Test @Order(13)
    void tnumberOverleft_low_overleft_of_high_is_true() throws Exception {
        assertTrue(PosOpsUDFs.tnumberOverleft.call(TINT_LOW, TINT_HIGH));
    }

    @Test @Order(14)
    void tnumberOverright_high_overright_of_low_is_true() throws Exception {
        assertTrue(PosOpsUDFs.tnumberOverright.call(TINT_HIGH, TINT_LOW));
    }

    @Test @Order(15)
    void tnumberLeft_null_returns_null() throws Exception {
        assertNull(PosOpsUDFs.tnumberLeft.call(null, TINT_HIGH));
    }

    // ------------------------------------------------------------------
    // Spatial x-axis: tpoint ↔ tpoint
    // ------------------------------------------------------------------

    @Test @Order(16)
    void tpointLeft_left_trip_is_left_of_right_trip() throws Exception {
        assertTrue(PosOpsUDFs.tpointLeft.call(TRIP_LEFT, TRIP_RIGHT));
    }

    @Test @Order(17)
    void tpointRight_right_trip_is_right_of_left_trip() throws Exception {
        assertTrue(PosOpsUDFs.tpointRight.call(TRIP_RIGHT, TRIP_LEFT));
    }

    @Test @Order(18)
    void tpointOverleft_left_overleft_of_right() throws Exception {
        assertTrue(PosOpsUDFs.tpointOverleft.call(TRIP_LEFT, TRIP_RIGHT));
    }

    @Test @Order(19)
    void tpointOverright_right_overright_of_left() throws Exception {
        assertTrue(PosOpsUDFs.tpointOverright.call(TRIP_RIGHT, TRIP_LEFT));
    }

    @Test @Order(20)
    void tpointLeft_null_returns_null() throws Exception {
        assertNull(PosOpsUDFs.tpointLeft.call(null, TRIP_RIGHT));
    }

    // ------------------------------------------------------------------
    // Spatial y-axis: tpoint ↔ tpoint
    // ------------------------------------------------------------------

    @Test @Order(21)
    void tpointBelow_and_above_y_axis() throws Exception {
        // TRIP_LEFT has Y=0; TRIP_RIGHT has Y=0 — same Y, so neither is strictly below
        // Use a trip with Y=5 for the above direction
        String tripHigh = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 5)@2020-01-01, POINT(1 5)@2020-01-02]"), (byte) 0);
        assertTrue(PosOpsUDFs.tpointBelow.call(TRIP_LEFT, tripHigh));
        assertTrue(PosOpsUDFs.tpointAbove.call(tripHigh, TRIP_LEFT));
    }

    @Test @Order(22)
    void tpointOverbelow_and_overabove_y_axis() throws Exception {
        String tripHigh = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 5)@2020-01-01, POINT(1 5)@2020-01-02]"), (byte) 0);
        assertTrue(PosOpsUDFs.tpointOverbelow.call(TRIP_LEFT, tripHigh));
        assertTrue(PosOpsUDFs.tpointOverabove.call(tripHigh, TRIP_LEFT));
    }
}
