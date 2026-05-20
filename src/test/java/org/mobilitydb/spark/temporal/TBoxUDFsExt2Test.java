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

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.junit.jupiter.api.*;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for new TBoxUDFs:
 *   tboxMake, timestamptzToTbox, numspanTimestamptzToTbox,
 *   tboxExpandTime, tboxShiftScaleTime,
 *   tboxExpandFloat, tboxExpandInt,
 *   tboxfloatXmin, tboxfloatXmax, tboxintXmin, tboxintXmax.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TBoxUDFsExt2Test {

    private static String TBOX_XT_HEX;      // TBox with X and T dimensions (floatspan)
    private static String TBOX_T_HEX;       // TBox with T dimension only
    private static String FLOATSPAN_HEX;
    private static String TSTZSPAN_HEX;
    private static String TBOX_INT_HEX;     // TBox with X and T dimensions (intspan)
    private static String INTSPAN_HEX;
    private static String TBOX_FUTURE_HEX;  // TBox strictly after TBOX_XT_HEX in time
    private static String TBOX_HIGH_HEX;    // TBox strictly right of TBOX_XT_HEX in X

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        // Build a tbox with float X + time via tnumber_to_tbox on a tfloat
        Pointer tfloatPtr = tfloat_in("[1.5@2020-01-01 00:00:00+00, 9.5@2020-01-03 00:00:00+00]");
        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        TBOX_XT_HEX = tbox_as_hexwkb(tnumber_to_tbox(tfloatPtr), (byte) 0, sizeOut);

        // Build a tbox with integer X + time via tnumber_to_tbox on a tint
        Pointer tintPtr = tint_in("[1@2020-01-01 00:00:00+00, 9@2020-01-03 00:00:00+00]");
        sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        TBOX_INT_HEX = tbox_as_hexwkb(tnumber_to_tbox(tintPtr), (byte) 0, sizeOut);

        // T-only TBox from tstzspan
        Pointer tstzspanPtr = tstzspan_in("[2020-01-01 00:00:00+00, 2020-01-03 00:00:00+00]");
        TSTZSPAN_HEX = span_as_hexwkb(tstzspanPtr, (byte) 0);
        sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        TBOX_T_HEX = tbox_as_hexwkb(span_to_tbox(tstzspanPtr), (byte) 0, sizeOut);

        FLOATSPAN_HEX = span_as_hexwkb(floatspan_in("[1.0, 10.0]"), (byte) 0);
        INTSPAN_HEX   = span_as_hexwkb(intspan_in("[1, 10]"), (byte) 0);

        // TBox strictly after TBOX_XT_HEX in time (2020-01-10 onwards)
        Pointer futurePtr = tfloat_in("[1.5@2020-01-10 00:00:00+00, 1.5@2020-01-12 00:00:00+00]");
        sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        TBOX_FUTURE_HEX = tbox_as_hexwkb(tnumber_to_tbox(futurePtr), (byte) 0, sizeOut);

        // TBox strictly right of TBOX_XT_HEX in X (X range [100, 200])
        Pointer highPtr = tfloat_in("[100.0@2020-01-01 00:00:00+00, 200.0@2020-01-03 00:00:00+00]");
        sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        TBOX_HIGH_HEX = tbox_as_hexwkb(tnumber_to_tbox(highPtr), (byte) 0, sizeOut);
    }

    // ------------------------------------------------------------------
    // tboxMake
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tboxMake_xt_returns_nonnull() throws Exception {
        String r = TBoxUDFs.tboxMake.call(FLOATSPAN_HEX, TSTZSPAN_HEX);
        assertNotNull(r, "tboxMake(floatspan, tstzspan) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tboxMake_t_only_returns_nonnull() throws Exception {
        String r = TBoxUDFs.tboxMake.call(null, TSTZSPAN_HEX);
        assertNotNull(r, "tboxMake(null, tstzspan) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void tboxMake_both_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxMake.call(null, null));
    }

    // ------------------------------------------------------------------
    // timestamptzToTbox
    // ------------------------------------------------------------------

    @Test @Order(4)
    void timestamptzToTbox_returns_nonnull() throws Exception {
        java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2020-01-01 00:00:00");
        String r = TBoxUDFs.timestamptzToTbox.call(ts);
        assertNotNull(r, "timestamptzToTbox must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(5)
    void timestamptzToTbox_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.timestamptzToTbox.call((java.sql.Timestamp) null));
    }

    // ------------------------------------------------------------------
    // numspanTimestamptzToTbox
    // ------------------------------------------------------------------

    @Test @Order(6)
    void numspanTimestamptzToTbox_returns_nonnull() throws Exception {
        java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2020-01-01 00:00:00");
        String r = TBoxUDFs.numspanTimestamptzToTbox.call(FLOATSPAN_HEX, ts);
        assertNotNull(r, "numspanTimestamptzToTbox must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void numspanTimestamptzToTbox_null_returns_null() throws Exception {
        java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2020-01-01 00:00:00");
        assertNull(TBoxUDFs.numspanTimestamptzToTbox.call(null, ts));
        assertNull(TBoxUDFs.numspanTimestamptzToTbox.call(FLOATSPAN_HEX, null));
    }

    // ------------------------------------------------------------------
    // tboxExpandTime
    // ------------------------------------------------------------------

    @Test @Order(8)
    void tboxExpandTime_returns_nonnull() throws Exception {
        String r = TBoxUDFs.tboxExpandTime.call(TBOX_XT_HEX, "1 day");
        assertNotNull(r, "tboxExpandTime must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void tboxExpandTime_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxExpandTime.call(null, "1 day"));
        assertNull(TBoxUDFs.tboxExpandTime.call(TBOX_XT_HEX, null));
    }

    // ------------------------------------------------------------------
    // tboxShiftScaleTime
    // ------------------------------------------------------------------

    @Test @Order(10)
    void tboxShiftScaleTime_shift_returns_nonnull() throws Exception {
        String r = TBoxUDFs.tboxShiftScaleTime.call(TBOX_XT_HEX, "1 day", null);
        assertNotNull(r, "tboxShiftScaleTime(shift) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(11)
    void tboxShiftScaleTime_scale_returns_nonnull() throws Exception {
        String r = TBoxUDFs.tboxShiftScaleTime.call(TBOX_XT_HEX, null, "2 days");
        assertNotNull(r, "tboxShiftScaleTime(scale) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(12)
    void tboxShiftScaleTime_null_stbox_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxShiftScaleTime.call(null, "1 day", null));
    }

    // ------------------------------------------------------------------
    // tboxfloatXmin / tboxfloatXmax
    // ------------------------------------------------------------------

    @Test @Order(13)
    void tboxfloatXmin_returns_correct_value() throws Exception {
        Double r = TBoxUDFs.tboxfloatXmin.call(TBOX_XT_HEX);
        assertNotNull(r, "tboxfloatXmin must return non-null for XT tbox");
        assertEquals(1.5, r, 1e-9);
    }

    @Test @Order(14)
    void tboxfloatXmax_returns_correct_value() throws Exception {
        Double r = TBoxUDFs.tboxfloatXmax.call(TBOX_XT_HEX);
        assertNotNull(r, "tboxfloatXmax must return non-null for XT tbox");
        assertEquals(9.5, r, 1e-9);
    }

    @Test @Order(15)
    void tboxfloatXmin_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxfloatXmin.call(null));
    }

    // ------------------------------------------------------------------
    // tboxintXmin / tboxintXmax
    // ------------------------------------------------------------------

    @Test @Order(16)
    void tboxintXmin_returns_correct_value() throws Exception {
        Integer r = TBoxUDFs.tboxintXmin.call(TBOX_INT_HEX);
        assertNotNull(r, "tboxintXmin must return non-null for integer tbox");
        assertEquals(1, r);
    }

    @Test @Order(17)
    void tboxintXmax_returns_correct_value() throws Exception {
        Integer r = TBoxUDFs.tboxintXmax.call(TBOX_INT_HEX);
        assertNotNull(r, "tboxintXmax must return non-null for integer tbox");
        assertEquals(9, r);
    }

    @Test @Order(18)
    void tboxintXmin_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxintXmin.call(null));
    }

    // ------------------------------------------------------------------
    // intersectionTboxTbox / unionTboxTbox
    // ------------------------------------------------------------------

    @Test @Order(19)
    void intersectionTboxTbox_overlapping_boxes_returns_nonnull() throws Exception {
        String r = TBoxUDFs.intersectionTboxTbox.call(TBOX_XT_HEX, TBOX_XT_HEX);
        assertNotNull(r, "intersection of a box with itself must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(20)
    void unionTboxTbox_returns_nonnull() throws Exception {
        String r = TBoxUDFs.unionTboxTbox.call(TBOX_XT_HEX, TBOX_XT_HEX);
        assertNotNull(r, "union of a box with itself must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(21)
    void intersectionTboxTbox_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.intersectionTboxTbox.call(null, TBOX_XT_HEX));
        assertNull(TBoxUDFs.intersectionTboxTbox.call(TBOX_XT_HEX, null));
    }

    @Test @Order(22)
    void unionTboxTbox_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.unionTboxTbox.call(null, TBOX_XT_HEX));
    }

    // ------------------------------------------------------------------
    // TBox positional predicates
    // ------------------------------------------------------------------

    @Test @Order(23)
    void tboxLeft_x_left_of_high_box_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxLeft.call(TBOX_XT_HEX, TBOX_HIGH_HEX),
            "tbox([1.5,9.5]) is strictly left of tbox([100,200])");
    }

    @Test @Order(24)
    void tboxRight_high_box_right_of_x_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxRight.call(TBOX_HIGH_HEX, TBOX_XT_HEX),
            "tbox([100,200]) is strictly right of tbox([1.5,9.5])");
    }

    @Test @Order(25)
    void tboxBefore_xt_before_future_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxBefore.call(TBOX_XT_HEX, TBOX_FUTURE_HEX),
            "tbox ending 2020-01-03 is before tbox starting 2020-01-10");
    }

    @Test @Order(26)
    void tboxAfter_future_after_xt_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxAfter.call(TBOX_FUTURE_HEX, TBOX_XT_HEX),
            "tbox starting 2020-01-10 is after tbox ending 2020-01-03");
    }

    @Test @Order(27)
    void tboxLeft_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxLeft.call(null, TBOX_XT_HEX));
        assertNull(TBoxUDFs.tboxLeft.call(TBOX_XT_HEX, null));
    }

    // ------------------------------------------------------------------
    // TBox topology predicates
    // ------------------------------------------------------------------

    @Test @Order(28)
    void tboxContains_same_box_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxContains.call(TBOX_XT_HEX, TBOX_XT_HEX),
            "a tbox contains itself");
    }

    @Test @Order(29)
    void tboxContained_same_box_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxContained.call(TBOX_XT_HEX, TBOX_XT_HEX),
            "a tbox is contained in itself");
    }

    @Test @Order(30)
    void tboxOverlaps_same_box_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxOverlaps.call(TBOX_XT_HEX, TBOX_XT_HEX),
            "a tbox overlaps itself");
    }

    @Test @Order(31)
    void tboxContains_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxContains.call(null, TBOX_XT_HEX));
        assertNull(TBoxUDFs.tboxContains.call(TBOX_XT_HEX, null));
    }
}
