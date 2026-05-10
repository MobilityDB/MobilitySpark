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

import java.sql.Timestamp;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SpanAlgebraUDFs new functions:
 *   intspanToFloatspan, floatspanToIntspan,
 *   datespanToTstzspan, tstzspanToDatespan,
 *   intsetToFloatset, floatsetToIntset,
 *   setToSpan, setToSpanset,
 *   tstzspanDuration, datespanDuration,
 *   tstzspanShiftScale, tstzspansetShiftScale,
 *   timestamptzToSpan, timestamptzToSet.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpanAlgebraUDFsExtTest {

    private static String INTSPAN_HEX;
    private static String FLOATSPAN_HEX;
    private static String TSTZSPAN_HEX;
    private static String DATESPAN_HEX;
    private static String INTSET_HEX;
    private static String FLOATSET_HEX;
    private static String TSTZSPANSET_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        INTSPAN_HEX      = span_as_hexwkb(intspan_in("[1, 10]"), (byte) 0);
        FLOATSPAN_HEX    = span_as_hexwkb(floatspan_in("[1.0, 10.0]"), (byte) 0);
        TSTZSPAN_HEX     = span_as_hexwkb(tstzspan_in(
            "[2020-01-01 00:00:00+00, 2020-01-03 00:00:00+00]"), (byte) 0);
        DATESPAN_HEX     = span_as_hexwkb(datespan_in("[2020-01-01, 2020-01-03]"), (byte) 0);
        INTSET_HEX       = set_as_hexwkb(intset_in("{1, 2, 3, 5}"), (byte) 0);
        FLOATSET_HEX     = set_as_hexwkb(floatset_in("{1.0, 2.0, 3.0}"), (byte) 0);
        TSTZSPANSET_HEX  = spanset_as_hexwkb(tstzspanset_in(
            "{[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]," +
            "[2020-02-01 00:00:00+00, 2020-02-03 00:00:00+00]}"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // intspanToFloatspan / floatspanToIntspan
    // ------------------------------------------------------------------

    @Test @Order(1)
    void intspanToFloatspan_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.intspanToFloatspan.call(INTSPAN_HEX);
        assertNotNull(r, "intspanToFloatspan must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void intspanToFloatspan_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.intspanToFloatspan.call(null));
    }

    @Test @Order(3)
    void floatspanToIntspan_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.floatspanToIntspan.call(FLOATSPAN_HEX);
        assertNotNull(r, "floatspanToIntspan must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void floatspanToIntspan_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.floatspanToIntspan.call(null));
    }

    // ------------------------------------------------------------------
    // datespanToTstzspan / tstzspanToDatespan
    // ------------------------------------------------------------------

    @Test @Order(5)
    void datespanToTstzspan_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.datespanToTstzspan.call(DATESPAN_HEX);
        assertNotNull(r, "datespanToTstzspan must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void datespanToTstzspan_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.datespanToTstzspan.call(null));
    }

    @Test @Order(7)
    void tstzspanToDatespan_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.tstzspanToDatespan.call(TSTZSPAN_HEX);
        assertNotNull(r, "tstzspanToDatespan must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(8)
    void tstzspanToDatespan_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.tstzspanToDatespan.call(null));
    }

    // ------------------------------------------------------------------
    // intsetToFloatset / floatsetToIntset
    // ------------------------------------------------------------------

    @Test @Order(9)
    void intsetToFloatset_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.intsetToFloatset.call(INTSET_HEX);
        assertNotNull(r, "intsetToFloatset must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(10)
    void intsetToFloatset_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.intsetToFloatset.call(null));
    }

    @Test @Order(11)
    void floatsetToIntset_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.floatsetToIntset.call(FLOATSET_HEX);
        assertNotNull(r, "floatsetToIntset must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(12)
    void floatsetToIntset_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.floatsetToIntset.call(null));
    }

    // ------------------------------------------------------------------
    // setToSpan / setToSpanset
    // ------------------------------------------------------------------

    @Test @Order(13)
    void setToSpan_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.setToSpan.call(INTSET_HEX);
        assertNotNull(r, "setToSpan must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(14)
    void setToSpan_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.setToSpan.call(null));
    }

    @Test @Order(15)
    void setToSpanset_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.setToSpanset.call(INTSET_HEX);
        assertNotNull(r, "setToSpanset must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(16)
    void setToSpanset_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.setToSpanset.call(null));
    }

    // ------------------------------------------------------------------
    // tstzspanDuration / datespanDuration
    // ------------------------------------------------------------------

    @Test @Order(17)
    void tstzspanDuration_returns_nonnull_string() throws Exception {
        String r = SpanAlgebraUDFs.tstzspanDuration.call(TSTZSPAN_HEX);
        assertNotNull(r, "tstzspanDuration must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(18)
    void tstzspanDuration_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.tstzspanDuration.call(null));
    }

    @Test @Order(19)
    void datespanDuration_returns_nonnull_string() throws Exception {
        String r = SpanAlgebraUDFs.datespanDuration.call(DATESPAN_HEX);
        assertNotNull(r, "datespanDuration must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(20)
    void datespanDuration_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.datespanDuration.call(null));
    }

    // ------------------------------------------------------------------
    // tstzspanShiftScale / tstzspansetShiftScale
    // ------------------------------------------------------------------

    @Test @Order(21)
    void tstzspanShiftScale_shift_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.tstzspanShiftScale.call(TSTZSPAN_HEX, "1 day", null);
        assertNotNull(r, "tstzspanShiftScale(shift) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(22)
    void tstzspanShiftScale_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.tstzspanShiftScale.call(null, "1 day", null));
        assertNull(SpanAlgebraUDFs.tstzspanShiftScale.call(TSTZSPAN_HEX, null, null));
    }

    @Test @Order(23)
    void tstzspansetShiftScale_shift_returns_nonnull() throws Exception {
        String r = SpanAlgebraUDFs.tstzspansetShiftScale.call(TSTZSPANSET_HEX, "1 day", null);
        assertNotNull(r, "tstzspansetShiftScale(shift) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(24)
    void tstzspansetShiftScale_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.tstzspansetShiftScale.call(null, "1 day", null));
    }

    // ------------------------------------------------------------------
    // timestamptzToSpan / timestamptzToSet
    // ------------------------------------------------------------------

    @Test @Order(25)
    void timestamptzToSpan_returns_nonnull() throws Exception {
        Timestamp ts = new Timestamp(1577836800000L); // 2020-01-01 00:00:00 UTC
        String r = SpanAlgebraUDFs.timestamptzToSpan.call(ts);
        assertNotNull(r, "timestamptzToSpan must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(26)
    void timestamptzToSpan_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.timestamptzToSpan.call(null));
    }

    @Test @Order(27)
    void timestamptzToSet_returns_nonnull() throws Exception {
        Timestamp ts = new Timestamp(1577836800000L); // 2020-01-01 00:00:00 UTC
        String r = SpanAlgebraUDFs.timestamptzToSet.call(ts);
        assertNotNull(r, "timestamptzToSet must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(28)
    void timestamptzToSet_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.timestamptzToSet.call(null));
    }

    // ------------------------------------------------------------------
    // Cross-type spanset × span algebra
    // ------------------------------------------------------------------

    @Test @Order(29)
    void spansetIntersectionSpan_overlapping_returns_nonnull() throws Exception {
        // TSTZSPANSET_HEX has two periods; intersect with the first span
        String overlapSpan = span_as_hexwkb(tstzspan_in(
            "[2020-01-01 06:00:00+00, 2020-01-01 18:00:00+00]"), (byte) 0);
        String r = SpanAlgebraUDFs.spansetIntersectionSpan.call(TSTZSPANSET_HEX, overlapSpan);
        assertNotNull(r, "spansetIntersectionSpan must return non-null for overlap");
        assertFalse(r.isBlank());
    }

    @Test @Order(30)
    void spansetUnionSpan_returns_nonnull() throws Exception {
        String addSpan = span_as_hexwkb(tstzspan_in(
            "[2020-03-01 00:00:00+00, 2020-03-03 00:00:00+00]"), (byte) 0);
        String r = SpanAlgebraUDFs.spansetUnionSpan.call(TSTZSPANSET_HEX, addSpan);
        assertNotNull(r, "spansetUnionSpan must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(31)
    void spansetMinusSpan_removes_overlap() throws Exception {
        // remove a chunk from within the first period
        String removeSpan = span_as_hexwkb(tstzspan_in(
            "[2020-01-01 06:00:00+00, 2020-01-01 18:00:00+00]"), (byte) 0);
        String r = SpanAlgebraUDFs.spansetMinusSpan.call(TSTZSPANSET_HEX, removeSpan);
        assertNotNull(r, "spansetMinusSpan must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(32)
    void spansetIntersectionSpan_null_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.spansetIntersectionSpan.call(null, TSTZSPAN_HEX));
        assertNull(SpanAlgebraUDFs.spansetIntersectionSpan.call(TSTZSPANSET_HEX, null));
    }
}
