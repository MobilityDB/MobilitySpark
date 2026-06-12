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

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SpanAccessorUDFs — span/spanset/set bound and count accessors.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpanAccessorUDFsTest {

    private static String INTSPAN_1_10;
    private static String INTSPAN_5_15;
    private static String FLOATSPAN;
    private static String BIGINTSPAN;
    private static String DATESPAN;
    private static String TSTZSPAN;
    private static String INTSPANSET;
    private static String INTSET;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        INTSPAN_1_10 = span_as_hexwkb(intspan_in("[1,10)"),   (byte) 0);
        INTSPAN_5_15 = span_as_hexwkb(intspan_in("[5,15)"),   (byte) 0);
        FLOATSPAN    = span_as_hexwkb(floatspan_in("[1.5,4.5)"), (byte) 0);
        BIGINTSPAN   = span_as_hexwkb(bigintspan_in("[100,200)"), (byte) 0);
        DATESPAN     = span_as_hexwkb(datespan_in("[2020-01-01,2020-02-01)"), (byte) 0);
        TSTZSPAN     = span_as_hexwkb(tstzspan_in("[2020-01-01 00:00:00+00,2020-01-02 00:00:00+00)"), (byte) 0);
        INTSPANSET   = spanset_as_hexwkb(intspanset_in("{[1,5),[10,20)}"), (byte) 0);
        INTSET       = set_as_hexwkb(intset_in("{2,4,6,8}"), (byte) 0);
    }

    @Test @Order(1)
    void intspanLower_returns_one() throws Exception {
        assertEquals(1, SpanAccessorUDFs.intspanLower.call(INTSPAN_1_10));
    }

    @Test @Order(2)
    void intspanUpper_returns_ten() throws Exception {
        assertEquals(10, SpanAccessorUDFs.intspanUpper.call(INTSPAN_1_10));
    }

    @Test @Order(3)
    void intspanWidth_returns_nine() throws Exception {
        assertEquals(9, SpanAccessorUDFs.intspanWidth.call(INTSPAN_1_10));
    }

    @Test @Order(4)
    void intspanLower_null_input_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.intspanLower.call(null));
    }

    @Test @Order(5)
    void floatspanLower_returns_correct_value() throws Exception {
        Double lo = SpanAccessorUDFs.floatspanLower.call(FLOATSPAN);
        assertNotNull(lo);
        assertEquals(1.5, lo, 1e-9);
    }

    @Test @Order(6)
    void floatspanUpper_returns_correct_value() throws Exception {
        Double hi = SpanAccessorUDFs.floatspanUpper.call(FLOATSPAN);
        assertNotNull(hi);
        assertEquals(4.5, hi, 1e-9);
    }

    @Test @Order(7)
    void floatspanWidth_returns_three() throws Exception {
        Double w = SpanAccessorUDFs.floatspanWidth.call(FLOATSPAN);
        assertNotNull(w);
        assertEquals(3.0, w, 1e-9);
    }

    @Test @Order(8)
    void bigintspanLower_returns_100() throws Exception {
        assertEquals(100L, SpanAccessorUDFs.bigintspanLower.call(BIGINTSPAN));
    }

    @Test @Order(9)
    void bigintspanUpper_returns_200() throws Exception {
        assertEquals(200L, SpanAccessorUDFs.bigintspanUpper.call(BIGINTSPAN));
    }

    @Test @Order(10)
    void datespanLower_returns_2020_01_01() throws Exception {
        java.sql.Date d = SpanAccessorUDFs.datespanLower.call(DATESPAN);
        assertNotNull(d);
        assertEquals("2020-01-01", d.toString());
    }

    @Test @Order(11)
    void datespanUpper_returns_2020_02_01() throws Exception {
        java.sql.Date d = SpanAccessorUDFs.datespanUpper.call(DATESPAN);
        assertNotNull(d);
        assertEquals("2020-02-01", d.toString());
    }

    @Test @Order(12)
    void tstzspanLower_returns_2020_01_01() throws Exception {
        java.sql.Timestamp ts = SpanAccessorUDFs.tstzspanLower.call(TSTZSPAN);
        assertNotNull(ts);
        assertTrue(ts.toInstant().toString().startsWith("2020-01-01"),
            "Expected 2020-01-01, got: " + ts.toInstant());
    }

    @Test @Order(13)
    void tstzspanUpper_returns_2020_01_02() throws Exception {
        java.sql.Timestamp ts = SpanAccessorUDFs.tstzspanUpper.call(TSTZSPAN);
        assertNotNull(ts);
        assertTrue(ts.toInstant().toString().startsWith("2020-01-02"),
            "Expected 2020-01-02, got: " + ts.toInstant());
    }

    @Test @Order(14)
    void spanLowerInc_closed_lower() throws Exception {
        assertTrue(SpanAccessorUDFs.spanLowerInc.call(INTSPAN_1_10));
    }

    @Test @Order(15)
    void spanUpperInc_open_upper_returns_false() throws Exception {
        assertFalse(SpanAccessorUDFs.spanUpperInc.call(INTSPAN_1_10));
    }

    @Test @Order(16)
    void spansetNumSpans_returns_two() throws Exception {
        assertEquals(2, SpanAccessorUDFs.spansetNumSpans.call(INTSPANSET));
    }

    @Test @Order(17)
    void spansetStartSpan_returns_non_null_hex() throws Exception {
        String start = SpanAccessorUDFs.spansetStartSpan.call(INTSPANSET);
        assertNotNull(start, "spansetStartSpan should return a span hex-WKB");
        assertFalse(start.isBlank());
    }

    @Test @Order(18)
    void spansetEndSpan_lower_bound_is_10() throws Exception {
        String end = SpanAccessorUDFs.spansetEndSpan.call(INTSPANSET);
        assertNotNull(end);
        assertEquals(10, SpanAccessorUDFs.intspanLower.call(end));
    }

    @Test @Order(19)
    void setNumValues_returns_four() throws Exception {
        assertEquals(4, SpanAccessorUDFs.setNumValues.call(INTSET));
    }

    @Test @Order(20)
    void setNumValues_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.setNumValues.call(null));
    }
}
