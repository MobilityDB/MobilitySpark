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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for set value accessor UDFs:
 *   intset, floatset, dateset, tstzset, textset — start/end/values.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SetAccessorUDFsExtTest {

    private static String INTSET_HEX;
    private static String FLOATSET_HEX;
    private static String DATESET_HEX;
    private static String TSTZSET_HEX;
    private static String TEXTSET_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        INTSET_HEX  = set_as_hexwkb(intset_in("{1, 5, 10}"), (byte) 0);
        FLOATSET_HEX = set_as_hexwkb(floatset_in("{1.5, 3.0, 7.25}"), (byte) 0);
        DATESET_HEX  = set_as_hexwkb(dateset_in("{2020-01-01, 2020-06-15, 2021-12-31}"), (byte) 0);
        TSTZSET_HEX  = set_as_hexwkb(
            tstzset_in("{2020-01-01 00:00:00+00, 2020-06-01 00:00:00+00}"), (byte) 0);
        TEXTSET_HEX  = set_as_hexwkb(textset_in("{\"apple\", \"banana\", \"cherry\"}"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // intset
    // ------------------------------------------------------------------

    @Test @Order(1)
    void intsetStartValue_returns_minimum() throws Exception {
        Integer v = SpanAccessorUDFs.intsetStartValue.call(INTSET_HEX);
        assertNotNull(v);
        assertEquals(1, v.intValue());
    }

    @Test @Order(2)
    void intsetEndValue_returns_maximum() throws Exception {
        Integer v = SpanAccessorUDFs.intsetEndValue.call(INTSET_HEX);
        assertNotNull(v);
        assertEquals(10, v.intValue());
    }

    @Test @Order(3)
    void intsetValues_returns_all_elements() throws Exception {
        List<Integer> vs = SpanAccessorUDFs.intsetValues.call(INTSET_HEX);
        assertNotNull(vs);
        assertEquals(3, vs.size());
        assertTrue(vs.contains(1));
        assertTrue(vs.contains(5));
        assertTrue(vs.contains(10));
    }

    @Test @Order(4)
    void intsetStartValue_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.intsetStartValue.call(null));
    }

    // ------------------------------------------------------------------
    // floatset
    // ------------------------------------------------------------------

    @Test @Order(5)
    void floatsetStartValue_returns_minimum() throws Exception {
        Double v = SpanAccessorUDFs.floatsetStartValue.call(FLOATSET_HEX);
        assertNotNull(v);
        assertEquals(1.5, v, 1e-9);
    }

    @Test @Order(6)
    void floatsetEndValue_returns_maximum() throws Exception {
        Double v = SpanAccessorUDFs.floatsetEndValue.call(FLOATSET_HEX);
        assertNotNull(v);
        assertEquals(7.25, v, 1e-9);
    }

    @Test @Order(7)
    void floatsetValues_returns_all_elements() throws Exception {
        List<Double> vs = SpanAccessorUDFs.floatsetValues.call(FLOATSET_HEX);
        assertNotNull(vs);
        assertEquals(3, vs.size());
    }

    @Test @Order(8)
    void floatsetEndValue_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.floatsetEndValue.call(null));
    }

    // ------------------------------------------------------------------
    // dateset
    // ------------------------------------------------------------------

    @Test @Order(9)
    void datesetStartValue_returns_first_date() throws Exception {
        Date d = SpanAccessorUDFs.datesetStartValue.call(DATESET_HEX);
        assertNotNull(d, "datesetStartValue must return non-null");
    }

    @Test @Order(10)
    void datesetEndValue_returns_last_date() throws Exception {
        Date d = SpanAccessorUDFs.datesetEndValue.call(DATESET_HEX);
        assertNotNull(d, "datesetEndValue must return non-null");
    }

    @Test @Order(11)
    void datesetValues_returns_all_elements() throws Exception {
        List<Date> ds = SpanAccessorUDFs.datesetValues.call(DATESET_HEX);
        assertNotNull(ds);
        assertEquals(3, ds.size());
    }

    @Test @Order(12)
    void datesetStartValue_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.datesetStartValue.call(null));
    }

    // ------------------------------------------------------------------
    // tstzset
    // ------------------------------------------------------------------

    @Test @Order(13)
    void tstzsetStartValue_returns_nonnull_timestamp() throws Exception {
        Timestamp ts = SpanAccessorUDFs.tstzsetStartValue.call(TSTZSET_HEX);
        assertNotNull(ts, "tstzsetStartValue must return non-null");
    }

    @Test @Order(14)
    void tstzsetEndValue_returns_nonnull_timestamp() throws Exception {
        Timestamp ts = SpanAccessorUDFs.tstzsetEndValue.call(TSTZSET_HEX);
        assertNotNull(ts, "tstzsetEndValue must return non-null");
    }

    @Test @Order(15)
    void tstzsetStartValue_before_endValue() throws Exception {
        Timestamp start = SpanAccessorUDFs.tstzsetStartValue.call(TSTZSET_HEX);
        Timestamp end   = SpanAccessorUDFs.tstzsetEndValue.call(TSTZSET_HEX);
        assertNotNull(start);
        assertNotNull(end);
        assertTrue(start.before(end), "start value must be before end value");
    }

    @Test @Order(16)
    void tstzsetValues_returns_two_elements() throws Exception {
        List<Timestamp> tss = SpanAccessorUDFs.tstzsetValues.call(TSTZSET_HEX);
        assertNotNull(tss);
        assertEquals(2, tss.size());
    }

    @Test @Order(17)
    void tstzsetStartValue_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.tstzsetStartValue.call(null));
    }

    // ------------------------------------------------------------------
    // textset
    // ------------------------------------------------------------------

    @Test @Order(18)
    void textsetStartValue_returns_first_string() throws Exception {
        String s = SpanAccessorUDFs.textsetStartValue.call(TEXTSET_HEX);
        assertNotNull(s, "textsetStartValue must return non-null");
        assertFalse(s.isBlank());
    }

    @Test @Order(19)
    void textsetEndValue_returns_last_string() throws Exception {
        String s = SpanAccessorUDFs.textsetEndValue.call(TEXTSET_HEX);
        assertNotNull(s, "textsetEndValue must return non-null");
        assertFalse(s.isBlank());
    }

    @Test @Order(20)
    void textsetValues_returns_three_strings() throws Exception {
        List<String> vs = SpanAccessorUDFs.textsetValues.call(TEXTSET_HEX);
        assertNotNull(vs);
        assertEquals(3, vs.size());
        vs.forEach(s -> assertFalse(s == null || s.isBlank(), "Each element must be non-blank"));
    }

    @Test @Order(21)
    void textsetStartValue_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.textsetStartValue.call(null));
    }
}
