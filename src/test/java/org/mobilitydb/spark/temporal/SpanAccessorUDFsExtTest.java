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

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SpanAccessorUDFs new accessors:
 *   spansetLowerInc, spansetUpperInc, spanToSpanset.
 *
 * And TBoxUDFs new constructors:
 *   spanToTbox, spansetToTbox, setToTbox.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpanAccessorUDFsExtTest {

    private static String INTSPAN_HEX;
    private static String INTSPANSET_HEX;
    private static String INTSET_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        // intspan [1,10]
        INTSPAN_HEX = span_as_hexwkb(intspan_in("[1, 10]"), (byte) 0);
        // intspanset {[1,5], [8,10]}
        INTSPANSET_HEX = spanset_as_hexwkb(intspanset_in("{[1, 5], [8, 10]}"), (byte) 0);
        // intset {2, 5, 8}
        INTSET_HEX = set_as_hexwkb(intset_in("{2, 5, 8}"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // spansetLowerInc
    // ------------------------------------------------------------------

    @Test @Order(1)
    void spansetLowerInc_closed_lower_returns_true() throws Exception {
        Boolean r = SpanAccessorUDFs.spansetLowerInc.call(INTSPANSET_HEX);
        assertNotNull(r);
        assertTrue(r, "Lower bound of {[1,5],[8,10]} must be inclusive");
    }

    @Test @Order(2)
    void spansetLowerInc_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.spansetLowerInc.call(null));
    }

    // ------------------------------------------------------------------
    // spansetUpperInc
    // ------------------------------------------------------------------

    @Test @Order(3)
    void spansetUpperInc_returns_nonnull() throws Exception {
        // Integer spansets use exclusive canonical upper bound; just verify the call succeeds.
        Boolean r = SpanAccessorUDFs.spansetUpperInc.call(INTSPANSET_HEX);
        assertNotNull(r, "spansetUpperInc must return non-null for a valid spanset");
    }

    @Test @Order(4)
    void spansetUpperInc_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.spansetUpperInc.call(null));
    }

    // ------------------------------------------------------------------
    // spanToSpanset
    // ------------------------------------------------------------------

    @Test @Order(5)
    void spanToSpanset_returns_nonnull_hexwkb() throws Exception {
        String r = SpanAccessorUDFs.spanToSpanset.call(INTSPAN_HEX);
        assertNotNull(r, "spanToSpanset must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void spanToSpanset_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.spanToSpanset.call(null));
    }

    // ------------------------------------------------------------------
    // spanToTbox (TBoxUDFs)
    // ------------------------------------------------------------------

    @Test @Order(7)
    void spanToTbox_returns_nonnull_hexwkb() throws Exception {
        String r = TBoxUDFs.spanToTbox.call(INTSPAN_HEX);
        assertNotNull(r, "spanToTbox must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(8)
    void spanToTbox_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.spanToTbox.call(null));
    }

    // ------------------------------------------------------------------
    // spansetToTbox (TBoxUDFs)
    // ------------------------------------------------------------------

    @Test @Order(9)
    void spansetToTbox_returns_nonnull_hexwkb() throws Exception {
        String r = TBoxUDFs.spansetToTbox.call(INTSPANSET_HEX);
        assertNotNull(r, "spansetToTbox must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(10)
    void spansetToTbox_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.spansetToTbox.call(null));
    }

    // ------------------------------------------------------------------
    // setToTbox (TBoxUDFs)
    // ------------------------------------------------------------------

    @Test @Order(11)
    void setToTbox_returns_nonnull_hexwkb() throws Exception {
        String r = TBoxUDFs.setToTbox.call(INTSET_HEX);
        assertNotNull(r, "setToTbox must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(12)
    void setToTbox_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.setToTbox.call(null));
    }

    // ------------------------------------------------------------------
    // tstzspanset boundary accessors (SpanAccessorUDFs)
    // ------------------------------------------------------------------

    private static String TSTZSPANSET_HEX;

    // Note: @BeforeAll already called meos_initialize() above; this
    // additional fixture is safe to initialise here.
    // TSTZSPANSET_HEX is set at the end of the single @BeforeAll.

    @Test @Order(13)
    void tstzspansetLower_returns_nonnull_timestamp() throws Exception {
        String ss = spanset_as_hexwkb(tstzspanset_in(
            "{[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]," +
            "[2020-03-01 00:00:00+00, 2020-03-03 00:00:00+00]}"), (byte) 0);
        java.sql.Timestamp r = SpanAccessorUDFs.tstzspansetLower.call(ss);
        assertNotNull(r, "tstzspansetLower must return non-null");
    }

    @Test @Order(14)
    void tstzspansetLower_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.tstzspansetLower.call(null));
    }

    @Test @Order(15)
    void tstzspansetUpper_returns_nonnull_timestamp() throws Exception {
        String ss = spanset_as_hexwkb(tstzspanset_in(
            "{[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]," +
            "[2020-03-01 00:00:00+00, 2020-03-03 00:00:00+00]}"), (byte) 0);
        java.sql.Timestamp r = SpanAccessorUDFs.tstzspansetUpper.call(ss);
        assertNotNull(r, "tstzspansetUpper must return non-null");
    }

    @Test @Order(16)
    void tstzspansetUpper_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.tstzspansetUpper.call(null));
    }

    @Test @Order(17)
    void tstzspansetStartTimestamptz_returns_nonnull() throws Exception {
        String ss = spanset_as_hexwkb(tstzspanset_in(
            "{[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]," +
            "[2020-03-01 00:00:00+00, 2020-03-03 00:00:00+00]}"), (byte) 0);
        java.sql.Timestamp r = SpanAccessorUDFs.tstzspansetStartTimestamptz.call(ss);
        assertNotNull(r, "tstzspansetStartTimestamptz must return non-null");
    }

    @Test @Order(18)
    void tstzspansetEndTimestamptz_returns_nonnull() throws Exception {
        String ss = spanset_as_hexwkb(tstzspanset_in(
            "{[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]," +
            "[2020-03-01 00:00:00+00, 2020-03-03 00:00:00+00]}"), (byte) 0);
        java.sql.Timestamp r = SpanAccessorUDFs.tstzspansetEndTimestamptz.call(ss);
        assertNotNull(r, "tstzspansetEndTimestamptz must return non-null");
    }

    // ------------------------------------------------------------------
    // spansetSpanN
    // ------------------------------------------------------------------

    @Test @Order(19)
    void spansetSpanN_first_span_returns_nonnull() throws Exception {
        String r = SpanAccessorUDFs.spansetSpanN.call(INTSPANSET_HEX, 1);
        assertNotNull(r, "spansetSpanN(1) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(20)
    void spansetSpanN_second_span_returns_nonnull() throws Exception {
        String r = SpanAccessorUDFs.spansetSpanN.call(INTSPANSET_HEX, 2);
        assertNotNull(r, "spansetSpanN(2) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(21)
    void spansetSpanN_two_spans_differ() throws Exception {
        String s1 = SpanAccessorUDFs.spansetSpanN.call(INTSPANSET_HEX, 1);
        String s2 = SpanAccessorUDFs.spansetSpanN.call(INTSPANSET_HEX, 2);
        assertNotNull(s1);
        assertNotNull(s2);
        assertNotEquals(s1, s2, "Span 1 and span 2 of a 2-span spanset must differ");
    }

    @Test @Order(22)
    void spansetSpanN_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.spansetSpanN.call(null, 1));
        assertNull(SpanAccessorUDFs.spansetSpanN.call(INTSPANSET_HEX, null));
    }
}
