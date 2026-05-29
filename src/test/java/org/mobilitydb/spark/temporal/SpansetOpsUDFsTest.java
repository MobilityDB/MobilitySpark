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
 * Unit tests for cross-type Span × Spanset positional/topological UDFs.
 *
 * Fixtures (all floatspan / floatspanset for arithmetic semantics):
 *   SPAN_LO     — floatspan [0, 5)
 *   SPAN_HI     — floatspan [10, 15)
 *   SPANSET_LO  — floatspanset { [0,5), [6,8) }
 *   SPANSET_HI  — floatspanset { [10,12), [13,15) }
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpansetOpsUDFsTest extends MeosTestBase {

    private static String SPAN_LO;
    private static String SPAN_HI;
    private static String SPANSET_LO;
    private static String SPANSET_HI;

    @BeforeAll
    static void initMeos() {
        SPAN_LO = span_as_hexwkb(floatspan_in("[0, 5)"), (byte) 0);
        SPAN_HI = span_as_hexwkb(floatspan_in("[10, 15)"), (byte) 0);
        SPANSET_LO = spanset_as_hexwkb(floatspanset_in("{[0,5), [6,8)}"), (byte) 0);
        SPANSET_HI = spanset_as_hexwkb(floatspanset_in("{[10,12), [13,15)}"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // Null guards
    // ------------------------------------------------------------------

    @Test @Order(1)
    void spanLeftSpanset_null_returns_null() throws Exception {
        assertNull(SpansetOpsUDFs.spanLeftSpanset.call(null, SPANSET_HI));
        assertNull(SpansetOpsUDFs.spanLeftSpanset.call(SPAN_LO, null));
    }

    @Test @Order(2)
    void spansetLeftSpan_null_returns_null() throws Exception {
        assertNull(SpansetOpsUDFs.spansetLeftSpan.call(null, SPAN_HI));
        assertNull(SpansetOpsUDFs.spansetLeftSpan.call(SPANSET_LO, null));
    }

    @Test @Order(3)
    void spansetLeftSpanset_null_returns_null() throws Exception {
        assertNull(SpansetOpsUDFs.spansetLeftSpanset.call(null, SPANSET_HI));
        assertNull(SpansetOpsUDFs.spansetLeftSpanset.call(SPANSET_LO, null));
    }

    // ------------------------------------------------------------------
    // span × spanset
    // ------------------------------------------------------------------

    @Test @Order(4)
    void spanLeftSpanset_low_left_of_high() throws Exception {
        assertTrue(SpansetOpsUDFs.spanLeftSpanset.call(SPAN_LO, SPANSET_HI));
    }

    @Test @Order(5)
    void spanLeftSpanset_high_not_left_of_low() throws Exception {
        assertFalse(SpansetOpsUDFs.spanLeftSpanset.call(SPAN_HI, SPANSET_LO));
    }

    @Test @Order(6)
    void spanRightSpanset_high_right_of_low() throws Exception {
        assertTrue(SpansetOpsUDFs.spanRightSpanset.call(SPAN_HI, SPANSET_LO));
    }

    @Test @Order(7)
    void spanOverlapsSpanset_overlap_returns_true() throws Exception {
        assertTrue(SpansetOpsUDFs.spanOverlapsSpanset.call(SPAN_LO, SPANSET_LO));
    }

    @Test @Order(8)
    void spanOverlapsSpanset_disjoint_returns_false() throws Exception {
        assertFalse(SpansetOpsUDFs.spanOverlapsSpanset.call(SPAN_LO, SPANSET_HI));
    }

    @Test @Order(9)
    void spanContainsSpanset_returns_nonnull() throws Exception {
        assertNotNull(SpansetOpsUDFs.spanContainsSpanset.call(SPAN_LO, SPANSET_LO));
    }

    @Test @Order(10)
    void spanContainedSpanset_self_returns_true() throws Exception {
        assertTrue(SpansetOpsUDFs.spanContainedSpanset.call(SPAN_LO, SPANSET_LO),
            "[0,5) is contained in {[0,5), [6,8)}");
    }

    @Test @Order(11)
    void spanAdjacentSpanset_returns_nonnull() throws Exception {
        assertNotNull(SpansetOpsUDFs.spanAdjacentSpanset.call(SPAN_LO, SPANSET_HI));
    }

    @Test @Order(12)
    void spanOverleftSpanset_low_overleft_of_high() throws Exception {
        assertTrue(SpansetOpsUDFs.spanOverleftSpanset.call(SPAN_LO, SPANSET_HI));
    }

    // ------------------------------------------------------------------
    // spanset × span
    // ------------------------------------------------------------------

    @Test @Order(13)
    void spansetLeftSpan_low_left_of_high() throws Exception {
        assertTrue(SpansetOpsUDFs.spansetLeftSpan.call(SPANSET_LO, SPAN_HI));
    }

    @Test @Order(14)
    void spansetRightSpan_high_right_of_low() throws Exception {
        assertTrue(SpansetOpsUDFs.spansetRightSpan.call(SPANSET_HI, SPAN_LO));
    }

    @Test @Order(15)
    void spansetOverlapsSpan_overlap_returns_true() throws Exception {
        assertTrue(SpansetOpsUDFs.spansetOverlapsSpan.call(SPANSET_LO, SPAN_LO));
    }

    @Test @Order(16)
    void spansetOverlapsSpan_disjoint_returns_false() throws Exception {
        assertFalse(SpansetOpsUDFs.spansetOverlapsSpan.call(SPANSET_LO, SPAN_HI));
    }

    @Test @Order(17)
    void spansetContainedSpan_returns_nonnull() throws Exception {
        assertNotNull(SpansetOpsUDFs.spansetContainedSpan.call(SPANSET_LO, SPAN_LO));
    }

    @Test @Order(18)
    void spansetAdjacentSpan_returns_nonnull() throws Exception {
        assertNotNull(SpansetOpsUDFs.spansetAdjacentSpan.call(SPANSET_LO, SPAN_HI));
    }

    @Test @Order(19)
    void spansetOverleftSpan_returns_nonnull() throws Exception {
        assertNotNull(SpansetOpsUDFs.spansetOverleftSpan.call(SPANSET_LO, SPAN_HI));
    }

    @Test @Order(20)
    void spansetOverrightSpan_returns_nonnull() throws Exception {
        assertNotNull(SpansetOpsUDFs.spansetOverrightSpan.call(SPANSET_HI, SPAN_LO));
    }

    // ------------------------------------------------------------------
    // spanset × spanset
    // ------------------------------------------------------------------

    @Test @Order(21)
    void spansetLeftSpanset_low_left_of_high() throws Exception {
        assertTrue(SpansetOpsUDFs.spansetLeftSpanset.call(SPANSET_LO, SPANSET_HI));
    }

    @Test @Order(22)
    void spansetRightSpanset_high_right_of_low() throws Exception {
        assertTrue(SpansetOpsUDFs.spansetRightSpanset.call(SPANSET_HI, SPANSET_LO));
    }

    @Test @Order(23)
    void spansetContainsSpanset_self_returns_true() throws Exception {
        assertTrue(SpansetOpsUDFs.spansetContainsSpanset.call(SPANSET_LO, SPANSET_LO));
    }

    @Test @Order(24)
    void spansetContainedSpanset_self_returns_true() throws Exception {
        assertTrue(SpansetOpsUDFs.spansetContainedSpanset.call(SPANSET_LO, SPANSET_LO));
    }

    @Test @Order(25)
    void spansetAdjacentSpanset_returns_nonnull() throws Exception {
        assertNotNull(SpansetOpsUDFs.spansetAdjacentSpanset.call(SPANSET_LO, SPANSET_HI));
    }

    @Test @Order(26)
    void spansetOverleftSpanset_low_overleft_of_high() throws Exception {
        assertTrue(SpansetOpsUDFs.spansetOverleftSpanset.call(SPANSET_LO, SPANSET_HI));
    }

    @Test @Order(27)
    void spansetOverrightSpanset_high_overright_of_low() throws Exception {
        assertTrue(SpansetOpsUDFs.spansetOverrightSpanset.call(SPANSET_HI, SPANSET_LO));
    }
}
