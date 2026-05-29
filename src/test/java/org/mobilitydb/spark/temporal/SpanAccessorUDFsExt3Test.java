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
 * Unit tests for intspanset/floatspanset bound accessors:
 *   intspansetLower/Upper/Width, floatspansetLower/Upper/Width.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpanAccessorUDFsExt3Test extends MeosTestBase {

    private static String INTSPANSET_HEX;
    private static String FLOATSPANSET_HEX;

    @BeforeAll
    static void initMeos() {
        INTSPANSET_HEX   = spanset_as_hexwkb(intspanset_in("{[1, 5], [10, 20]}"), (byte) 0);
        FLOATSPANSET_HEX = spanset_as_hexwkb(floatspanset_in("{[1.0, 5.0], [10.0, 20.0]}"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // intspanset bounds
    // ------------------------------------------------------------------

    @Test @Order(1)
    void intspansetLower_returns_first_lower_bound() throws Exception {
        Integer v = SpanAccessorUDFs.intspansetLower.call(INTSPANSET_HEX);
        assertNotNull(v);
        assertEquals(1, v.intValue());
    }

    @Test @Order(2)
    void intspansetUpper_returns_nonnull() throws Exception {
        // Integer spans store exclusive upper bound internally;
        // [10, 20] is stored as upper = 21.
        Integer v = SpanAccessorUDFs.intspansetUpper.call(INTSPANSET_HEX);
        assertNotNull(v, "intspansetUpper must return non-null");
    }

    @Test @Order(3)
    void intspansetWidth_ignoreGaps_false_returns_nonnull() throws Exception {
        Integer w = SpanAccessorUDFs.intspansetWidth.call(INTSPANSET_HEX, false);
        assertNotNull(w, "intspansetWidth must return non-null");
    }

    @Test @Order(4)
    void intspansetWidth_ignoreGaps_true_returns_nonnull() throws Exception {
        Integer w = SpanAccessorUDFs.intspansetWidth.call(INTSPANSET_HEX, true);
        assertNotNull(w, "intspansetWidth(ignoreGaps=true) must return non-null");
    }

    @Test @Order(5)
    void intspansetLower_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.intspansetLower.call(null));
    }

    // ------------------------------------------------------------------
    // floatspanset bounds
    // ------------------------------------------------------------------

    @Test @Order(6)
    void floatspansetLower_returns_first_lower_bound() throws Exception {
        Double v = SpanAccessorUDFs.floatspansetLower.call(FLOATSPANSET_HEX);
        assertNotNull(v);
        assertEquals(1.0, v, 1e-9);
    }

    @Test @Order(7)
    void floatspansetUpper_returns_last_upper_bound() throws Exception {
        Double v = SpanAccessorUDFs.floatspansetUpper.call(FLOATSPANSET_HEX);
        assertNotNull(v);
        assertEquals(20.0, v, 1e-9);
    }

    @Test @Order(8)
    void floatspansetWidth_returns_nonnull() throws Exception {
        Double w = SpanAccessorUDFs.floatspansetWidth.call(FLOATSPANSET_HEX, false);
        assertNotNull(w, "floatspansetWidth must return non-null");
    }

    @Test @Order(9)
    void floatspansetLower_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.floatspansetLower.call(null));
    }
}
