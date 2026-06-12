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
 * Unit tests for tint value-domain shift/scale UDFs:
 *   tintShiftValue, tintScaleValue, tintShiftScaleValue.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TransformUDFsExt3Test {

    private static String TINT_SEQ;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TINT_SEQ = temporal_as_hexwkb(
            tint_in("[1@2020-01-01, 3@2020-01-03]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // tintShiftValue
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tintShiftValue_positive_shift_returns_nonnull() throws Exception {
        String r = TransformUDFs.tintShiftValue.call(TINT_SEQ, 10);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tintShiftValue_changes_start_value() throws Exception {
        String r = TransformUDFs.tintShiftValue.call(TINT_SEQ, 5);
        assertNotNull(r);
        Integer sv = AccessorUDFs.tintStartValue.call(r);
        assertNotNull(sv);
        assertEquals(6, sv.intValue());
    }

    @Test @Order(3)
    void tintShiftValue_negative_shift_returns_nonnull() throws Exception {
        String r = TransformUDFs.tintShiftValue.call(TINT_SEQ, -1);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void tintShiftValue_null_returns_null() throws Exception {
        assertNull(TransformUDFs.tintShiftValue.call(null, 5));
        assertNull(TransformUDFs.tintShiftValue.call(TINT_SEQ, null));
    }

    // ------------------------------------------------------------------
    // tintScaleValue
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tintScaleValue_doubles_start_value() throws Exception {
        String r = TransformUDFs.tintScaleValue.call(TINT_SEQ, 2);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void tintScaleValue_by_one_is_identity() throws Exception {
        String r = TransformUDFs.tintScaleValue.call(TINT_SEQ, 1);
        assertNotNull(r);
        Integer sv = AccessorUDFs.tintStartValue.call(r);
        assertNotNull(sv);
        assertEquals(1, sv.intValue());
    }

    @Test @Order(7)
    void tintScaleValue_null_returns_null() throws Exception {
        assertNull(TransformUDFs.tintScaleValue.call(null, 2));
        assertNull(TransformUDFs.tintScaleValue.call(TINT_SEQ, null));
    }

    // ------------------------------------------------------------------
    // tintShiftScaleValue
    // ------------------------------------------------------------------

    @Test @Order(8)
    void tintShiftScaleValue_shift_then_scale_returns_nonnull() throws Exception {
        String r = TransformUDFs.tintShiftScaleValue.call(TINT_SEQ, 1, 3);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void tintShiftScaleValue_null_returns_null() throws Exception {
        assertNull(TransformUDFs.tintShiftScaleValue.call(null, 1, 2));
        assertNull(TransformUDFs.tintShiftScaleValue.call(TINT_SEQ, null, 2));
        assertNull(TransformUDFs.tintShiftScaleValue.call(TINT_SEQ, 1, null));
    }
}
