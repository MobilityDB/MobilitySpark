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
 * Unit tests for TransformUDFs set and span transforms:
 *   floatset (ceil/floor/round/degrees/radians),
 *   textset (lower/upper/initcap),
 *   intspan/floatspan shift-scale,
 *   intspanset/floatspanset shift-scale, ceil, floor, round, type conversions.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TransformUDFsExt4Test {

    private static String FLOATSET_HEX;
    private static String TEXTSET_HEX;
    private static String INTSPAN_HEX;
    private static String FLOATSPAN_HEX;
    private static String INTSPANSET_HEX;
    private static String FLOATSPANSET_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        FLOATSET_HEX     = set_as_hexwkb(floatset_in("{1.1, 2.2, 3.3}"), (byte) 0);
        TEXTSET_HEX      = set_as_hexwkb(textset_in("{\"apple\", \"BANANA\", \"Cherry\"}"), (byte) 0);
        INTSPAN_HEX      = span_as_hexwkb(intspan_in("[1, 10]"), (byte) 0);
        FLOATSPAN_HEX    = span_as_hexwkb(floatspan_in("[1.5, 10.5]"), (byte) 0);
        INTSPANSET_HEX   = spanset_as_hexwkb(intspanset_in("{[1, 5], [10, 20]}"), (byte) 0);
        FLOATSPANSET_HEX = spanset_as_hexwkb(floatspanset_in("{[1.5, 5.5], [10.0, 20.0]}"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // floatset transforms
    // ------------------------------------------------------------------

    @Test @Order(1)
    void floatsetCeil_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatsetCeil.call(FLOATSET_HEX);
        assertNotNull(r, "floatsetCeil must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void floatsetFloor_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatsetFloor.call(FLOATSET_HEX);
        assertNotNull(r, "floatsetFloor must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void floatsetDegrees_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatsetDegrees.call(FLOATSET_HEX);
        assertNotNull(r, "floatsetDegrees must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(5)
    void floatsetRadians_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatsetRadians.call(FLOATSET_HEX);
        assertNotNull(r, "floatsetRadians must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void floatsetCeil_null_returns_null() throws Exception {
        assertNull(TransformUDFs.floatsetCeil.call(null));
    }

    // ------------------------------------------------------------------
    // textset case normalization
    // ------------------------------------------------------------------

    @Test @Order(7)
    void textsetLower_returns_nonnull() throws Exception {
        String r = TransformUDFs.textsetLower.call(TEXTSET_HEX);
        assertNotNull(r, "textsetLower must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(8)
    void textsetUpper_returns_nonnull() throws Exception {
        String r = TransformUDFs.textsetUpper.call(TEXTSET_HEX);
        assertNotNull(r, "textsetUpper must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void textsetInitcap_returns_nonnull() throws Exception {
        String r = TransformUDFs.textsetInitcap.call(TEXTSET_HEX);
        assertNotNull(r, "textsetInitcap must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(10)
    void textsetLower_null_returns_null() throws Exception {
        assertNull(TransformUDFs.textsetLower.call(null));
    }

    // ------------------------------------------------------------------
    // intspan / floatspan shift-scale
    // ------------------------------------------------------------------

    @Test @Order(11)
    void intspanShiftScale_shift_only_returns_nonnull() throws Exception {
        String r = TransformUDFs.intspanShiftScale.call(INTSPAN_HEX, 5, null);
        assertNotNull(r, "intspanShiftScale (shift only) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(12)
    void intspanShiftScale_scale_only_returns_nonnull() throws Exception {
        String r = TransformUDFs.intspanShiftScale.call(INTSPAN_HEX, null, 20);
        assertNotNull(r, "intspanShiftScale (scale only) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(13)
    void intspanShiftScale_null_span_returns_null() throws Exception {
        assertNull(TransformUDFs.intspanShiftScale.call(null, 5, 10));
    }

    @Test @Order(14)
    void floatspanShiftScale_shift_only_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatspanShiftScale.call(FLOATSPAN_HEX, 2.5, null);
        assertNotNull(r, "floatspanShiftScale (shift only) must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(15)
    void floatspanShiftScale_null_span_returns_null() throws Exception {
        assertNull(TransformUDFs.floatspanShiftScale.call(null, 1.0, 5.0));
    }

    // ------------------------------------------------------------------
    // intspanset / floatspanset transforms
    // ------------------------------------------------------------------

    @Test @Order(16)
    void intspansetShiftScale_returns_nonnull() throws Exception {
        String r = TransformUDFs.intspansetShiftScale.call(INTSPANSET_HEX, 10, null);
        assertNotNull(r, "intspansetShiftScale must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(17)
    void floatspansetShiftScale_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatspansetShiftScale.call(FLOATSPANSET_HEX, 1.0, null);
        assertNotNull(r, "floatspansetShiftScale must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(18)
    void floatspansetCeil_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatspansetCeil.call(FLOATSPANSET_HEX);
        assertNotNull(r, "floatspansetCeil must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(19)
    void floatspansetFloor_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatspansetFloor.call(FLOATSPANSET_HEX);
        assertNotNull(r, "floatspansetFloor must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(20)
    void floatspansetRound_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatspansetRound.call(FLOATSPANSET_HEX, 1);
        assertNotNull(r, "floatspansetRound must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(21)
    void intspansetToFloat_returns_nonnull() throws Exception {
        String r = TransformUDFs.intspansetToFloat.call(INTSPANSET_HEX);
        assertNotNull(r, "intspansetToFloat must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(22)
    void floatspansetToInt_returns_nonnull() throws Exception {
        String r = TransformUDFs.floatspansetToInt.call(FLOATSPANSET_HEX);
        assertNotNull(r, "floatspansetToInt must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(23)
    void floatspansetCeil_null_returns_null() throws Exception {
        assertNull(TransformUDFs.floatspansetCeil.call(null));
    }
}
