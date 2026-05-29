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
 * Unit tests for the new PredicateUDFs:
 *   - scalar-first reversed forms (int/float OP tint/tfloat)
 *   - tbool × bool predicates
 *   - ttext × text predicates (both directions)
 *
 * Fixtures:
 *   TINT5_HEX   — constant tint 5 at a single instant
 *   TFLOAT2_HEX — constant tfloat 2.0 at a single instant
 *   TBOOL_HEX   — constant tbool true at a single instant
 *   TTEXT_HEX   — constant ttext "hello" at a single instant
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PredicateUDFsExt2Test extends MeosTestBase {

    private static String TINT5_HEX;
    private static String TFLOAT2_HEX;
    private static String TBOOL_HEX;
    private static String TTEXT_HEX;

    @BeforeAll
    static void initMeos() {
        TINT5_HEX   = temporal_as_hexwkb(tint_in("5@2020-01-01 00:00:00+00"),    (byte) 0);
        TFLOAT2_HEX = temporal_as_hexwkb(tfloat_in("2.0@2020-01-01 00:00:00+00"), (byte) 0);
        TBOOL_HEX   = temporal_as_hexwkb(tbool_in("true@2020-01-01 00:00:00+00"), (byte) 0);
        TTEXT_HEX   = temporal_as_hexwkb(ttext_in("hello@2020-01-01 00:00:00+00"),(byte) 0);
    }

    // ------------------------------------------------------------------
    // scalar-first int OP tint
    // ------------------------------------------------------------------

    @Test @Order(1)
    void alwaysEqIntTint_matching_value_returns_true() throws Exception {
        assertTrue(PredicateUDFs.alwaysEqIntTint.call(5, TINT5_HEX));
    }

    @Test @Order(2)
    void alwaysEqIntTint_non_matching_returns_false() throws Exception {
        assertFalse(PredicateUDFs.alwaysEqIntTint.call(9, TINT5_HEX));
    }

    @Test @Order(3)
    void everEqIntTint_matching_value_returns_true() throws Exception {
        assertTrue(PredicateUDFs.everEqIntTint.call(5, TINT5_HEX));
    }

    @Test @Order(4)
    void everLtIntTint_value_less_than_tint_returns_true() throws Exception {
        // 3 < tint(5) → true
        assertTrue(PredicateUDFs.everLtIntTint.call(3, TINT5_HEX));
    }

    @Test @Order(5)
    void alwaysGtIntTint_value_greater_than_tint_returns_true() throws Exception {
        // 9 > tint(5) → always true
        assertTrue(PredicateUDFs.alwaysGtIntTint.call(9, TINT5_HEX));
    }

    @Test @Order(6)
    void alwaysEqIntTint_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.alwaysEqIntTint.call(null, TINT5_HEX));
        assertNull(PredicateUDFs.alwaysEqIntTint.call(5, null));
    }

    // ------------------------------------------------------------------
    // scalar-first float OP tfloat
    // ------------------------------------------------------------------

    @Test @Order(7)
    void alwaysEqFloatTfloat_matching_value_returns_true() throws Exception {
        assertTrue(PredicateUDFs.alwaysEqFloatTfloat.call(2.0, TFLOAT2_HEX));
    }

    @Test @Order(8)
    void everLtFloatTfloat_value_less_than_tfloat_returns_true() throws Exception {
        // 1.0 < tfloat(2.0) → ever true
        assertTrue(PredicateUDFs.everLtFloatTfloat.call(1.0, TFLOAT2_HEX));
    }

    @Test @Order(9)
    void alwaysGeFloatTfloat_value_ge_tfloat_returns_true() throws Exception {
        // 2.0 >= tfloat(2.0) → always true
        assertTrue(PredicateUDFs.alwaysGeFloatTfloat.call(2.0, TFLOAT2_HEX));
    }

    @Test @Order(10)
    void alwaysEqFloatTfloat_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.alwaysEqFloatTfloat.call(null, TFLOAT2_HEX));
        assertNull(PredicateUDFs.alwaysEqFloatTfloat.call(2.0, null));
    }

    // ------------------------------------------------------------------
    // tbool × bool predicates
    // ------------------------------------------------------------------

    @Test @Order(11)
    void alwaysEqTboolBool_true_value_returns_true() throws Exception {
        assertTrue(PredicateUDFs.alwaysEqTboolBool.call(TBOOL_HEX, true));
    }

    @Test @Order(12)
    void alwaysEqTboolBool_false_value_returns_false() throws Exception {
        assertFalse(PredicateUDFs.alwaysEqTboolBool.call(TBOOL_HEX, false));
    }

    @Test @Order(13)
    void everEqBoolTbool_true_value_returns_true() throws Exception {
        assertTrue(PredicateUDFs.everEqBoolTbool.call(true, TBOOL_HEX));
    }

    @Test @Order(14)
    void alwaysNeTboolBool_different_value_returns_true() throws Exception {
        assertTrue(PredicateUDFs.alwaysNeTboolBool.call(TBOOL_HEX, false));
    }

    @Test @Order(15)
    void everNeBoolTbool_different_value_returns_true() throws Exception {
        assertTrue(PredicateUDFs.everNeBoolTbool.call(false, TBOOL_HEX));
    }

    @Test @Order(16)
    void alwaysEqTboolBool_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.alwaysEqTboolBool.call(null, true));
        assertNull(PredicateUDFs.alwaysEqTboolBool.call(TBOOL_HEX, null));
    }

    // ------------------------------------------------------------------
    // ttext × text predicates (ttext OP text)
    // ------------------------------------------------------------------

    @Test @Order(17)
    void alwaysEqTtextText_matching_text_returns_true() throws Exception {
        assertTrue(PredicateUDFs.alwaysEqTtextText.call(TTEXT_HEX, "hello"));
    }

    @Test @Order(18)
    void alwaysEqTtextText_non_matching_returns_false() throws Exception {
        assertFalse(PredicateUDFs.alwaysEqTtextText.call(TTEXT_HEX, "world"));
    }

    @Test @Order(19)
    void everEqTtextText_matching_returns_true() throws Exception {
        assertTrue(PredicateUDFs.everEqTtextText.call(TTEXT_HEX, "hello"));
    }

    @Test @Order(20)
    void alwaysLtTtextText_hello_lt_xyz_returns_true() throws Exception {
        // ttext(hello) < "xyz" → always true
        assertTrue(PredicateUDFs.alwaysLtTtextText.call(TTEXT_HEX, "xyz"));
    }

    @Test @Order(21)
    void alwaysGtTtextText_hello_gt_abc_returns_true() throws Exception {
        // ttext(hello) > "abc" → always true
        assertTrue(PredicateUDFs.alwaysGtTtextText.call(TTEXT_HEX, "abc"));
    }

    @Test @Order(22)
    void alwaysEqTtextText_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.alwaysEqTtextText.call(null, "hello"));
        assertNull(PredicateUDFs.alwaysEqTtextText.call(TTEXT_HEX, null));
    }

    // ------------------------------------------------------------------
    // text × ttext predicates (text OP ttext)
    // ------------------------------------------------------------------

    @Test @Order(23)
    void alwaysEqTextTtext_matching_returns_true() throws Exception {
        assertTrue(PredicateUDFs.alwaysEqTextTtext.call("hello", TTEXT_HEX));
    }

    @Test @Order(24)
    void everEqTextTtext_matching_returns_true() throws Exception {
        assertTrue(PredicateUDFs.everEqTextTtext.call("hello", TTEXT_HEX));
    }

    @Test @Order(25)
    void alwaysLtTextTtext_abc_lt_hello_returns_true() throws Exception {
        // "abc" < ttext(hello) → always true
        assertTrue(PredicateUDFs.alwaysLtTextTtext.call("abc", TTEXT_HEX));
    }

    @Test @Order(26)
    void alwaysGtTextTtext_xyz_gt_hello_returns_true() throws Exception {
        // "xyz" > ttext(hello) → always true
        assertTrue(PredicateUDFs.alwaysGtTextTtext.call("xyz", TTEXT_HEX));
    }

    @Test @Order(27)
    void alwaysEqTextTtext_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.alwaysEqTextTtext.call(null, TTEXT_HEX));
        assertNull(PredicateUDFs.alwaysEqTextTtext.call("hello", null));
    }
}
