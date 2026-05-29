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
 * Unit tests for cross-type TBox × TNumber positional/topological UDFs.
 *
 * Fixtures:
 *   TBOX_LO  — TBOXFLOAT XT([0,5],[2020-01-01,2020-01-02])  — value range [0,5]
 *   TBOX_HI  — TBOXFLOAT XT([10,15],[2020-01-03,2020-01-04]) — value range [10,15]
 *   TFLOAT_LO — tfloat trip 0→5 over 2020-01-01 → 2020-01-02
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TBoxOpsUDFsTest extends MeosTestBase {

    private static String TBOX_LO;
    private static String TBOX_HI;
    private static String TFLOAT_LO;

    @BeforeAll
    static void initMeos() throws Exception {
        TBOX_LO = ConstructorUDFs.tbox.call(
            "TBOXFLOAT XT([0,5],[2020-01-01 00:00:00+00,2020-01-02 00:00:00+00])");
        TBOX_HI = ConstructorUDFs.tbox.call(
            "TBOXFLOAT XT([10,15],[2020-01-03 00:00:00+00,2020-01-04 00:00:00+00])");
        TFLOAT_LO = temporal_as_hexwkb(
            tfloat_in("[0.0@2020-01-01 00:00:00+00, 5.0@2020-01-02 00:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // Null guards
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tboxLeftTbox_null_returns_null() throws Exception {
        assertNull(TBoxOpsUDFs.tboxLeftTbox.call(null, TBOX_HI));
        assertNull(TBoxOpsUDFs.tboxLeftTbox.call(TBOX_LO, null));
    }

    @Test @Order(2)
    void tboxLeftTnumber_null_returns_null() throws Exception {
        assertNull(TBoxOpsUDFs.tboxLeftTnumber.call(null, TFLOAT_LO));
        assertNull(TBoxOpsUDFs.tboxLeftTnumber.call(TBOX_LO, null));
    }

    @Test @Order(3)
    void tnumberLeftTbox_null_returns_null() throws Exception {
        assertNull(TBoxOpsUDFs.tnumberLeftTbox.call(null, TBOX_HI));
        assertNull(TBoxOpsUDFs.tnumberLeftTbox.call(TFLOAT_LO, null));
    }

    // ------------------------------------------------------------------
    // tbox × tbox — semantic correctness
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tboxLeftTbox_low_is_left_of_high() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxLeftTbox.call(TBOX_LO, TBOX_HI),
            "TBOX_LO (val [0,5]) must be left of TBOX_HI (val [10,15])");
    }

    @Test @Order(5)
    void tboxLeftTbox_high_not_left_of_low() throws Exception {
        assertFalse(TBoxOpsUDFs.tboxLeftTbox.call(TBOX_HI, TBOX_LO));
    }

    @Test @Order(6)
    void tboxRightTbox_high_is_right_of_low() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxRightTbox.call(TBOX_HI, TBOX_LO));
    }

    @Test @Order(7)
    void tboxBeforeTbox_low_is_before_high() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxBeforeTbox.call(TBOX_LO, TBOX_HI),
            "TBOX_LO time precedes TBOX_HI time");
    }

    @Test @Order(8)
    void tboxAfterTbox_high_is_after_low() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxAfterTbox.call(TBOX_HI, TBOX_LO));
    }

    @Test @Order(9)
    void tboxOverlapsTbox_disjoint_returns_false() throws Exception {
        assertFalse(TBoxOpsUDFs.tboxOverlapsTbox.call(TBOX_LO, TBOX_HI));
    }

    @Test @Order(10)
    void tboxOverlapsTbox_self_returns_true() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxOverlapsTbox.call(TBOX_LO, TBOX_LO));
    }

    @Test @Order(11)
    void tboxSameTbox_self_returns_true() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxSameTbox.call(TBOX_LO, TBOX_LO));
    }

    @Test @Order(12)
    void tboxContainsTbox_self_returns_true() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxContainsTbox.call(TBOX_LO, TBOX_LO));
    }

    @Test @Order(13)
    void tboxContainedTbox_self_returns_true() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxContainedTbox.call(TBOX_LO, TBOX_LO));
    }

    @Test @Order(14)
    void tboxAdjacentTbox_disjoint_in_value_returns_nonnull() throws Exception {
        // Disjoint boxes are not adjacent (gap between [0,5] and [10,15])
        assertNotNull(TBoxOpsUDFs.tboxAdjacentTbox.call(TBOX_LO, TBOX_HI));
    }

    @Test @Order(15)
    void tboxOverleftTbox_low_is_overleft_of_high() throws Exception {
        // [0,5] is overleft (i.e., does not extend right) of [10,15]
        assertTrue(TBoxOpsUDFs.tboxOverleftTbox.call(TBOX_LO, TBOX_HI));
    }

    @Test @Order(16)
    void tboxOverbeforeTbox_low_is_overbefore_high() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxOverbeforeTbox.call(TBOX_LO, TBOX_HI));
    }

    // ------------------------------------------------------------------
    // tbox × tnumber — semantic correctness
    // ------------------------------------------------------------------

    @Test @Order(17)
    void tboxOverlapsTnumber_self_returns_true() throws Exception {
        // TBOX_LO covers tfloat 0→5 over the same time window
        assertTrue(TBoxOpsUDFs.tboxOverlapsTnumber.call(TBOX_LO, TFLOAT_LO));
    }

    @Test @Order(18)
    void tboxLeftTnumber_high_not_left_of_low_tnumber() throws Exception {
        // TBOX_HI (val [10,15]) is NOT left of TFLOAT_LO (val [0,5])
        assertFalse(TBoxOpsUDFs.tboxLeftTnumber.call(TBOX_HI, TFLOAT_LO));
    }

    @Test @Order(19)
    void tboxContainsTnumber_overlap_returns_nonnull() throws Exception {
        assertNotNull(TBoxOpsUDFs.tboxContainsTnumber.call(TBOX_LO, TFLOAT_LO));
    }

    @Test @Order(20)
    void tboxBeforeTnumber_high_not_before_low() throws Exception {
        // TBOX_HI starts 2020-01-03; TFLOAT_LO ends 2020-01-02. So TBOX_HI is AFTER, not BEFORE.
        assertFalse(TBoxOpsUDFs.tboxBeforeTnumber.call(TBOX_HI, TFLOAT_LO));
    }

    @Test @Order(21)
    void tboxAfterTnumber_high_is_after_low_tnumber() throws Exception {
        assertTrue(TBoxOpsUDFs.tboxAfterTnumber.call(TBOX_HI, TFLOAT_LO));
    }

    // ------------------------------------------------------------------
    // tnumber × tbox — semantic correctness
    // ------------------------------------------------------------------

    @Test @Order(22)
    void tnumberOverlapsTbox_self_returns_true() throws Exception {
        assertTrue(TBoxOpsUDFs.tnumberOverlapsTbox.call(TFLOAT_LO, TBOX_LO));
    }

    @Test @Order(23)
    void tnumberLeftTbox_low_is_left_of_high() throws Exception {
        // TFLOAT_LO (val [0,5]) is left of TBOX_HI (val [10,15])
        assertTrue(TBoxOpsUDFs.tnumberLeftTbox.call(TFLOAT_LO, TBOX_HI));
    }

    @Test @Order(24)
    void tnumberBeforeTbox_low_is_before_high() throws Exception {
        assertTrue(TBoxOpsUDFs.tnumberBeforeTbox.call(TFLOAT_LO, TBOX_HI));
    }

    @Test @Order(25)
    void tnumberContainedTbox_self_returns_nonnull() throws Exception {
        assertNotNull(TBoxOpsUDFs.tnumberContainedTbox.call(TFLOAT_LO, TBOX_LO));
    }

    @Test @Order(26)
    void tnumberOverrightTbox_low_not_overright_of_high() throws Exception {
        // TFLOAT_LO (val [0,5]) does NOT extend past the right of TBOX_HI (val [10,15])
        assertFalse(TBoxOpsUDFs.tnumberOverrightTbox.call(TFLOAT_LO, TBOX_HI));
    }

    @Test @Order(27)
    void tnumberSameTbox_self_returns_nonnull() throws Exception {
        assertNotNull(TBoxOpsUDFs.tnumberSameTbox.call(TFLOAT_LO, TBOX_LO));
    }

    // ------------------------------------------------------------------
    // tboxExpandFloat / tboxExpandInt — wired via MeosNative
    // ------------------------------------------------------------------

    @Test @Order(28)
    void tboxExpandFloat_returns_nonnull() throws Exception {
        String r = TBoxUDFs.tboxExpandFloat.call(TBOX_LO, 2.5);
        assertNotNull(r, "tboxExpandFloat must return non-null hex-WKB");
        assertFalse(r.isBlank());
    }

    @Test @Order(29)
    void tboxExpandFloat_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxExpandFloat.call(null, 2.5));
        assertNull(TBoxUDFs.tboxExpandFloat.call(TBOX_LO, null));
    }

    @Test @Order(30)
    void tboxExpandInt_returns_nonnull() throws Exception {
        // Need an integer TBox for tintbox_expand
        String tboxInt = ConstructorUDFs.tbox.call(
            "TBOXINT XT([0,5],[2020-01-01 00:00:00+00,2020-01-02 00:00:00+00])");
        String r = TBoxUDFs.tboxExpandInt.call(tboxInt, 3);
        assertNotNull(r, "tboxExpandInt must return non-null hex-WKB");
        assertFalse(r.isBlank());
    }

    @Test @Order(31)
    void tboxExpandInt_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxExpandInt.call(null, 3));
    }
}
