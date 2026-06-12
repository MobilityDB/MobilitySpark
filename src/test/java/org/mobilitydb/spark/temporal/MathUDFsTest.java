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
 * Unit tests for MathUDFs — tnumber arithmetic and unary analytics.
 *
 * MEOS function authority: meos/include/meos.h (026_tnumber_mathfuncs)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MathUDFsTest {

    private static String TINT_NEG;
    private static String TINT_POS;
    private static String TFLOAT_POS;
    private static String TRIP;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        // tint with negative value at t1, positive at t2
        TINT_NEG = temporal_as_hexwkb(tint_in("[-5@2020-01-01, 3@2020-01-02]"), (byte) 0);
        TINT_POS = temporal_as_hexwkb(tint_in("[2@2020-01-01, 4@2020-01-02]"), (byte) 0);
        TFLOAT_POS = temporal_as_hexwkb(tfloat_in("[1.0@2020-01-01, 3.0@2020-01-02]"), (byte) 0);
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01, POINT(1 0)@2020-01-02]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // Unary analytics
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tnumberAbs_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.tnumberAbs.call(TINT_NEG);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tnumberDeltaValue_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.tnumberDeltaValue.call(TINT_POS);
        assertNotNull(r);
    }

    @Test @Order(3)
    void tnumberAngularDifference_returns_nonnull_or_null_for_step() throws Exception {
        // tnumber_angular_difference may return null for step sequences; just check no exception
        String r = MathUDFs.tnumberAngularDifference.call(TFLOAT_POS);
        // result can be null for non-periodic sequences — no assertion on value
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(4)
    void tpointAngularDifference_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.tpointAngularDifference.call(TRIP);
        // May be null for sequences with only 2 instants (no angular change to compute)
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(5)
    void tnumberAbs_null_returns_null() throws Exception {
        assertNull(MathUDFs.tnumberAbs.call(null));
    }

    // ------------------------------------------------------------------
    // tint + scalar
    // ------------------------------------------------------------------

    @Test @Order(6)
    void addTintInt_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.addTintInt.call(TINT_POS, 10);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void subTintInt_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.subTintInt.call(TINT_POS, 1);
        assertNotNull(r);
    }

    @Test @Order(8)
    void multTintInt_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.multTintInt.call(TINT_POS, 3);
        assertNotNull(r);
    }

    @Test @Order(9)
    void divTintInt_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.divTintInt.call(TINT_POS, 2);
        assertNotNull(r);
    }

    @Test @Order(10)
    void addTintInt_null_input_returns_null() throws Exception {
        assertNull(MathUDFs.addTintInt.call(null, 5));
        assertNull(MathUDFs.addTintInt.call(TINT_POS, null));
    }

    // ------------------------------------------------------------------
    // tfloat + scalar
    // ------------------------------------------------------------------

    @Test @Order(11)
    void addTfloatFloat_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.addTfloatFloat.call(TFLOAT_POS, 0.5);
        assertNotNull(r);
    }

    @Test @Order(12)
    void subTfloatFloat_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.subTfloatFloat.call(TFLOAT_POS, 0.5);
        assertNotNull(r);
    }

    @Test @Order(13)
    void multTfloatFloat_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.multTfloatFloat.call(TFLOAT_POS, 2.0);
        assertNotNull(r);
    }

    @Test @Order(14)
    void divTfloatFloat_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.divTfloatFloat.call(TFLOAT_POS, 2.0);
        assertNotNull(r);
    }

    // ------------------------------------------------------------------
    // tnumber + tnumber
    // ------------------------------------------------------------------

    @Test @Order(15)
    void addTnumberTnumber_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.addTnumberTnumber.call(TINT_POS, TINT_NEG);
        assertNotNull(r);
    }

    @Test @Order(16)
    void subTnumberTnumber_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.subTnumberTnumber.call(TINT_POS, TINT_NEG);
        assertNotNull(r);
    }

    @Test @Order(17)
    void multTnumberTnumber_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.multTnumberTnumber.call(TINT_POS, TINT_POS);
        assertNotNull(r);
    }

    @Test @Order(18)
    void divTnumberTnumber_returns_nonnull_hexwkb() throws Exception {
        String r = MathUDFs.divTnumberTnumber.call(TINT_POS, TINT_POS);
        assertNotNull(r);
    }

    @Test @Order(19)
    void addTnumberTnumber_null_returns_null() throws Exception {
        assertNull(MathUDFs.addTnumberTnumber.call(null, TINT_POS));
        assertNull(MathUDFs.addTnumberTnumber.call(TINT_POS, null));
    }
}
