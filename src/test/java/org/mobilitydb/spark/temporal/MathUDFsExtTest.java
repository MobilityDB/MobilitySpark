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

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for new MathUDFs — transcendental functions (exp, ln, log10)
 * and tnumberTrend, plus new BoolOps/Predicate UDFs (tboolWhenTrue,
 * tpointIsSimple).
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MathUDFsExtTest extends MeosTestBase {

    private static String TFLOAT_SEQ;
    private static String TINT_SEQ;
    private static String TBOOL_SEQ;
    private static String TRIP;

    @BeforeAll
    static void initMeos() {
        TFLOAT_SEQ = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01, 2.0@2020-01-02, 4.0@2020-01-04]"), (byte) 0);
        TINT_SEQ = temporal_as_hexwkb(
            tint_in("[1@2020-01-01, 3@2020-01-03]"), (byte) 0);
        TBOOL_SEQ = temporal_as_hexwkb(
            tbool_in("[true@2020-01-01, false@2020-01-02, true@2020-01-03]"), (byte) 0);
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(3 4)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // Transcendental functions
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tfloatExp_returns_nonnull() throws Exception {
        String r = MathUDFs.tfloatExp.call(TFLOAT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tfloatLn_returns_nonnull() throws Exception {
        String r = MathUDFs.tfloatLn.call(TFLOAT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void tfloatLog10_returns_nonnull() throws Exception {
        String r = MathUDFs.tfloatLog10.call(TFLOAT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // tnumberTrend
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tnumberTrend_tfloat_returns_nonnull() throws Exception {
        String r = AnalyticsUDFs.tnumberTrend.call(TFLOAT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // tnumber_trend computes a linear slope, so it requires linear
    // interpolation.  A tint is step-interpolated, so MEOS tnumber_trend
    // returns NULL (ensure_linear_interp guard in tnumber_mathfuncs.c).
    @Test @Order(5)
    void tnumberTrend_tint_step_returns_null() throws Exception {
        String r = AnalyticsUDFs.tnumberTrend.call(TINT_SEQ);
        assertNull(r);
    }

    // ------------------------------------------------------------------
    // tboolWhenTrue
    // ------------------------------------------------------------------

    @Test @Order(6)
    void tboolWhenTrue_returns_nonnull() throws Exception {
        String r = BoolOpsUDFs.tboolWhenTrue.call(TBOOL_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // tpointIsSimple
    // ------------------------------------------------------------------

    @Test @Order(7)
    void tpointIsSimple_simple_trip() throws Exception {
        Boolean r = PredicateUDFs.tpointIsSimple.call(TRIP);
        assertNotNull(r);
        assertTrue(r);
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(8)
    void null_input_returns_null() throws Exception {
        assertNull(MathUDFs.tfloatExp.call(null));
        assertNull(MathUDFs.tfloatLn.call(null));
        assertNull(MathUDFs.tfloatLog10.call(null));
        assertNull(AnalyticsUDFs.tnumberTrend.call(null));
        assertNull(BoolOpsUDFs.tboolWhenTrue.call(null));
        assertNull(PredicateUDFs.tpointIsSimple.call(null));
    }
}
