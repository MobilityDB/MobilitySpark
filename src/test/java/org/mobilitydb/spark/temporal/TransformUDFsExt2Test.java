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
 * Unit tests for second batch of TransformUDF extensions:
 * tintToTfloat, temporalSimplifyMinTdelta, temporalTPrecision,
 * and temporalDeleteTstzset (restriction extension).
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TransformUDFsExt2Test extends MeosTestBase {

    private static String TINT_SEQ;
    private static String TFLOAT_SEQ;
    private static String TSTZSET_HEX;

    @BeforeAll
    static void initMeos() {
        TINT_SEQ = temporal_as_hexwkb(
            tint_in("[1@2020-01-01, 2@2020-01-02, 3@2020-01-03, 4@2020-01-04, 5@2020-01-05]"),
            (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(
            tfloat_in("[1.5@2020-01-01, 2.5@2020-01-03, 3.5@2020-01-05]"), (byte) 0);
        TSTZSET_HEX = set_as_hexwkb(
            tstzset_in("{2020-01-02 00:00:00+00, 2020-01-04 00:00:00+00}"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // tintToTfloat
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tintToTfloat_returns_tfloat_hex() throws Exception {
        String r = TransformUDFs.tintToTfloat.call(TINT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tintToTfloat_value_preserved() throws Exception {
        String r = TransformUDFs.tintToTfloat.call(TINT_SEQ);
        assertNotNull(r);
        Double sv = AccessorUDFs.tfloatStartValue.call(r);
        assertNotNull(sv);
        assertEquals(1.0, sv, 1e-9);
    }

    // ------------------------------------------------------------------
    // temporalSimplifyMinTdelta
    // ------------------------------------------------------------------

    @Test @Order(3)
    void temporalSimplifyMinTdelta_returns_nonnull() throws Exception {
        String r = TransformUDFs.temporalSimplifyMinTdelta.call(TFLOAT_SEQ, "1 day");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // temporalTPrecision
    // ------------------------------------------------------------------

    @Test @Order(4)
    void temporalTPrecision_returns_nonnull() throws Exception {
        String r = TransformUDFs.temporalTPrecision.call(TFLOAT_SEQ, "1 day");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // temporalDeleteTstzset (RestrictionUDFs)
    // ------------------------------------------------------------------

    @Test @Order(5)
    void temporalDeleteTstzset_removes_instants() throws Exception {
        String r = RestrictionUDFs.temporalDeleteTstzset.call(TINT_SEQ, TSTZSET_HEX);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(6)
    void null_inputs_return_null() throws Exception {
        assertNull(TransformUDFs.tintToTfloat.call(null));
        assertNull(TransformUDFs.temporalSimplifyMinTdelta.call(null, "1 day"));
        assertNull(TransformUDFs.temporalSimplifyMinTdelta.call(TFLOAT_SEQ, null));
        assertNull(TransformUDFs.temporalTPrecision.call(null, "1 day"));
        assertNull(RestrictionUDFs.temporalDeleteTstzset.call(null, TSTZSET_HEX));
        assertNull(RestrictionUDFs.temporalDeleteTstzset.call(TINT_SEQ, null));
    }
}
