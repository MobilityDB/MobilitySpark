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
 * Unit tests for PredicateUDFs ever_ne / always_ne predicates:
 *   everNeTintInt, everNeTfloatFloat, everNeTemporal,
 *   alwaysNeTintInt, alwaysNeTfloatFloat, alwaysNeTemporal.
 *
 * Fixture:
 *   TINT_CONST   — constant tint [5@t1, 5@t2]
 *   TINT_VARYING — varying tint  [1@t1, 3@t3]
 *   TFLOAT_CONST — constant tfloat [2.0@t1, 2.0@t3]
 *   TFLOAT_VARY  — varying tfloat  [1.0@t1, 3.0@t3]
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PredicateUDFsExtTest extends MeosTestBase {

    private static String TINT_CONST;
    private static String TINT_VARYING;
    private static String TFLOAT_CONST;
    private static String TFLOAT_VARY;

    @BeforeAll
    static void initMeos() {
        TINT_CONST   = temporal_as_hexwkb(tint_in("Interp=Step;[5@2020-01-01, 5@2020-01-03]"), (byte) 0);
        TINT_VARYING = temporal_as_hexwkb(tint_in("[1@2020-01-01, 3@2020-01-03]"),              (byte) 0);
        TFLOAT_CONST = temporal_as_hexwkb(tfloat_in("Interp=Step;[2.0@2020-01-01, 2.0@2020-01-03]"), (byte) 0);
        TFLOAT_VARY  = temporal_as_hexwkb(tfloat_in("[1.0@2020-01-01, 3.0@2020-01-03]"),        (byte) 0);
    }

    // ------------------------------------------------------------------
    // everNeTintInt
    // ------------------------------------------------------------------

    @Test @Order(1)
    void everNeTintInt_const_value_eq_never_ne_returns_false() throws Exception {
        assertFalse(PredicateUDFs.everNeTintInt.call(TINT_CONST, 5),
            "A constant tint [5,5] is never != 5 — ever_ne must be false");
    }

    @Test @Order(2)
    void everNeTintInt_varying_always_has_ne_instants_returns_true() throws Exception {
        assertTrue(PredicateUDFs.everNeTintInt.call(TINT_VARYING, 5),
            "A varying tint [1,3] is always != 5 — ever_ne must be true");
    }

    @Test @Order(3)
    void everNeTintInt_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.everNeTintInt.call(null, 5));
        assertNull(PredicateUDFs.everNeTintInt.call(TINT_VARYING, null));
    }

    // ------------------------------------------------------------------
    // everNeTfloatFloat
    // ------------------------------------------------------------------

    @Test @Order(4)
    void everNeTfloatFloat_const_never_ne_returns_false() throws Exception {
        assertFalse(PredicateUDFs.everNeTfloatFloat.call(TFLOAT_CONST, 2.0),
            "Constant tfloat [2.0,2.0] is never != 2.0 — ever_ne must be false");
    }

    @Test @Order(5)
    void everNeTfloatFloat_varying_has_ne_returns_true() throws Exception {
        assertTrue(PredicateUDFs.everNeTfloatFloat.call(TFLOAT_VARY, 2.0),
            "Varying tfloat [1.0,3.0] passes through != 2.0 — ever_ne must be true");
    }

    @Test @Order(6)
    void everNeTfloatFloat_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.everNeTfloatFloat.call(null, 1.0));
        assertNull(PredicateUDFs.everNeTfloatFloat.call(TFLOAT_VARY, null));
    }

    // ------------------------------------------------------------------
    // everNeTemporal
    // ------------------------------------------------------------------

    @Test @Order(7)
    void everNeTemporal_same_value_returns_false() throws Exception {
        assertFalse(PredicateUDFs.everNeTemporal.call(TINT_CONST, TINT_CONST),
            "A sequence compared with itself is never != — ever_ne must be false");
    }

    @Test @Order(8)
    void everNeTemporal_different_values_returns_true() throws Exception {
        assertTrue(PredicateUDFs.everNeTemporal.call(TINT_VARYING, TINT_CONST),
            "Varying vs constant with different values — ever_ne must be true");
    }

    @Test @Order(9)
    void everNeTemporal_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.everNeTemporal.call(null, TINT_CONST));
        assertNull(PredicateUDFs.everNeTemporal.call(TINT_CONST, null));
    }

    // ------------------------------------------------------------------
    // alwaysNeTintInt
    // ------------------------------------------------------------------

    @Test @Order(10)
    void alwaysNeTintInt_varying_never_equals_target_returns_true() throws Exception {
        assertTrue(PredicateUDFs.alwaysNeTintInt.call(TINT_VARYING, 5),
            "Varying tint [1,3] is always != 5 — always_ne must be true");
    }

    @Test @Order(11)
    void alwaysNeTintInt_const_equals_target_returns_false() throws Exception {
        assertFalse(PredicateUDFs.alwaysNeTintInt.call(TINT_CONST, 5),
            "Constant tint [5,5] is always 5 — always_ne(5) must be false");
    }

    @Test @Order(12)
    void alwaysNeTintInt_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.alwaysNeTintInt.call(null, 5));
        assertNull(PredicateUDFs.alwaysNeTintInt.call(TINT_VARYING, null));
    }

    // ------------------------------------------------------------------
    // alwaysNeTfloatFloat
    // ------------------------------------------------------------------

    @Test @Order(13)
    void alwaysNeTfloatFloat_no_instance_returns_true() throws Exception {
        assertTrue(PredicateUDFs.alwaysNeTfloatFloat.call(TFLOAT_VARY, 5.0),
            "Varying tfloat [1.0,3.0] never equals 5.0 — always_ne must be true");
    }

    @Test @Order(14)
    void alwaysNeTfloatFloat_equals_target_returns_false() throws Exception {
        assertFalse(PredicateUDFs.alwaysNeTfloatFloat.call(TFLOAT_CONST, 2.0),
            "Constant tfloat [2.0,2.0] equals 2.0 — always_ne(2.0) must be false");
    }

    @Test @Order(15)
    void alwaysNeTfloatFloat_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.alwaysNeTfloatFloat.call(null, 1.0));
    }

    // ------------------------------------------------------------------
    // alwaysNeTemporal
    // ------------------------------------------------------------------

    @Test @Order(16)
    void alwaysNeTemporal_same_value_returns_false() throws Exception {
        assertFalse(PredicateUDFs.alwaysNeTemporal.call(TINT_CONST, TINT_CONST),
            "Constant compared with itself — always_ne must be false");
    }

    @Test @Order(17)
    void alwaysNeTemporal_disjoint_values_returns_true() throws Exception {
        assertTrue(PredicateUDFs.alwaysNeTemporal.call(TINT_VARYING, TINT_CONST),
            "TINT_VARYING [1,3] is always != TINT_CONST [5,5] — always_ne must be true");
    }

    @Test @Order(18)
    void alwaysNeTemporal_null_returns_null() throws Exception {
        assertNull(PredicateUDFs.alwaysNeTemporal.call(null, TINT_CONST));
    }
}
