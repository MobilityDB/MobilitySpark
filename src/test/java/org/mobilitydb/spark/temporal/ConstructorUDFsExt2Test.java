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
 * Unit tests for ConstructorUDFs constant-temporal constructors:
 *   tboolFromBaseTemp, tintFromBaseTemp, tfloatFromBaseTemp, ttextFromBaseTemp.
 *
 * Each creates a temporal value that is constant at the given scalar over the
 * same time structure as a reference temporal.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConstructorUDFsExt2Test {

    private static String TINT_REF_HEX;
    private static String TFLOAT_REF_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        // Reference temporals whose time structure will be reused
        TINT_REF_HEX = temporal_as_hexwkb(
            tint_in("[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00]"), (byte) 0);
        TFLOAT_REF_HEX = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01 00:00:00+00, 2.0@2020-01-02 00:00:00+00]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // tboolFromBaseTemp
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tboolFromBaseTemp_returns_nonnull_hexwkb() throws Exception {
        String r = ConstructorUDFs.tboolFromBaseTemp.call(true, TINT_REF_HEX);
        assertNotNull(r, "tboolFromBaseTemp must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tboolFromBaseTemp_false_returns_nonnull() throws Exception {
        String r = ConstructorUDFs.tboolFromBaseTemp.call(false, TINT_REF_HEX);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void tboolFromBaseTemp_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.tboolFromBaseTemp.call(null, TINT_REF_HEX));
        assertNull(ConstructorUDFs.tboolFromBaseTemp.call(true, null));
    }

    // ------------------------------------------------------------------
    // tintFromBaseTemp
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tintFromBaseTemp_returns_nonnull_hexwkb() throws Exception {
        String r = ConstructorUDFs.tintFromBaseTemp.call(42, TINT_REF_HEX);
        assertNotNull(r, "tintFromBaseTemp must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(5)
    void tintFromBaseTemp_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.tintFromBaseTemp.call(null, TINT_REF_HEX));
        assertNull(ConstructorUDFs.tintFromBaseTemp.call(42, null));
    }

    // ------------------------------------------------------------------
    // tfloatFromBaseTemp
    // ------------------------------------------------------------------

    @Test @Order(6)
    void tfloatFromBaseTemp_returns_nonnull_hexwkb() throws Exception {
        String r = ConstructorUDFs.tfloatFromBaseTemp.call(3.14, TFLOAT_REF_HEX);
        assertNotNull(r, "tfloatFromBaseTemp must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void tfloatFromBaseTemp_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.tfloatFromBaseTemp.call(null, TFLOAT_REF_HEX));
        assertNull(ConstructorUDFs.tfloatFromBaseTemp.call(3.14, null));
    }

    // ------------------------------------------------------------------
    // ttextFromBaseTemp
    // ------------------------------------------------------------------

    @Test @Order(8)
    void ttextFromBaseTemp_returns_nonnull_hexwkb() throws Exception {
        String r = ConstructorUDFs.ttextFromBaseTemp.call("hello", TINT_REF_HEX);
        assertNotNull(r, "ttextFromBaseTemp must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void ttextFromBaseTemp_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.ttextFromBaseTemp.call(null, TINT_REF_HEX));
        assertNull(ConstructorUDFs.ttextFromBaseTemp.call("hello", null));
    }
}
