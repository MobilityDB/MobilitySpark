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
 * Unit tests for new accessor UDFs:
 *   tintValueN (MoreAccessorUDFs), tnumberToSpan, tnumberToTbox (AccessorUDFs).
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AccessorUDFsExt2Test extends MeosTestBase {

    private static String TINT_SEQ;
    private static String TFLOAT_SEQ;

    @BeforeAll
    static void initMeos() {
        TINT_SEQ   = temporal_as_hexwkb(tint_in("[3@2020-01-01, 7@2020-01-03]"),  (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(tfloat_in("[1.5@2020-01-01, 4.5@2020-01-03]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // tintValueN (MoreAccessorUDFs)
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tintValueN_first_value_returns_correct() throws Exception {
        Integer r = MoreAccessorUDFs.tintValueN.call(TINT_SEQ, 1);
        assertNotNull(r, "First distinct value must be non-null");
        assertEquals(3, r.intValue());
    }

    @Test @Order(2)
    void tintValueN_second_value_returns_correct() throws Exception {
        Integer r = MoreAccessorUDFs.tintValueN.call(TINT_SEQ, 2);
        assertNotNull(r, "Second distinct value must be non-null");
        assertEquals(7, r.intValue());
    }

    @Test @Order(3)
    void tintValueN_out_of_range_returns_null() throws Exception {
        Integer r = MoreAccessorUDFs.tintValueN.call(TINT_SEQ, 99);
        assertNull(r, "Out-of-range index must return null");
    }

    @Test @Order(4)
    void tintValueN_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.tintValueN.call(null, 1));
        assertNull(MoreAccessorUDFs.tintValueN.call(TINT_SEQ, null));
    }

    // ------------------------------------------------------------------
    // tnumberToSpan (AccessorUDFs)
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tnumberToSpan_returns_nonnull_hexwkb() throws Exception {
        String r = AccessorUDFs.tnumberToSpan.call(TINT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void tnumberToSpan_tfloat_returns_nonnull_hexwkb() throws Exception {
        String r = AccessorUDFs.tnumberToSpan.call(TFLOAT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void tnumberToSpan_null_returns_null() throws Exception {
        assertNull(AccessorUDFs.tnumberToSpan.call(null));
    }

    // ------------------------------------------------------------------
    // tnumberToTbox (AccessorUDFs)
    // ------------------------------------------------------------------

    @Test @Order(8)
    void tnumberToTbox_returns_nonnull_hexwkb() throws Exception {
        String r = AccessorUDFs.tnumberToTbox.call(TINT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void tnumberToTbox_tfloat_returns_nonnull_hexwkb() throws Exception {
        String r = AccessorUDFs.tnumberToTbox.call(TFLOAT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(10)
    void tnumberToTbox_null_returns_null() throws Exception {
        assertNull(AccessorUDFs.tnumberToTbox.call(null));
    }
}
