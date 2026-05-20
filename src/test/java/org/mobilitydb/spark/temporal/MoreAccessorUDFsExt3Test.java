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

import java.util.List;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MoreAccessorUDFs array-returning accessors:
 *   tintValues, tfloatValues.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MoreAccessorUDFsExt3Test {

    private static String TINT_SEQ_HEX;
    private static String TFLOAT_SEQ_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TINT_SEQ_HEX = temporal_as_hexwkb(
            tint_in("Interp=Step;[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00, 3@2020-01-03 00:00:00+00]"),
            (byte) 0);
        TFLOAT_SEQ_HEX = temporal_as_hexwkb(
            tfloat_in("Interp=Step;[1.5@2020-01-01 00:00:00+00, 2.5@2020-01-02 00:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // tintValues
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tintValues_returns_nonnull_list() throws Exception {
        List<Integer> r = MoreAccessorUDFs.tintValues.call(TINT_SEQ_HEX);
        assertNotNull(r, "tintValues must return non-null");
        assertFalse(r.isEmpty());
    }

    @Test @Order(2)
    void tintValues_contains_expected_values() throws Exception {
        List<Integer> r = MoreAccessorUDFs.tintValues.call(TINT_SEQ_HEX);
        assertNotNull(r);
        assertTrue(r.contains(1), "Must contain value 1");
        assertTrue(r.contains(2), "Must contain value 2");
        assertTrue(r.contains(3), "Must contain value 3");
    }

    @Test @Order(3)
    void tintValues_count_matches_distinct_instants() throws Exception {
        List<Integer> r = MoreAccessorUDFs.tintValues.call(TINT_SEQ_HEX);
        assertNotNull(r);
        assertEquals(3, r.size(), "3-instant step tint must yield 3 distinct values");
    }

    @Test @Order(4)
    void tintValues_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.tintValues.call(null));
    }

    // ------------------------------------------------------------------
    // tfloatValues
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tfloatValues_returns_nonnull_list() throws Exception {
        List<Double> r = MoreAccessorUDFs.tfloatValues.call(TFLOAT_SEQ_HEX);
        assertNotNull(r, "tfloatValues must return non-null");
        assertFalse(r.isEmpty());
    }

    @Test @Order(6)
    void tfloatValues_contains_expected_values() throws Exception {
        List<Double> r = MoreAccessorUDFs.tfloatValues.call(TFLOAT_SEQ_HEX);
        assertNotNull(r);
        assertTrue(r.contains(1.5), "Must contain value 1.5");
        assertTrue(r.contains(2.5), "Must contain value 2.5");
    }

    @Test @Order(7)
    void tfloatValues_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.tfloatValues.call(null));
    }
}
