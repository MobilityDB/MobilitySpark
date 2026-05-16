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
import org.mobilitydb.spark.MeosTestBase;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MoreAccessorUDFs.ttextValues.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MoreAccessorUDFsExt5Test extends MeosTestBase {

    /** ttext sequence with two distinct values. */
    private static String TTEXT_SEQ_HEX;
    /** ttext sequence with repeated value — distinct count = 1. */
    private static String TTEXT_CONST_HEX;

    @BeforeAll
    static void initMeos() {
        TTEXT_SEQ_HEX = temporal_as_hexwkb(
            ttext_in("[hello@2020-01-01 00:00:00+00, world@2020-01-02 00:00:00+00]"),
            (byte) 0);
        TTEXT_CONST_HEX = temporal_as_hexwkb(
            ttext_in("[hello@2020-01-01 00:00:00+00, hello@2020-01-03 00:00:00+00]"),
            (byte) 0);
    }

    @Test @Order(1)
    void ttextValues_seq_returns_nonnull() throws Exception {
        List<String> vs = MoreAccessorUDFs.ttextValues.call(TTEXT_SEQ_HEX);
        assertNotNull(vs, "ttextValues must return non-null for ttext sequence");
    }

    @Test @Order(2)
    void ttextValues_seq_count_is_two() throws Exception {
        List<String> vs = MoreAccessorUDFs.ttextValues.call(TTEXT_SEQ_HEX);
        assertNotNull(vs);
        assertEquals(2, vs.size(), "two-value sequence must yield 2 distinct values");
    }

    @Test @Order(3)
    void ttextValues_elements_are_non_blank() throws Exception {
        List<String> vs = MoreAccessorUDFs.ttextValues.call(TTEXT_SEQ_HEX);
        assertNotNull(vs);
        for (String s : vs) {
            assertNotNull(s);
            assertFalse(s.isBlank(), "each value string must be non-blank");
        }
    }

    @Test @Order(4)
    void ttextValues_constant_seq_returns_one_distinct_value() throws Exception {
        List<String> vs = MoreAccessorUDFs.ttextValues.call(TTEXT_CONST_HEX);
        assertNotNull(vs);
        assertEquals(1, vs.size(), "constant ttext must yield exactly 1 distinct value");
    }

    @Test @Order(5)
    void ttextValues_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.ttextValues.call(null));
    }
}
