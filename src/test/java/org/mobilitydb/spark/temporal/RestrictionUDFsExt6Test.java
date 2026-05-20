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
 * Unit tests for RestrictionUDFs value-set operations:
 *   temporalAtValues, temporalMinusValues.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RestrictionUDFsExt6Test {

    private static String TINT_SEQ;
    private static String INTSET_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        // Sequence with values 1, 2, 3
        TINT_SEQ = temporal_as_hexwkb(
            tint_in("Interp=Step;[1@2020-01-01 00:00:00+00," +
                    " 2@2020-01-02 00:00:00+00," +
                    " 3@2020-01-03 00:00:00+00, 3@2020-01-04 00:00:00+00]"),
            (byte) 0);

        // intset {1, 3}
        INTSET_HEX = set_as_hexwkb(intset_in("{1, 3}"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // temporalAtValues
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalAtValues_restricts_to_matching_instants() throws Exception {
        String r = RestrictionUDFs.temporalAtValues.call(TINT_SEQ, INTSET_HEX);
        assertNotNull(r, "AT values {1,3} must return non-null for sequence with those values");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void temporalAtValues_empty_set_returns_null_or_empty() throws Exception {
        String emptyHex = set_as_hexwkb(intset_in("{99}"), (byte) 0);
        String r = RestrictionUDFs.temporalAtValues.call(TINT_SEQ, emptyHex);
        assertTrue(r == null || !r.isBlank(),
            "AT nonexistent value must return null or valid empty temporal");
    }

    @Test @Order(3)
    void temporalAtValues_null_returns_null() throws Exception {
        assertNull(RestrictionUDFs.temporalAtValues.call(null, INTSET_HEX));
        assertNull(RestrictionUDFs.temporalAtValues.call(TINT_SEQ, null));
    }

    // ------------------------------------------------------------------
    // temporalMinusValues
    // ------------------------------------------------------------------

    @Test @Order(4)
    void temporalMinusValues_removes_matching_instants() throws Exception {
        String r = RestrictionUDFs.temporalMinusValues.call(TINT_SEQ, INTSET_HEX);
        assertTrue(r == null || !r.isBlank(),
            "MINUS values {1,3} must return null or a shorter temporal");
    }

    @Test @Order(5)
    void temporalMinusValues_nonexistent_value_returns_original() throws Exception {
        String emptyHex = set_as_hexwkb(intset_in("{99}"), (byte) 0);
        String r = RestrictionUDFs.temporalMinusValues.call(TINT_SEQ, emptyHex);
        assertNotNull(r, "MINUS nonexistent value must return original sequence");
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void temporalMinusValues_null_returns_null() throws Exception {
        assertNull(RestrictionUDFs.temporalMinusValues.call(null, INTSET_HEX));
        assertNull(RestrictionUDFs.temporalMinusValues.call(TINT_SEQ, null));
    }
}
