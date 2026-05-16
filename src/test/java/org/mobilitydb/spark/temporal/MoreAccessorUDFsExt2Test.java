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

import java.sql.Timestamp;
import java.util.List;
import org.mobilitydb.spark.MeosTestBase;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MoreAccessorUDFs array-returning accessors:
 *   temporalTimestamps, tboolValues.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MoreAccessorUDFsExt2Test extends MeosTestBase {

    private static String TINT_SEQ_HEX;
    private static String TBOOL_SEQ_HEX;

    @BeforeAll
    static void initMeos() {
        TINT_SEQ_HEX = temporal_as_hexwkb(
            tint_in("[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00, 3@2020-01-03 00:00:00+00]"),
            (byte) 0);
        TBOOL_SEQ_HEX = temporal_as_hexwkb(
            tbool_in("[true@2020-01-01 00:00:00+00, false@2020-01-02 00:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // temporalTimestamps
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalTimestamps_returns_nonnull_list() throws Exception {
        List<Timestamp> r = MoreAccessorUDFs.temporalTimestamps.call(TINT_SEQ_HEX);
        assertNotNull(r, "temporalTimestamps must return non-null");
        assertFalse(r.isEmpty(), "List must not be empty for 3-instant tint sequence");
    }

    @Test @Order(2)
    void temporalTimestamps_count_matches_instants() throws Exception {
        List<Timestamp> r = MoreAccessorUDFs.temporalTimestamps.call(TINT_SEQ_HEX);
        assertNotNull(r);
        assertEquals(3, r.size(), "3-instant sequence must yield 3 timestamps");
    }

    @Test @Order(3)
    void temporalTimestamps_timestamps_are_ordered() throws Exception {
        List<Timestamp> r = MoreAccessorUDFs.temporalTimestamps.call(TINT_SEQ_HEX);
        assertNotNull(r);
        assertEquals(3, r.size());
        assertTrue(r.get(0).before(r.get(1)), "timestamps must be in ascending order");
        assertTrue(r.get(1).before(r.get(2)));
    }

    @Test @Order(4)
    void temporalTimestamps_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.temporalTimestamps.call(null));
    }

    // ------------------------------------------------------------------
    // tboolValues
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tboolValues_returns_nonnull_list() throws Exception {
        List<Boolean> r = MoreAccessorUDFs.tboolValues.call(TBOOL_SEQ_HEX);
        assertNotNull(r, "tboolValues must return non-null");
        assertFalse(r.isEmpty());
    }

    @Test @Order(6)
    void tboolValues_contains_both_values() throws Exception {
        List<Boolean> r = MoreAccessorUDFs.tboolValues.call(TBOOL_SEQ_HEX);
        assertNotNull(r);
        assertTrue(r.contains(Boolean.TRUE),  "Must contain true (appears in sequence)");
        assertTrue(r.contains(Boolean.FALSE), "Must contain false (appears in sequence)");
    }

    @Test @Order(7)
    void tboolValues_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.tboolValues.call(null));
    }
}
