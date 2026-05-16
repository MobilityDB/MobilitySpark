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
 * Unit tests for MoreAccessorUDFs temporal decomposition accessors:
 *   temporalInstants, temporalSequences, temporalSegments.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MoreAccessorUDFsExt4Test extends MeosTestBase {

    /** 3-instant linear tint sequence. */
    private static String TINT_SEQ_HEX;
    /** TSequenceSet: 2 disjoint sequences. */
    private static String TINT_SEQSET_HEX;

    @BeforeAll
    static void initMeos() {
        TINT_SEQ_HEX = temporal_as_hexwkb(
            tint_in("[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00, 3@2020-01-03 00:00:00+00]"),
            (byte) 0);
        TINT_SEQSET_HEX = temporal_as_hexwkb(
            tint_in("{[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00]," +
                     "[5@2020-02-01 00:00:00+00, 7@2020-02-03 00:00:00+00]}"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // temporalInstants
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalInstants_seq_returns_nonnull_list() throws Exception {
        List<String> r = MoreAccessorUDFs.temporalInstants.call(TINT_SEQ_HEX);
        assertNotNull(r, "temporalInstants must return non-null");
        assertFalse(r.isEmpty());
    }

    @Test @Order(2)
    void temporalInstants_seq_count_matches_instants() throws Exception {
        List<String> r = MoreAccessorUDFs.temporalInstants.call(TINT_SEQ_HEX);
        assertNotNull(r);
        assertEquals(3, r.size(), "3-instant sequence must yield 3 instants");
    }

    @Test @Order(3)
    void temporalInstants_elements_are_valid_hexwkb() throws Exception {
        List<String> r = MoreAccessorUDFs.temporalInstants.call(TINT_SEQ_HEX);
        assertNotNull(r);
        for (String elem : r) {
            assertNotNull(elem, "Each element must be non-null");
            assertFalse(elem.isBlank(), "Each element must be a non-blank hex string");
        }
    }

    @Test @Order(4)
    void temporalInstants_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.temporalInstants.call(null));
    }

    // ------------------------------------------------------------------
    // temporalSequences
    // ------------------------------------------------------------------

    @Test @Order(5)
    void temporalSequences_seqset_returns_nonnull_list() throws Exception {
        List<String> r = MoreAccessorUDFs.temporalSequences.call(TINT_SEQSET_HEX);
        assertNotNull(r, "temporalSequences must return non-null for TSequenceSet");
        assertFalse(r.isEmpty());
    }

    @Test @Order(6)
    void temporalSequences_seqset_count_matches_sequences() throws Exception {
        List<String> r = MoreAccessorUDFs.temporalSequences.call(TINT_SEQSET_HEX);
        assertNotNull(r);
        assertEquals(2, r.size(), "2-sequence set must yield 2 sequences");
    }

    @Test @Order(7)
    void temporalSequences_elements_are_valid_hexwkb() throws Exception {
        List<String> r = MoreAccessorUDFs.temporalSequences.call(TINT_SEQSET_HEX);
        assertNotNull(r);
        for (String elem : r) {
            assertNotNull(elem);
            assertFalse(elem.isBlank());
        }
    }

    @Test @Order(8)
    void temporalSequences_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.temporalSequences.call(null));
    }

    // ------------------------------------------------------------------
    // temporalSegments
    // ------------------------------------------------------------------

    @Test @Order(9)
    void temporalSegments_seq_returns_nonnull_list() throws Exception {
        List<String> r = MoreAccessorUDFs.temporalSegments.call(TINT_SEQ_HEX);
        assertNotNull(r, "temporalSegments must return non-null");
        assertFalse(r.isEmpty());
    }

    @Test @Order(10)
    void temporalSegments_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.temporalSegments.call(null));
    }
}
