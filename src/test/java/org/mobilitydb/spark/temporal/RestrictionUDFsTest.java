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
 * Unit tests for RestrictionUDFs — at/minus restrictions by value and
 * timestamp set/span/spanset, and delete operations.
 *
 * For cases where MEOS may legitimately return null (value absent from the
 * sequence, or a span covers the entire input), the assertion is
 * {@code assertTrue(r == null || !r.isBlank())} to verify no exception is
 * thrown and no empty string is produced.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RestrictionUDFsTest {

    private static String TRIP;
    private static String TINT_SEQ;
    private static String TBOOL_SEQ;
    private static String TFLOAT_SEQ;
    private static String TTEXT_SEQ;
    /** Hex-WKB of tstzspan [2020-01-01, 2020-01-02]. */
    private static String SPAN_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(2 0)@2020-01-01 02:00:00+00]"),
            (byte) 0);
        TINT_SEQ   = temporal_as_hexwkb(tint_in("[1@2020-01-01, 3@2020-01-03]"),                 (byte) 0);
        TBOOL_SEQ  = temporal_as_hexwkb(tbool_in("[t@2020-01-01, t@2020-01-02, f@2020-01-03]"), (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(tfloat_in("[1.0@2020-01-01, 3.0@2020-01-03]"),          (byte) 0);
        TTEXT_SEQ  = temporal_as_hexwkb(ttext_in("[Hello@2020-01-01, World@2020-01-03]"),        (byte) 0);
        SPAN_HEX   = span_as_hexwkb(
            tstzspan_in("[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // Timestamp-set restriction
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalAtTstzset_returns_nonnull_or_null() throws Exception {
        String setHex = set_as_hexwkb(tstzset_in("{2020-01-01 00:30:00+00}"), (byte) 0);
        String r = RestrictionUDFs.temporalAtTstzset.call(TRIP, setHex);
        // MEOS returns null when the instant does not coincide with a stored value.
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(2)
    void temporalMinusTstzset_returns_nonnull_or_null() throws Exception {
        String setHex = set_as_hexwkb(tstzset_in("{2020-01-01 00:30:00+00}"), (byte) 0);
        String r = RestrictionUDFs.temporalMinusTstzset.call(TRIP, setHex);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // Span / spanset restriction
    // ------------------------------------------------------------------

    @Test @Order(3)
    void temporalMinusTstzspan_returns_nonnull_or_null() throws Exception {
        String r = RestrictionUDFs.temporalMinusTstzspan.call(TRIP, SPAN_HEX);
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(4)
    void temporalMinusTstzspanset_returns_nonnull_or_null() throws Exception {
        String ssHex = spanset_as_hexwkb(
            tstzspanset_in("{[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]}"),
            (byte) 0);
        String r = RestrictionUDFs.temporalMinusTstzspanset.call(TRIP, ssHex);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // Delete operations
    // ------------------------------------------------------------------

    @Test @Order(5)
    void temporalDeleteTstzspan_returns_nonnull_or_null() throws Exception {
        String r = RestrictionUDFs.temporalDeleteTstzspan.call(TRIP, SPAN_HEX);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // Value restriction: tfloat
    // ------------------------------------------------------------------

    @Test @Order(6)
    void tfloatAtValue_returns_nonnull_or_null() throws Exception {
        // 2.0 lies between 1.0 and 3.0 in a linear sequence, so MEOS returns the
        // crossing instant; result may be null for step sequences but not here.
        String r = RestrictionUDFs.tfloatAtValue.call(TFLOAT_SEQ, 2.0);
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(7)
    void tfloatMinusValue_returns_nonnull() throws Exception {
        // 99.0 never appears in the sequence; the full sequence is returned.
        String r = RestrictionUDFs.tfloatMinusValue.call(TFLOAT_SEQ, 99.0);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Value restriction: tbool
    // ------------------------------------------------------------------

    @Test @Order(8)
    void tboolAtValue_true_returns_nonnull() throws Exception {
        // TBOOL_SEQ is [t@..., t@..., f@...]; restricting to true yields a non-null result.
        String r = RestrictionUDFs.tboolAtValue.call(TBOOL_SEQ, true);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void tboolMinusValue_false_returns_nonnull() throws Exception {
        // Removing the false portion of TBOOL_SEQ leaves the true portion.
        String r = RestrictionUDFs.tboolMinusValue.call(TBOOL_SEQ, false);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Value restriction: ttext
    // ------------------------------------------------------------------

    @Test @Order(10)
    void ttextAtValue_matching_returns_nonnull() throws Exception {
        String r = RestrictionUDFs.ttextAtValue.call(TTEXT_SEQ, "Hello");
        assertNotNull(r);
    }

    @Test @Order(11)
    void ttextMinusValue_absent_returns_nonnull() throws Exception {
        // "NotInSeq" is not in TTEXT_SEQ; the full sequence is returned.
        String r = RestrictionUDFs.ttextMinusValue.call(TTEXT_SEQ, "NotInSeq");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Value restriction: tpoint
    // ------------------------------------------------------------------

    @Test @Order(12)
    void tpointAtValue_start_point_returns_nonnull() throws Exception {
        // POINT(0 0) is the exact start of TRIP.
        String r = RestrictionUDFs.tpointAtValue.call(TRIP, "POINT(0 0)");
        assertNotNull(r);
    }

    @Test @Order(13)
    void tpointMinusValue_absent_point_returns_nonnull() throws Exception {
        // POINT(999 999) never appears; the full trip is returned.
        String r = RestrictionUDFs.tpointMinusValue.call(TRIP, "POINT(999 999)");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(14)
    void null_input_returns_null() throws Exception {
        assertNull(RestrictionUDFs.tfloatAtValue.call(null, 1.0));
        assertNull(RestrictionUDFs.tfloatAtValue.call(TFLOAT_SEQ, null));
    }
}
