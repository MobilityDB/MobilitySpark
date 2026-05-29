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
 * Unit tests for MoreAccessorUDFs — subtype, instant/sequence navigation,
 * timestampN, inclusivity flags, duration, type-specific valueN accessors,
 * and tpoint geometry accessors.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MoreAccessorUDFsTest extends MeosTestBase {

    /** TSequence with 3 instants — covers instant/sequence navigation. */
    private static String TRIP;
    private static String TINT_SEQ;
    private static String TBOOL_SEQ;
    private static String TFLOAT_SEQ;
    private static String TTEXT_SEQ;

    @BeforeAll
    static void initMeos() {
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(1 0)@2020-01-01 01:00:00+00,"
                        + " POINT(2 0)@2020-01-02 00:00:00+00]"),
            (byte) 0);
        TINT_SEQ   = temporal_as_hexwkb(tint_in("[1@2020-01-01, 3@2020-01-03]"),                    (byte) 0);
        TBOOL_SEQ  = temporal_as_hexwkb(tbool_in("[t@2020-01-01, t@2020-01-02, f@2020-01-03]"),    (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(tfloat_in("[1.5@2020-01-01, 3.5@2020-01-02]"),             (byte) 0);
        TTEXT_SEQ  = temporal_as_hexwkb(ttext_in("[AAA@2020-01-01, ZZZ@2020-01-03]"),              (byte) 0);
    }

    // ------------------------------------------------------------------
    // Subtype
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalSubtype_tsequence_returns_Sequence() throws Exception {
        String r = MoreAccessorUDFs.temporalSubtype.call(TRIP);
        assertNotNull(r);
        assertTrue(r.contains("Sequence"));
    }

    // ------------------------------------------------------------------
    // Instant navigation
    // ------------------------------------------------------------------

    @Test @Order(2)
    void startInstant_returns_nonnull_hexwkb() throws Exception {
        String r = MoreAccessorUDFs.startInstant.call(TRIP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void endInstant_returns_nonnull_hexwkb() throws Exception {
        String r = MoreAccessorUDFs.endInstant.call(TRIP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void instantN_returns_nonnull_hexwkb() throws Exception {
        String r = MoreAccessorUDFs.instantN.call(TRIP, 1);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Sequence navigation
    // ------------------------------------------------------------------

    @Test @Order(5)
    void startSequence_returns_nonnull_hexwkb() throws Exception {
        String r = MoreAccessorUDFs.startSequence.call(TRIP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void endSequence_returns_nonnull_hexwkb() throws Exception {
        String r = MoreAccessorUDFs.endSequence.call(TRIP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void sequenceN_returns_nonnull_hexwkb() throws Exception {
        String r = MoreAccessorUDFs.sequenceN.call(TRIP, 1);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Min/max instant
    // ------------------------------------------------------------------

    @Test @Order(8)
    void minInstant_returns_nonnull_hexwkb() throws Exception {
        String r = MoreAccessorUDFs.minInstant.call(TINT_SEQ);
        assertNotNull(r);
    }

    @Test @Order(9)
    void maxInstant_returns_nonnull_hexwkb() throws Exception {
        String r = MoreAccessorUDFs.maxInstant.call(TINT_SEQ);
        assertNotNull(r);
    }

    // ------------------------------------------------------------------
    // Timestamp accessors
    // ------------------------------------------------------------------

    @Test @Order(10)
    void numTimestamps_returns_positive_int() throws Exception {
        Integer r = MoreAccessorUDFs.numTimestamps.call(TRIP);
        assertNotNull(r);
        assertTrue(r >= 1);
    }

    @Test @Order(11)
    void timestampN_returns_nonnull_timestamp() throws Exception {
        java.sql.Timestamp r = MoreAccessorUDFs.timestampN.call(TRIP, 1);
        assertNotNull(r);
    }

    // ------------------------------------------------------------------
    // Inclusivity flags
    // ------------------------------------------------------------------

    @Test @Order(12)
    void lowerInc_returns_boolean() throws Exception {
        Boolean r = MoreAccessorUDFs.lowerInc.call(TRIP);
        assertNotNull(r);
    }

    @Test @Order(13)
    void upperInc_returns_boolean() throws Exception {
        Boolean r = MoreAccessorUDFs.upperInc.call(TRIP);
        assertNotNull(r);
    }

    // ------------------------------------------------------------------
    // Duration
    // ------------------------------------------------------------------

    @Test @Order(14)
    void duration_returns_nonnull_string() throws Exception {
        String r = MoreAccessorUDFs.duration.call(TRIP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // tbool value accessor
    // ------------------------------------------------------------------

    @Test @Order(15)
    void tboolValueN_returns_boolean() throws Exception {
        Boolean r = MoreAccessorUDFs.tboolValueN.call(TBOOL_SEQ, 1);
        assertNotNull(r);
    }

    // ------------------------------------------------------------------
    // tfloat value accessor
    // ------------------------------------------------------------------

    @Test @Order(16)
    void tfloatValueN_returns_double() throws Exception {
        Double r = MoreAccessorUDFs.tfloatValueN.call(TFLOAT_SEQ, 1);
        assertNotNull(r);
        assertTrue(r >= 0.0);
    }

    // ------------------------------------------------------------------
    // ttext value accessors
    // ------------------------------------------------------------------

    @Test @Order(17)
    void ttextMinValue_returns_string() throws Exception {
        String r = MoreAccessorUDFs.ttextMinValue.call(TTEXT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(18)
    void ttextMaxValue_returns_string() throws Exception {
        String r = MoreAccessorUDFs.ttextMaxValue.call(TTEXT_SEQ);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(19)
    void ttextValueN_returns_string() throws Exception {
        String r = MoreAccessorUDFs.ttextValueN.call(TTEXT_SEQ, 1);
        assertNotNull(r);
    }

    // ------------------------------------------------------------------
    // tpoint accessors
    // ------------------------------------------------------------------

    @Test @Order(20)
    void tpointSrid_returns_int() throws Exception {
        Integer r = MoreAccessorUDFs.tpointSrid.call(TRIP);
        assertNotNull(r);
    }

    @Test @Order(21)
    void tpointValueN_returns_wkt() throws Exception {
        String r = MoreAccessorUDFs.tpointValueN.call(TRIP, 1);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(22)
    void tpointConvexHull_returns_wkt() throws Exception {
        String r = MoreAccessorUDFs.tpointConvexHull.call(TRIP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(23)
    void startInstant_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.startInstant.call(null));
    }

    @Test @Order(24)
    void timestampN_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.timestampN.call(null, 1));
    }
}
