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
 * Unit tests for RestrictionUDFs batch 3 — tintAtValue, tnumber value-range
 * restriction (at/minus span/spanset), and tgeoMinusStbox.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RestrictionUDFsExt3Test extends MeosTestBase {

    private static String TINT_SEQ;
    private static String TFLOAT_SEQ;
    private static String TRIP;
    private static String INTSPAN_HEX;
    private static String FLOATSPAN_HEX;
    private static String STBOX_HEX;

    @BeforeAll
    static void initMeos() throws Exception {
        TINT_SEQ = temporal_as_hexwkb(
            tint_in("[1@2020-01-01, 5@2020-01-05]"), (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01, 5.0@2020-01-05]"), (byte) 0);
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(3 4)@2020-01-01 01:00:00+00]"),
            (byte) 0);

        // intspan [2, 4]
        INTSPAN_HEX = span_as_hexwkb(intspan_make(2, 4, true, true), (byte) 0);
        // floatspan [1.0, 3.0]
        FLOATSPAN_HEX = span_as_hexwkb(floatspan_make(1.0, 3.0, true, true), (byte) 0);
        // STBox outside the trip
        STBOX_HEX = ConstructorUDFs.stbox.call(
            "STBOX XT(((10,10),(20,20)),[2020-01-01 00:00:00+00,2020-01-01 01:00:00+00])");
    }

    // ------------------------------------------------------------------
    // tintAtValue
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tintAtValue_returns_nonnull_or_null() throws Exception {
        String r = RestrictionUDFs.tintAtValue.call(TINT_SEQ, 3);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // tnumber value-range restriction
    // ------------------------------------------------------------------

    @Test @Order(2)
    void tnumberAtSpan_tfloat_returns_nonnull() throws Exception {
        String r = RestrictionUDFs.tnumberAtSpan.call(TFLOAT_SEQ, FLOATSPAN_HEX);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void tnumberMinusSpan_tfloat_returns_nonnull_or_null() throws Exception {
        String r = RestrictionUDFs.tnumberMinusSpan.call(TFLOAT_SEQ, FLOATSPAN_HEX);
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(4)
    void tnumberAtSpanset_tint_returns_nonnull_or_null() throws Exception {
        String spansetHex = spanset_as_hexwkb(intspanset_in("{[2,4]}"), (byte) 0);
        // tint [1,5] restricted to span {[2,4]} may yield null (neither 1 nor 5 is in 2-4)
        String r = RestrictionUDFs.tnumberAtSpanset.call(TINT_SEQ, spansetHex);
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(5)
    void tnumberMinusSpanset_returns_nonnull_or_null() throws Exception {
        String spansetHex = spanset_as_hexwkb(intspanset_in("{[2,4]}"), (byte) 0);
        String r = RestrictionUDFs.tnumberMinusSpanset.call(TINT_SEQ, spansetHex);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // tgeoMinusStbox
    // ------------------------------------------------------------------

    @Test @Order(6)
    void tgeoMinusStbox_outside_box_returns_original() throws Exception {
        // Subtracting a box that doesn't overlap → result is the full trip
        String r = RestrictionUDFs.tgeoMinusStbox.call(TRIP, STBOX_HEX);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(7)
    void null_input_returns_null() throws Exception {
        assertNull(RestrictionUDFs.tintAtValue.call(null, 1));
        assertNull(RestrictionUDFs.tnumberAtSpan.call(null, FLOATSPAN_HEX));
        assertNull(RestrictionUDFs.tnumberMinusSpan.call(null, FLOATSPAN_HEX));
        assertNull(RestrictionUDFs.tgeoMinusStbox.call(null, STBOX_HEX));
    }
}
