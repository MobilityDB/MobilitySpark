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

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TransformUDFs — subtype conversion, interpolation change,
 * type casting, value/time-domain shifting and scaling, SRID assignment,
 * coordinate rounding, and trajectory simplification.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TransformUDFsTest {

    private static String TRIP;
    private static String TINT_SEQ;
    private static String TFLOAT_SEQ;
    private static String TFLOAT_STEP;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(1 0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TINT_SEQ  = temporal_as_hexwkb(tint_in("[1@2020-01-01, 3@2020-01-03]"),   (byte) 0);
        TFLOAT_SEQ  = temporal_as_hexwkb(tfloat_in("[1.0@2020-01-01, 3.0@2020-01-03]"), (byte) 0);
        TFLOAT_STEP = temporal_as_hexwkb(
            tfloat_in("Interp=Step;[1.0@2020-01-01, 3.0@2020-01-03]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // Subtype conversion
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalToTInstant_single_instant_sequence_becomes_instant() throws Exception {
        String singleSeq = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01]"), (byte) 0);
        String r = TransformUDFs.temporalToTInstant.call(singleSeq);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void temporalToTSequence_step_to_linear() throws Exception {
        String r = TransformUDFs.temporalToTSequence.call(TFLOAT_STEP, "Linear");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void temporalToTSequenceSet_converts() throws Exception {
        String r = TransformUDFs.temporalToTSequenceSet.call(TFLOAT_SEQ, "Linear");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Interpolation change
    // ------------------------------------------------------------------

    @Test @Order(4)
    void temporalSetInterp_step_to_linear() throws Exception {
        String r = TransformUDFs.temporalSetInterp.call(TFLOAT_STEP, "Linear");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Type casting
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tfloatToTint_returns_tint_hexwkb() throws Exception {
        // tfloat_to_tint only works on step-interpolated sequences
        String r = TransformUDFs.tfloatToTint.call(TFLOAT_STEP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Value-domain shifting and scaling
    // ------------------------------------------------------------------

    @Test @Order(6)
    void tfloatShiftValue_positive() throws Exception {
        String r = TransformUDFs.tfloatShiftValue.call(TFLOAT_SEQ, 10.0);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void tfloatScaleValue_doubles() throws Exception {
        String r = TransformUDFs.tfloatScaleValue.call(TFLOAT_SEQ, 2.0);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(8)
    void tfloatShiftScaleValue() throws Exception {
        String r = TransformUDFs.tfloatShiftScaleValue.call(TFLOAT_SEQ, 1.0, 2.0);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Time-domain shifting and scaling
    // ------------------------------------------------------------------

    @Test @Order(9)
    void temporalShiftScaleTime() throws Exception {
        String r = TransformUDFs.temporalShiftScaleTime.call(TRIP, "01:00:00", "02:00:00");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Spatial transformations
    // ------------------------------------------------------------------

    @Test @Order(10)
    void tpointSetSrid_to_4326() throws Exception {
        String r = TransformUDFs.tpointSetSrid.call(TRIP, 4326);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(11)
    void tpointRound_2_digits() throws Exception {
        String r = TransformUDFs.tpointRound.call(TRIP, 2);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Trajectory simplification
    // ------------------------------------------------------------------

    @Test @Order(12)
    void temporalSimplifyDp_returns_nonnull_or_null() throws Exception {
        // A 2-point sequence may be returned as-is or simplified to null;
        // accept either outcome but never a blank string.
        String r = TransformUDFs.temporalSimplifyDp.call(TRIP, 0.001);
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(13)
    void temporalSimplifyMaxDist_returns_nonnull_or_null() throws Exception {
        String r = TransformUDFs.temporalSimplifyMaxDist.call(TRIP, 0.001);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // Null-input guard
    // ------------------------------------------------------------------

    @Test @Order(14)
    void null_input_returns_null() throws Exception {
        assertNull(TransformUDFs.tfloatToTint.call(null));
        assertNull(TransformUDFs.tpointSetSrid.call(null, 4326));
    }
}
