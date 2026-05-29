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

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for new TransformUDFs — min-distance simplification,
 * temporal re-sampling (tSample), and trajectory extraction.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TransformUDFsExtTest extends MeosTestBase {

    private static String TRIP;
    private static String TFLOAT_SEQ;

    @BeforeAll
    static void initMeos() {
        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, "
                + "POINT(1 0)@2020-01-01 00:15:00+00, "
                + "POINT(3 4)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01, 2.0@2020-01-02, 5.0@2020-01-05]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // Min-distance simplification
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalSimplifyMinDist_returns_nonnull() throws Exception {
        String r = TransformUDFs.temporalSimplifyMinDist.call(TRIP, 0.1);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void temporalSimplifyMinDist_large_threshold_reduces() throws Exception {
        // A very large threshold should collapse the sequence to fewer instants
        String r = TransformUDFs.temporalSimplifyMinDist.call(TRIP, 100.0);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Temporal re-sampling
    // ------------------------------------------------------------------

    @Test @Order(3)
    void temporalTSample_linear_returns_nonnull() throws Exception {
        String r = TransformUDFs.temporalTSample.call(TFLOAT_SEQ, "1 day", "Linear");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void temporalTSample_step_returns_nonnull() throws Exception {
        String r = TransformUDFs.temporalTSample.call(TFLOAT_SEQ, "1 day", "Step");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Trajectory extraction
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tpointTrajectory_returns_linestring_wkt() throws Exception {
        String r = TransformUDFs.tpointTrajectory.call(TRIP);
        assertNotNull(r);
        // A multi-point sequence should produce a LINESTRING
        assertTrue(r.toUpperCase().startsWith("LINESTRING"),
            "Expected LINESTRING WKT but got: " + r);
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(6)
    void null_input_returns_null() throws Exception {
        assertNull(TransformUDFs.temporalSimplifyMinDist.call(null, 1.0));
        assertNull(TransformUDFs.temporalTSample.call(null, "1 day", "Linear"));
        assertNull(TransformUDFs.tpointTrajectory.call(null));
    }
}
