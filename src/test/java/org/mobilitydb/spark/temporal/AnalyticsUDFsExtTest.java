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
 * Unit tests for new AnalyticsUDFs and TransformUDFs additions:
 * tpointCumulativeLength, tgeoTraversedArea,
 * temporalShiftTime, temporalScaleTime.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AnalyticsUDFsExtTest {

    private static String TRIP;
    private static String TFLOAT_SEQ;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TRIP = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(3 4)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01, 5.0@2020-01-05]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // tpointCumulativeLength
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tpointCumulativeLength_returns_nonnull() throws Exception {
        String r = AnalyticsUDFs.tpointCumulativeLength.call(TRIP);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // tgeoTraversedArea
    // ------------------------------------------------------------------

    @Test @Order(2)
    void tgeoTraversedArea_returns_wkt_or_null() throws Exception {
        // tgeompoint (moving point) may return null from traversed_area;
        // the function is primarily for polygon/body temporal types.
        String r = AnalyticsUDFs.tgeoTraversedArea.call(TRIP);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // temporalShiftTime / temporalScaleTime
    // ------------------------------------------------------------------

    @Test @Order(3)
    void temporalShiftTime_returns_nonnull() throws Exception {
        String r = TransformUDFs.temporalShiftTime.call(TFLOAT_SEQ, "1 day");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void temporalScaleTime_returns_nonnull() throws Exception {
        String r = TransformUDFs.temporalScaleTime.call(TFLOAT_SEQ, "10 days");
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(5)
    void null_input_returns_null() throws Exception {
        assertNull(AnalyticsUDFs.tpointCumulativeLength.call(null));
        assertNull(AnalyticsUDFs.tgeoTraversedArea.call(null));
        assertNull(TransformUDFs.temporalShiftTime.call(null, "1 day"));
        assertNull(TransformUDFs.temporalScaleTime.call(null, "1 day"));
    }
}
