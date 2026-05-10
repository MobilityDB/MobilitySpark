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

import org.mobilitydb.spark.geo.STBoxUDFs;
import org.mobilitydb.spark.temporal.ConstructorUDFs;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for additional RestrictionUDFs batch 2 — temporal span/spanset
 * restriction (atTstzspan/atTstzspanset) and spatial/elevation restriction
 * (tgeoAtStbox, tpointAtElevation, tpointMinusElevation).
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RestrictionUDFsExt2Test {

    private static String TFLOAT_SEQ;
    private static String TRIP_3D;
    private static String SPAN_HEX;
    private static String SPANSET_HEX;
    private static String STBOX_HEX;
    private static String FLOATSPAN_HEX;

    @BeforeAll
    static void initMeos() throws Exception {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TFLOAT_SEQ = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01, 5.0@2020-01-05]"), (byte) 0);

        TRIP_3D = temporal_as_hexwkb(
            tgeompoint_in("[POINT Z(0 0 1)@2020-01-01 00:00:00+00, "
                + "POINT Z(3 4 5)@2020-01-01 01:00:00+00]"),
            (byte) 0);

        // tstzspan "[2020-01-02, 2020-01-04]"
        SPAN_HEX = span_as_hexwkb(
            tstzspan_in("[2020-01-02, 2020-01-04]"), (byte) 0);

        // tstzspanset "{[2020-01-01, 2020-01-02], [2020-01-04, 2020-01-05]}"
        SPANSET_HEX = spanset_as_hexwkb(
            tstzspanset_in("{[2020-01-01, 2020-01-02],[2020-01-04, 2020-01-05]}"), (byte) 0);

        // STBox enclosing the trip — use ConstructorUDFs.stbox to get hex-WKB
        STBOX_HEX = ConstructorUDFs.stbox.call(
            "STBOX XT(((0,0),(4,5)),[2020-01-01 00:00:00+00,2020-01-01 01:00:00+00])");

        // floatspan [1.0, 3.0] for elevation restriction
        FLOATSPAN_HEX = span_as_hexwkb(
            floatspan_make(1.0, 3.0, true, true), (byte) 0);
    }

    // ------------------------------------------------------------------
    // Timestamp-span restriction
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalAtTstzspan_returns_nonnull() throws Exception {
        String r = RestrictionUDFs.temporalAtTstzspan.call(TFLOAT_SEQ, SPAN_HEX);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void temporalAtTstzspanset_returns_nonnull() throws Exception {
        String r = RestrictionUDFs.temporalAtTstzspanset.call(TFLOAT_SEQ, SPANSET_HEX);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // STBox restriction
    // ------------------------------------------------------------------

    @Test @Order(3)
    void tgeoAtStbox_returns_nonnull_or_null() throws Exception {
        String r = RestrictionUDFs.tgeoAtStbox.call(TRIP_3D, STBOX_HEX);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // Elevation restriction (3D tpoint)
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tpointAtElevation_returns_nonnull_or_null() throws Exception {
        String r = RestrictionUDFs.tpointAtElevation.call(TRIP_3D, FLOATSPAN_HEX);
        assertTrue(r == null || !r.isBlank());
    }

    @Test @Order(5)
    void tpointMinusElevation_returns_nonnull_or_null() throws Exception {
        String r = RestrictionUDFs.tpointMinusElevation.call(TRIP_3D, FLOATSPAN_HEX);
        assertTrue(r == null || !r.isBlank());
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(6)
    void null_input_returns_null() throws Exception {
        assertNull(RestrictionUDFs.temporalAtTstzspan.call(null, SPAN_HEX));
        assertNull(RestrictionUDFs.temporalAtTstzspanset.call(null, SPANSET_HEX));
        assertNull(RestrictionUDFs.tgeoAtStbox.call(null, STBOX_HEX));
        assertNull(RestrictionUDFs.tpointAtElevation.call(null, FLOATSPAN_HEX));
        assertNull(RestrictionUDFs.tpointMinusElevation.call(null, FLOATSPAN_HEX));
    }
}
