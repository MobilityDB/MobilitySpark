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

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.junit.jupiter.api.*;
import org.mobilitydb.spark.geo.STBoxUDFs;

import java.sql.Timestamp;
import org.mobilitydb.spark.MeosTestBase;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for new STBoxUDFs constructors:
 *   geoToStbox, tstzspanToStbox, timestamptzToStbox.
 *
 * MEOS function authority: meos/include/meos_geo.h, meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class STBoxUDFsExt2Test extends MeosTestBase {

    private static String TSTZSPAN_HEX;
    private static String STBOX_A_HEX;  // STBox with spatial + time component
    private static String STBOX_B_HEX;  // STBox strictly right of STBOX_A in X

    @BeforeAll
    static void initMeos() {
        TSTZSPAN_HEX = span_as_hexwkb(
            tstzspan_in("[2020-01-01 00:00:00+00, 2020-01-03 00:00:00+00]"), (byte) 0);

        // Build spatial-only STBoxes from point geometries.
        // STBOX_A: centred at (0,0), STBOX_B: centred at (20,0) — strictly right
        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        STBOX_A_HEX = stbox_as_hexwkb(geo_to_stbox(geo_from_text("POINT(0 0)", 0)), (byte) 0, sizeOut);
        sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        STBOX_B_HEX = stbox_as_hexwkb(geo_to_stbox(geo_from_text("POINT(20 0)", 0)), (byte) 0, sizeOut);
    }

    // ------------------------------------------------------------------
    // geoToStbox
    // ------------------------------------------------------------------

    @Test @Order(1)
    void geoToStbox_returns_nonnull_hexwkb() throws Exception {
        String r = STBoxUDFs.geoToStbox.call("POINT(4.35 50.85)");
        assertNotNull(r, "geoToStbox must return non-null for valid WKT");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void geoToStbox_polygon_returns_nonnull() throws Exception {
        String r = STBoxUDFs.geoToStbox.call("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        assertNotNull(r, "geoToStbox must return non-null for polygon WKT");
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void geoToStbox_null_returns_null() throws Exception {
        assertNull(STBoxUDFs.geoToStbox.call(null));
    }

    // ------------------------------------------------------------------
    // tstzspanToStbox
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tstzspanToStbox_returns_nonnull_hexwkb() throws Exception {
        String r = STBoxUDFs.tstzspanToStbox.call(TSTZSPAN_HEX);
        assertNotNull(r, "tstzspanToStbox must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(5)
    void tstzspanToStbox_null_returns_null() throws Exception {
        assertNull(STBoxUDFs.tstzspanToStbox.call(null));
    }

    // ------------------------------------------------------------------
    // timestamptzToStbox
    // ------------------------------------------------------------------

    @Test @Order(6)
    void timestamptzToStbox_returns_nonnull_hexwkb() throws Exception {
        Timestamp ts = new Timestamp(1577836800000L); // 2020-01-01 00:00:00 UTC
        String r = STBoxUDFs.timestamptzToStbox.call(ts);
        assertNotNull(r, "timestamptzToStbox must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void timestamptzToStbox_null_returns_null() throws Exception {
        assertNull(STBoxUDFs.timestamptzToStbox.call((Timestamp) null));
    }

    // ------------------------------------------------------------------
    // intersectionStboxStbox / unionStboxStbox
    // ------------------------------------------------------------------

    @Test @Order(8)
    void intersectionStboxStbox_same_box_returns_nonnull() throws Exception {
        String box = STBoxUDFs.tstzspanToStbox.call(TSTZSPAN_HEX);
        assertNotNull(box);
        String r = STBoxUDFs.intersectionStboxStbox.call(box, box);
        assertNotNull(r, "intersection of a box with itself must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void unionStboxStbox_returns_nonnull() throws Exception {
        String box = STBoxUDFs.tstzspanToStbox.call(TSTZSPAN_HEX);
        assertNotNull(box);
        String r = STBoxUDFs.unionStboxStbox.call(box, box);
        assertNotNull(r, "union of a box with itself must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(10)
    void intersectionStboxStbox_null_returns_null() throws Exception {
        String box = STBoxUDFs.tstzspanToStbox.call(TSTZSPAN_HEX);
        assertNull(STBoxUDFs.intersectionStboxStbox.call(null, box));
        assertNull(STBoxUDFs.intersectionStboxStbox.call(box, null));
    }

    // ------------------------------------------------------------------
    // STBox positional predicates
    // ------------------------------------------------------------------

    @Test @Order(11)
    void stboxLeft_a_is_left_of_b() throws Exception {
        assertNotNull(STBOX_A_HEX, "STBOX_A_HEX setup required");
        assertNotNull(STBOX_B_HEX, "STBOX_B_HEX setup required");
        assertTrue(STBoxUDFs.stboxLeft.call(STBOX_A_HEX, STBOX_B_HEX),
            "stbox at (0,0) must be left of stbox at (20,0)");
    }

    @Test @Order(12)
    void stboxRight_b_is_right_of_a() throws Exception {
        assertTrue(STBoxUDFs.stboxRight.call(STBOX_B_HEX, STBOX_A_HEX),
            "stbox at (20,0) must be right of stbox at (0,0)");
    }

    @Test @Order(13)
    void stboxLeft_null_returns_null() throws Exception {
        assertNull(STBoxUDFs.stboxLeft.call(null, STBOX_B_HEX));
        assertNull(STBoxUDFs.stboxLeft.call(STBOX_A_HEX, null));
    }

    // ------------------------------------------------------------------
    // STBox topology predicates
    // ------------------------------------------------------------------

    @Test @Order(14)
    void stboxContains_same_box_returns_true() throws Exception {
        assertNotNull(STBOX_A_HEX);
        assertTrue(STBoxUDFs.stboxContains.call(STBOX_A_HEX, STBOX_A_HEX),
            "an stbox contains itself");
    }

    @Test @Order(15)
    void stboxContained_same_box_returns_true() throws Exception {
        assertTrue(STBoxUDFs.stboxContained.call(STBOX_A_HEX, STBOX_A_HEX),
            "an stbox is contained in itself");
    }

    @Test @Order(16)
    void stboxOverlaps_same_box_returns_true() throws Exception {
        assertTrue(STBoxUDFs.stboxOverlaps.call(STBOX_A_HEX, STBOX_A_HEX),
            "an stbox overlaps itself");
    }

    @Test @Order(17)
    void stboxContains_null_returns_null() throws Exception {
        assertNull(STBoxUDFs.stboxContains.call(null, STBOX_A_HEX));
        assertNull(STBoxUDFs.stboxContains.call(STBOX_A_HEX, null));
    }
}
