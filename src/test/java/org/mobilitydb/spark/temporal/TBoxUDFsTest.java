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
 * Unit tests for TBoxUDFs — TBox accessor and span-conversion operations.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TBoxUDFsTest extends MeosTestBase {

    // TBOX XT([1,10],[2020-01-01,2020-01-10])
    private static String TBOX_XT;
    // TBOX T([2020-01-01,2020-01-10]) — temporal only
    private static String TBOX_T;

    @BeforeAll
    static void initMeos() throws Exception {
        TBOX_XT = ConstructorUDFs.tbox.call(
            "TBOX XT([1,10],[2020-01-01 00:00:00+00,2020-01-10 00:00:00+00])");
        TBOX_T = ConstructorUDFs.tbox.call(
            "TBOX T([2020-01-01 00:00:00+00,2020-01-10 00:00:00+00])");
    }

    // ------------------------------------------------------------------
    // Has-component flags
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tboxHasx_xt_box_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxHasx.call(TBOX_XT));
    }

    @Test @Order(2)
    void tboxHasx_t_only_box_returns_false() throws Exception {
        assertFalse(TBoxUDFs.tboxHasx.call(TBOX_T));
    }

    @Test @Order(3)
    void tboxHast_xt_box_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxHast.call(TBOX_XT));
    }

    // ------------------------------------------------------------------
    // Numeric bound accessors
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tboxXmin_returns_one() throws Exception {
        Double xmin = TBoxUDFs.tboxXmin.call(TBOX_XT);
        assertNotNull(xmin);
        assertEquals(1.0, xmin, 1e-9);
    }

    @Test @Order(5)
    void tboxXmax_returns_ten() throws Exception {
        Double xmax = TBoxUDFs.tboxXmax.call(TBOX_XT);
        assertNotNull(xmax);
        assertEquals(10.0, xmax, 1e-9);
    }

    @Test @Order(6)
    void tboxXmin_t_only_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxXmin.call(TBOX_T));
    }

    @Test @Order(7)
    void tboxXminInc_closed_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxXminInc.call(TBOX_XT));
    }

    @Test @Order(8)
    void tboxXmaxInc_closed_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxXmaxInc.call(TBOX_XT));
    }

    // ------------------------------------------------------------------
    // Temporal bound accessors
    // ------------------------------------------------------------------

    @Test @Order(9)
    void tboxTmin_returns_2020_01_01() throws Exception {
        java.sql.Timestamp ts = TBoxUDFs.tboxTmin.call(TBOX_XT);
        assertNotNull(ts);
        assertTrue(ts.toString().startsWith("2020-01-01"),
            "Expected 2020-01-01, got: " + ts);
    }

    @Test @Order(10)
    void tboxTmax_returns_2020_01_10() throws Exception {
        java.sql.Timestamp ts = TBoxUDFs.tboxTmax.call(TBOX_XT);
        assertNotNull(ts);
        assertTrue(ts.toString().startsWith("2020-01-10"),
            "Expected 2020-01-10, got: " + ts);
    }

    @Test @Order(11)
    void tboxTminInc_closed_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxTminInc.call(TBOX_XT));
    }

    @Test @Order(12)
    void tboxTmaxInc_closed_returns_true() throws Exception {
        assertTrue(TBoxUDFs.tboxTmaxInc.call(TBOX_XT));
    }

    // ------------------------------------------------------------------
    // Span conversions
    // ------------------------------------------------------------------

    @Test @Order(13)
    void tboxToFloatspan_returns_hex() throws Exception {
        String hex = TBoxUDFs.tboxToFloatspan.call(TBOX_XT);
        assertNotNull(hex);
        assertFalse(hex.isBlank());
    }

    @Test @Order(14)
    void tboxToTstzspan_returns_hex() throws Exception {
        String hex = TBoxUDFs.tboxToTstzspan.call(TBOX_XT);
        assertNotNull(hex);
        assertFalse(hex.isBlank());
    }

    @Test @Order(15)
    void tboxToFloatspan_t_only_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxToFloatspan.call(TBOX_T));
    }

    // ------------------------------------------------------------------
    // Null-input guards
    // ------------------------------------------------------------------

    @Test @Order(16)
    void null_input_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxHasx.call(null));
        assertNull(TBoxUDFs.tboxHast.call(null));
        assertNull(TBoxUDFs.tboxXmin.call(null));
        assertNull(TBoxUDFs.tboxXmax.call(null));
        assertNull(TBoxUDFs.tboxTmin.call(null));
        assertNull(TBoxUDFs.tboxTmax.call(null));
        assertNull(TBoxUDFs.tboxToFloatspan.call(null));
        assertNull(TBoxUDFs.tboxToTstzspan.call(null));
    }
}
