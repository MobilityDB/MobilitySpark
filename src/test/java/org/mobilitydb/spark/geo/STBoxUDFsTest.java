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

package org.mobilitydb.spark.geo;

import org.junit.jupiter.api.*;
import org.mobilitydb.spark.temporal.ConstructorUDFs;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for STBoxUDFs — STBox accessor and expansion operations.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * Input STBox values are produced via ConstructorUDFs.stbox (which handles
 * the scratch-Pointer allocation required by stbox_as_hexwkb).
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class STBoxUDFsTest {

    // STBOX XT([-1,1],[-2,2],[2020-01-01,2020-01-02])
    private static String STBOX_XT;
    // STBOX T([2020-01-01,2020-01-02]) — temporal-only box
    private static String STBOX_T;

    @BeforeAll
    static void initMeos() throws Exception {
        meos_initialize();
        meos_initialize_timezone("UTC");

        STBOX_XT = ConstructorUDFs.stbox.call(
            "STBOX XT(((-1,-2),(1,2)),[2020-01-01 00:00:00+00,2020-01-02 00:00:00+00])");
        STBOX_T = ConstructorUDFs.stbox.call(
            "STBOX T([2020-01-01 00:00:00+00,2020-01-02 00:00:00+00])");
    }

    @Test @Order(1)
    void stboxHasx_spatial_box_returns_true() throws Exception {
        assertTrue(STBoxUDFs.stboxHasx.call(STBOX_XT));
    }

    @Test @Order(2)
    void stboxHasx_temporal_only_box_returns_false() throws Exception {
        assertFalse(STBoxUDFs.stboxHasx.call(STBOX_T));
    }

    @Test @Order(3)
    void stboxHast_spatial_temporal_box_returns_true() throws Exception {
        assertTrue(STBoxUDFs.stboxHast.call(STBOX_XT));
    }

    @Test @Order(4)
    void stboxHasz_2d_box_returns_false() throws Exception {
        assertFalse(STBoxUDFs.stboxHasz.call(STBOX_XT));
    }

    @Test @Order(5)
    void stboxXmin_returns_minus_one() throws Exception {
        Double v = STBoxUDFs.stboxXmin.call(STBOX_XT);
        assertNotNull(v);
        assertEquals(-1.0, v, 1e-9);
    }

    @Test @Order(6)
    void stboxXmax_returns_one() throws Exception {
        Double v = STBoxUDFs.stboxXmax.call(STBOX_XT);
        assertNotNull(v);
        assertEquals(1.0, v, 1e-9);
    }

    @Test @Order(7)
    void stboxYmin_returns_minus_two() throws Exception {
        Double v = STBoxUDFs.stboxYmin.call(STBOX_XT);
        assertNotNull(v);
        assertEquals(-2.0, v, 1e-9);
    }

    @Test @Order(8)
    void stboxYmax_returns_two() throws Exception {
        Double v = STBoxUDFs.stboxYmax.call(STBOX_XT);
        assertNotNull(v);
        assertEquals(2.0, v, 1e-9);
    }

    @Test @Order(9)
    void stboxZmin_no_z_returns_null() throws Exception {
        assertNull(STBoxUDFs.stboxZmin.call(STBOX_XT));
    }

    @Test @Order(10)
    void stboxTmin_returns_2020_01_01() throws Exception {
        java.sql.Timestamp ts = STBoxUDFs.stboxTmin.call(STBOX_XT);
        assertNotNull(ts, "stboxTmin should not be null for an XT box");
        assertTrue(ts.toInstant().toString().startsWith("2020-01-01"),
            "Expected 2020-01-01, got: " + ts.toInstant());
    }

    @Test @Order(11)
    void stboxTmax_returns_2020_01_02() throws Exception {
        java.sql.Timestamp ts = STBoxUDFs.stboxTmax.call(STBOX_XT);
        assertNotNull(ts);
        assertTrue(ts.toInstant().toString().startsWith("2020-01-02"),
            "Expected 2020-01-02, got: " + ts.toInstant());
    }

    @Test @Order(12)
    void stboxTminInc_closed_lower_returns_true() throws Exception {
        Boolean inc = STBoxUDFs.stboxTminInc.call(STBOX_XT);
        assertNotNull(inc);
        assertTrue(inc);
    }

    @Test @Order(13)
    void stboxTmaxInc_closed_upper_returns_true() throws Exception {
        Boolean inc = STBoxUDFs.stboxTmaxInc.call(STBOX_XT);
        assertNotNull(inc);
        assertTrue(inc);
    }

    @Test @Order(14)
    void stboxSrid_zero_for_no_srid() throws Exception {
        Integer srid = STBoxUDFs.stboxSrid.call(STBOX_XT);
        assertNotNull(srid);
        assertEquals(0, srid);
    }

    @Test @Order(15)
    void stboxXmin_null_input_returns_null() throws Exception {
        assertNull(STBoxUDFs.stboxXmin.call(null));
    }

    @Test @Order(16)
    void stboxExpandSpace_xmax_grows() throws Exception {
        String expanded = STBoxUDFs.stboxExpandSpace.call(STBOX_XT, 1.0);
        assertNotNull(expanded, "stboxExpandSpace should return a hex-WKB");
        Double xmax = STBoxUDFs.stboxXmax.call(expanded);
        assertNotNull(xmax);
        assertTrue(xmax > 1.0, "Expanded Xmax should exceed original 1.0, got " + xmax);
    }

    @Test @Order(17)
    void stboxExpandTime_returns_non_null_hex() throws Exception {
        String expanded = STBoxUDFs.stboxExpandTime.call(STBOX_XT, "1 day");
        assertNotNull(expanded, "stboxExpandTime should return a hex-WKB");
        assertFalse(expanded.isBlank());
    }
}
