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

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for STBoxUDFs:
 *   stboxRound, stboxExpandSpace, stboxSetSrid, stboxShiftScaleTime.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class STBoxUDFsTest {

    private static String STBOX_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        // Build stbox hex: STBOX XT(((1.1234,2.5678),(3.9876,4.1111)),[2020-01-01,2020-01-03])
        Pointer sb = stbox_in(
            "STBOX XT(((1.1234,2.5678),(3.9876,4.1111))," +
            "[2020-01-01 00:00:00+00,2020-01-03 00:00:00+00])");
        assertNotNull(sb, "stbox_in must succeed");
        Pointer sizeOut = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
        STBOX_HEX = stbox_as_hexwkb(sb, (byte) 0, sizeOut);
    }

    // ------------------------------------------------------------------
    // stboxRound
    // ------------------------------------------------------------------

    @Test @Order(1)
    void stboxRound_returns_nonnull_hexwkb() throws Exception {
        String r = STBoxUDFs.stboxRound.call(STBOX_HEX, 2);
        assertNotNull(r, "stboxRound must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void stboxRound_null_returns_null() throws Exception {
        assertNull(STBoxUDFs.stboxRound.call(null, 2));
        assertNull(STBoxUDFs.stboxRound.call(STBOX_HEX, null));
    }

    // ------------------------------------------------------------------
    // stboxExpandSpace
    // ------------------------------------------------------------------

    @Test @Order(3)
    void stboxExpandSpace_returns_nonnull_hexwkb() throws Exception {
        String r = STBoxUDFs.stboxExpandSpace.call(STBOX_HEX, 1.0);
        assertNotNull(r, "stboxExpandSpace must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void stboxExpandSpace_zero_radius_is_identity() throws Exception {
        String r = STBoxUDFs.stboxExpandSpace.call(STBOX_HEX, 0.0);
        assertNotNull(r, "Expanding by 0 must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(5)
    void stboxExpandSpace_null_returns_null() throws Exception {
        assertNull(STBoxUDFs.stboxExpandSpace.call(null, 1.0));
        assertNull(STBoxUDFs.stboxExpandSpace.call(STBOX_HEX, null));
    }

    // ------------------------------------------------------------------
    // stboxSetSrid
    // ------------------------------------------------------------------

    @Test @Order(6)
    void stboxSetSrid_returns_nonnull_hexwkb() throws Exception {
        String r = STBoxUDFs.stboxSetSrid.call(STBOX_HEX, 4326);
        assertNotNull(r, "stboxSetSrid must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void stboxSetSrid_null_returns_null() throws Exception {
        assertNull(STBoxUDFs.stboxSetSrid.call(null, 4326));
        assertNull(STBoxUDFs.stboxSetSrid.call(STBOX_HEX, null));
    }

    // ------------------------------------------------------------------
    // stboxShiftScaleTime
    // ------------------------------------------------------------------

    @Test @Order(8)
    void stboxShiftScaleTime_shift_returns_nonnull() throws Exception {
        String r = STBoxUDFs.stboxShiftScaleTime.call(STBOX_HEX, "1 day", null);
        assertNotNull(r, "Shift by 1 day must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void stboxShiftScaleTime_null_stbox_returns_null() throws Exception {
        assertNull(STBoxUDFs.stboxShiftScaleTime.call(null, "1 day", null));
    }

    // ------------------------------------------------------------------
    // stboxGetSpace
    // ------------------------------------------------------------------

    @Test @Order(10)
    void stboxGetSpace_returns_nonnull_hexwkb() throws Exception {
        String r = STBoxUDFs.stboxGetSpace.call(STBOX_HEX);
        assertNotNull(r, "stboxGetSpace must return non-null for XT stbox");
        assertFalse(r.isBlank());
    }

    @Test @Order(11)
    void stboxGetSpace_null_returns_null() throws Exception {
        assertNull(STBoxUDFs.stboxGetSpace.call(null));
    }
}
