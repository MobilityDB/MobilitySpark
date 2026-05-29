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
 * Unit tests for TBoxUDFs rounding: tboxRound.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TBoxUDFsExtTest extends MeosTestBase {

    private static String TBOX_HEX;

    @BeforeAll
    static void initMeos() throws Exception {
        // Build a tbox via tnumber_to_tbox from a tfloat sequence with non-trivial values
        TBOX_HEX = AccessorUDFs.tnumberToTbox.call(
            temporal_as_hexwkb(tfloat_in("[1.123456@2020-01-01, 9.987654@2020-01-03]"), (byte) 0));
    }

    @Test @Order(1)
    void tboxRound_returns_nonnull_hexwkb() throws Exception {
        assertNotNull(TBOX_HEX, "TBOX must be buildable");
        String r = TBoxUDFs.tboxRound.call(TBOX_HEX, 2);
        assertNotNull(r, "tboxRound must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tboxRound_xmin_is_rounded() throws Exception {
        String rounded = TBoxUDFs.tboxRound.call(TBOX_HEX, 2);
        assertNotNull(rounded);
        Double xmin = TBoxUDFs.tboxXmin.call(rounded);
        assertNotNull(xmin);
        assertEquals(1.12, xmin, 1e-9, "xmin rounded to 2 decimals must be 1.12");
    }

    @Test @Order(3)
    void tboxRound_xmax_is_rounded() throws Exception {
        String rounded = TBoxUDFs.tboxRound.call(TBOX_HEX, 2);
        assertNotNull(rounded);
        Double xmax = TBoxUDFs.tboxXmax.call(rounded);
        assertNotNull(xmax);
        assertEquals(9.99, xmax, 1e-9, "xmax rounded to 2 decimals must be 9.99");
    }

    @Test @Order(4)
    void tboxRound_null_returns_null() throws Exception {
        assertNull(TBoxUDFs.tboxRound.call(null, 2));
        assertNull(TBoxUDFs.tboxRound.call(TBOX_HEX, null));
    }
}
