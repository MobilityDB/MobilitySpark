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
 * Unit tests for temporal comparison operator UDFs:
 * teqTemporalTemporal, tneTemporalTemporal, tltTemporalTemporal,
 * tleTemporalTemporal, tgtTemporalTemporal, tgeTemporalTemporal.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BoolOpsUDFsExtTest extends MeosTestBase {

    private static String TFLOAT_A;
    private static String TFLOAT_B;
    private static String TBOOL_HEX;

    @BeforeAll
    static void initMeos() {
        TFLOAT_A = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01, 3.0@2020-01-03]"), (byte) 0);
        TFLOAT_B = temporal_as_hexwkb(
            tfloat_in("[2.0@2020-01-01, 2.0@2020-01-03]"), (byte) 0);
        TBOOL_HEX = temporal_as_hexwkb(
            tbool_in("[true@2020-01-01 00:00:00+00, false@2020-01-02 00:00:00+00]"), (byte) 0);
    }

    @Test @Order(1)
    void teqTemporalTemporal_returns_tbool_hex() throws Exception {
        String r = BoolOpsUDFs.teqTemporalTemporal.call(TFLOAT_A, TFLOAT_A);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tneTemporalTemporal_returns_tbool_hex() throws Exception {
        String r = BoolOpsUDFs.tneTemporalTemporal.call(TFLOAT_A, TFLOAT_B);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void tltTemporalTemporal_returns_tbool_hex() throws Exception {
        String r = BoolOpsUDFs.tltTemporalTemporal.call(TFLOAT_A, TFLOAT_B);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void tleTemporalTemporal_returns_tbool_hex() throws Exception {
        String r = BoolOpsUDFs.tleTemporalTemporal.call(TFLOAT_A, TFLOAT_B);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(5)
    void tgtTemporalTemporal_returns_tbool_hex() throws Exception {
        String r = BoolOpsUDFs.tgtTemporalTemporal.call(TFLOAT_A, TFLOAT_B);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void tgeTemporalTemporal_returns_tbool_hex() throws Exception {
        String r = BoolOpsUDFs.tgeTemporalTemporal.call(TFLOAT_A, TFLOAT_B);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void null_input_returns_null() throws Exception {
        assertNull(BoolOpsUDFs.teqTemporalTemporal.call(null, TFLOAT_A));
        assertNull(BoolOpsUDFs.tneTemporalTemporal.call(TFLOAT_A, null));
        assertNull(BoolOpsUDFs.tltTemporalTemporal.call(null, TFLOAT_B));
        assertNull(BoolOpsUDFs.tgeTemporalTemporal.call(null, null));
    }

    // ------------------------------------------------------------------
    // tnotTbool
    // ------------------------------------------------------------------

    @Test @Order(8)
    void tnotTbool_returns_nonnull_hexwkb() throws Exception {
        String r = BoolOpsUDFs.tnotTbool.call(TBOOL_HEX);
        assertNotNull(r, "tnotTbool must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void tnotTbool_null_returns_null() throws Exception {
        assertNull(BoolOpsUDFs.tnotTbool.call(null));
    }
}
