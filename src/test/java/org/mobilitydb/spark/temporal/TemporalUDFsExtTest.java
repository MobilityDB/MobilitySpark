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

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TemporalUDFs MFJSON output and text-output UDFs:
 *   temporalAsMfjson, tboolOut, tintOut, tfloatOut, ttextOut.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TemporalUDFsExtTest {

    private static String TRIP_HEX;
    private static String TBOOL_HEX;
    private static String TINT_HEX;
    private static String TFLOAT_HEX;
    private static String TTEXT_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TRIP_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(1 0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TBOOL_HEX = temporal_as_hexwkb(tbool_in("[true@2020-01-01, false@2020-01-03]"), (byte) 0);
        TINT_HEX  = temporal_as_hexwkb(tint_in("[1@2020-01-01, 3@2020-01-03]"),        (byte) 0);
        TFLOAT_HEX = temporal_as_hexwkb(tfloat_in("[1.5@2020-01-01, 2.5@2020-01-03]"), (byte) 0);
        TTEXT_HEX  = temporal_as_hexwkb(ttext_in("[hello@2020-01-01, world@2020-01-03]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // temporalAsMfjson
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalAsMfjson_returns_json_string() throws Exception {
        String r = TemporalUDFs.temporalAsMfjson.call(TRIP_HEX, 6);
        assertNotNull(r);
        assertFalse(r.isBlank());
        assertTrue(r.contains("\"type\""), "MFJSON output must be a JSON object");
    }

    @Test @Order(2)
    void temporalAsMfjson_null_precision_uses_default() throws Exception {
        String r = TemporalUDFs.temporalAsMfjson.call(TRIP_HEX, null);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void temporalAsMfjson_null_trip_returns_null() throws Exception {
        assertNull(TemporalUDFs.temporalAsMfjson.call(null, 6));
    }

    // ------------------------------------------------------------------
    // tboolOut
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tboolOut_returns_text_representation() throws Exception {
        String r = TemporalUDFs.tboolOut.call(TBOOL_HEX);
        assertNotNull(r);
        assertFalse(r.isBlank());
        assertTrue(r.contains("t") || r.contains("f"), "tbool text must contain t or f");
    }

    @Test @Order(5)
    void tboolOut_null_returns_null() throws Exception {
        assertNull(TemporalUDFs.tboolOut.call(null));
    }

    // ------------------------------------------------------------------
    // tintOut
    // ------------------------------------------------------------------

    @Test @Order(6)
    void tintOut_returns_text_representation() throws Exception {
        String r = TemporalUDFs.tintOut.call(TINT_HEX);
        assertNotNull(r);
        assertFalse(r.isBlank());
        assertTrue(r.contains("1"), "tintOut must contain the value 1");
    }

    @Test @Order(7)
    void tintOut_null_returns_null() throws Exception {
        assertNull(TemporalUDFs.tintOut.call(null));
    }

    // ------------------------------------------------------------------
    // tfloatOut
    // ------------------------------------------------------------------

    @Test @Order(8)
    void tfloatOut_returns_text_with_decimal_value() throws Exception {
        String r = TemporalUDFs.tfloatOut.call(TFLOAT_HEX, 2);
        assertNotNull(r);
        assertFalse(r.isBlank());
        assertTrue(r.contains("1.5") || r.contains("1.50"), "tfloatOut must contain 1.5");
    }

    @Test @Order(9)
    void tfloatOut_null_precision_uses_default() throws Exception {
        String r = TemporalUDFs.tfloatOut.call(TFLOAT_HEX, null);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(10)
    void tfloatOut_null_returns_null() throws Exception {
        assertNull(TemporalUDFs.tfloatOut.call(null, 6));
    }

    // ------------------------------------------------------------------
    // ttextOut
    // ------------------------------------------------------------------

    @Test @Order(11)
    void ttextOut_returns_text_with_value() throws Exception {
        String r = TemporalUDFs.ttextOut.call(TTEXT_HEX);
        assertNotNull(r);
        assertFalse(r.isBlank());
        assertTrue(r.contains("hello"), "ttextOut must contain the literal value 'hello'");
    }

    @Test @Order(12)
    void ttextOut_null_returns_null() throws Exception {
        assertNull(TemporalUDFs.ttextOut.call(null));
    }
}
