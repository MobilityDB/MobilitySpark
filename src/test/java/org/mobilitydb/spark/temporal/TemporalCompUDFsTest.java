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

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TemporalCompUDFsTest {

    private static String TINT;
    private static String TFLOAT;
    private static String TBOOL;
    private static String TTEXT;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TINT = temporal_as_hexwkb(
            tint_in("[1@2020-01-01 00:00:00+00, 5@2020-01-02 00:00:00+00]"), (byte) 0);
        TFLOAT = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01 00:00:00+00, 5.0@2020-01-02 00:00:00+00]"), (byte) 0);
        TBOOL = temporal_as_hexwkb(
            tbool_in("[true@2020-01-01 00:00:00+00, false@2020-01-02 00:00:00+00]"), (byte) 0);
        TTEXT = temporal_as_hexwkb(
            ttext_in("[\"a\"@2020-01-01 00:00:00+00, \"b\"@2020-01-02 00:00:00+00]"), (byte) 0);
    }

    // teq — one test per type combo

    @Test @Order(1) void teqTintInt() throws Exception {
        String r = TemporalCompUDFs.teqTintInt.call(TINT, 3);
        assertNotNull(r); assertFalse(r.isBlank());
    }

    @Test @Order(2) void teqTfloatFloat() throws Exception {
        assertNotNull(TemporalCompUDFs.teqTfloatFloat.call(TFLOAT, 3.0));
    }

    @Test @Order(3) void teqTboolBool() throws Exception {
        assertNotNull(TemporalCompUDFs.teqTboolBool.call(TBOOL, true));
    }

    @Test @Order(4) void teqTtextText() throws Exception {
        assertNotNull(TemporalCompUDFs.teqTtextText.call(TTEXT, "a"));
    }

    @Test @Order(5) void teqTemporal() throws Exception {
        assertNotNull(TemporalCompUDFs.teqTemporal.call(TINT, TINT));
    }

    // tne / tlt / tle / tgt / tge — one representative each

    @Test @Order(6) void tneTintInt() throws Exception {
        assertNotNull(TemporalCompUDFs.tneTintInt.call(TINT, 3));
    }

    @Test @Order(7) void tltTfloatFloat() throws Exception {
        assertNotNull(TemporalCompUDFs.tltTfloatFloat.call(TFLOAT, 3.0));
    }

    @Test @Order(8) void tleTtextText() throws Exception {
        assertNotNull(TemporalCompUDFs.tleTtextText.call(TTEXT, "b"));
    }

    @Test @Order(9) void tgtTintInt() throws Exception {
        assertNotNull(TemporalCompUDFs.tgtTintInt.call(TINT, 3));
    }

    @Test @Order(10) void tgeTemporal() throws Exception {
        assertNotNull(TemporalCompUDFs.tgeTemporal.call(TINT, TINT));
    }

    // null guards

    @Test @Order(11) void teqTintInt_null_returns_null() throws Exception {
        assertNull(TemporalCompUDFs.teqTintInt.call(null, 3));
        assertNull(TemporalCompUDFs.teqTintInt.call(TINT, null));
    }

    @Test @Order(12) void teqTemporal_null_returns_null() throws Exception {
        assertNull(TemporalCompUDFs.teqTemporal.call(null, TINT));
        assertNull(TemporalCompUDFs.teqTemporal.call(TINT, null));
    }
}
