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
 * Unit tests for BoolOpsUDFs — temporal AND/OR on tbool.
 *
 * MEOS function authority: meos/include/meos.h (028_tbool_boolops)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BoolOpsUDFsTest extends MeosTestBase {

    private static String TBOOL_TRUE;
    private static String TBOOL_FALSE;

    @BeforeAll
    static void initMeos() {
        TBOOL_TRUE  = temporal_as_hexwkb(tbool_in("t@2020-01-01"), (byte) 0);
        TBOOL_FALSE = temporal_as_hexwkb(tbool_in("f@2020-01-01"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // tand
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tandBool_true_and_true_returns_nonnull() throws Exception {
        String r = BoolOpsUDFs.tandBool.call(TBOOL_TRUE, true);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tandBool_true_and_false_returns_nonnull() throws Exception {
        String r = BoolOpsUDFs.tandBool.call(TBOOL_TRUE, false);
        assertNotNull(r);
    }

    @Test @Order(3)
    void tandBoolTbool_false_and_true_returns_nonnull() throws Exception {
        String r = BoolOpsUDFs.tandBoolTbool.call(false, TBOOL_TRUE);
        assertNotNull(r);
    }

    @Test @Order(4)
    void tandTboolTbool_true_and_false_returns_nonnull() throws Exception {
        String r = BoolOpsUDFs.tandTboolTbool.call(TBOOL_TRUE, TBOOL_FALSE);
        assertNotNull(r);
    }

    @Test @Order(5)
    void tandBool_null_input_returns_null() throws Exception {
        assertNull(BoolOpsUDFs.tandBool.call(null, true));
        assertNull(BoolOpsUDFs.tandBool.call(TBOOL_TRUE, null));
    }

    // ------------------------------------------------------------------
    // tor
    // ------------------------------------------------------------------

    @Test @Order(6)
    void torBool_false_or_true_returns_nonnull() throws Exception {
        String r = BoolOpsUDFs.torBool.call(TBOOL_FALSE, true);
        assertNotNull(r);
    }

    @Test @Order(7)
    void torBool_false_or_false_returns_nonnull() throws Exception {
        String r = BoolOpsUDFs.torBool.call(TBOOL_FALSE, false);
        assertNotNull(r);
    }

    @Test @Order(8)
    void torBoolTbool_true_or_false_returns_nonnull() throws Exception {
        String r = BoolOpsUDFs.torBoolTbool.call(true, TBOOL_FALSE);
        assertNotNull(r);
    }

    @Test @Order(9)
    void torTboolTbool_false_or_true_returns_nonnull() throws Exception {
        String r = BoolOpsUDFs.torTboolTbool.call(TBOOL_FALSE, TBOOL_TRUE);
        assertNotNull(r);
    }

    @Test @Order(10)
    void torBool_null_input_returns_null() throws Exception {
        assertNull(BoolOpsUDFs.torBool.call(null, true));
        assertNull(BoolOpsUDFs.torBool.call(TBOOL_FALSE, null));
    }
}
