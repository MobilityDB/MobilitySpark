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

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TTextUDFs — ttext case-conversion operations.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TTextUDFsTest {

    private static String TTEXT_HELLO;
    private static String TTEXT_WORLD;

    @BeforeAll
    static void initMeos() throws Exception {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TTEXT_HELLO = temporal_as_hexwkb(ttext_in("hello@2020-01-01"), (byte) 0);
        TTEXT_WORLD = temporal_as_hexwkb(ttext_in("World@2020-01-01"), (byte) 0);
    }

    @Test @Order(1)
    void ttextUpper_converts_to_uppercase() throws Exception {
        String upper = TTextUDFs.ttextUpper.call(TTEXT_HELLO);
        assertNotNull(upper);
        // Decode back and check the start value
        String sv = AccessorUDFs.ttextStartValue.call(upper);
        assertNotNull(sv);
        // text_out() is PostgreSQL textout() (pg_text_to_cstring): the raw,
        // unquoted text value. (Pre-pin libmeos quoted it — an input/output
        // asymmetry the centralised escape fix corrected.)
        assertEquals("HELLO", sv);
    }

    @Test @Order(2)
    void ttextLower_converts_to_lowercase() throws Exception {
        String lower = TTextUDFs.ttextLower.call(TTEXT_WORLD);
        assertNotNull(lower);
        String sv = AccessorUDFs.ttextStartValue.call(lower);
        assertNotNull(sv);
        assertEquals("world", sv);
    }

    @Test @Order(3)
    void ttextInitcap_capitalises_first_letter() throws Exception {
        String init = TTextUDFs.ttextInitcap.call(TTEXT_HELLO);
        assertNotNull(init);
        String sv = AccessorUDFs.ttextStartValue.call(init);
        assertNotNull(sv);
        assertEquals("Hello", sv);
    }

    @Test @Order(4)
    void ttextUpper_null_input_returns_null() throws Exception {
        assertNull(TTextUDFs.ttextUpper.call(null));
    }

    @Test @Order(5)
    void ttextLower_null_input_returns_null() throws Exception {
        assertNull(TTextUDFs.ttextLower.call(null));
    }

    @Test @Order(6)
    void ttextInitcap_null_input_returns_null() throws Exception {
        assertNull(TTextUDFs.ttextInitcap.call(null));
    }
}
