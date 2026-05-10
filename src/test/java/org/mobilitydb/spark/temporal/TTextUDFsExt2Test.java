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
 * Unit tests for TTextUDFs ttext comparison operators.
 *
 * Each comparison UDF returns a tbool hex-WKB; the start value (decoded via
 * AccessorUDFs.tboolStartValue) is used to verify correctness.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TTextUDFsExt2Test {

    /** ttext "hello" as a single instant: used as the right-hand operand. */
    private static String TTEXT_HELLO_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TTEXT_HELLO_HEX = temporal_as_hexwkb(
            ttext_in("hello@2020-01-01 00:00:00+00"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // text op ttext
    // ------------------------------------------------------------------

    @Test @Order(1)
    void teqTextTtext_equal_text_returns_true_tbool() throws Exception {
        String r = TTextUDFs.teqTextTtext.call("hello", TTEXT_HELLO_HEX);
        assertNotNull(r, "teqTextTtext must return non-null");
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "\"hello\" = ttext(hello) must be true");
    }

    @Test @Order(2)
    void teqTextTtext_different_text_returns_false_tbool() throws Exception {
        String r = TTextUDFs.teqTextTtext.call("world", TTEXT_HELLO_HEX);
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertFalse(sv, "\"world\" = ttext(hello) must be false");
    }

    @Test @Order(3)
    void tneTextTtext_different_text_returns_true_tbool() throws Exception {
        String r = TTextUDFs.tneTextTtext.call("world", TTEXT_HELLO_HEX);
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "\"world\" <> ttext(hello) must be true");
    }

    @Test @Order(4)
    void tltTextTtext_lesser_text_returns_true_tbool() throws Exception {
        // "abc" < "hello" lexicographically
        String r = TTextUDFs.tltTextTtext.call("abc", TTEXT_HELLO_HEX);
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "\"abc\" < ttext(hello) must be true");
    }

    @Test @Order(5)
    void tleTextTtext_equal_text_returns_true_tbool() throws Exception {
        String r = TTextUDFs.tleTextTtext.call("hello", TTEXT_HELLO_HEX);
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "\"hello\" <= ttext(hello) must be true");
    }

    @Test @Order(6)
    void tgtTextTtext_greater_text_returns_true_tbool() throws Exception {
        // "xyz" > "hello" lexicographically
        String r = TTextUDFs.tgtTextTtext.call("xyz", TTEXT_HELLO_HEX);
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "\"xyz\" > ttext(hello) must be true");
    }

    @Test @Order(7)
    void tgeTextTtext_equal_text_returns_true_tbool() throws Exception {
        String r = TTextUDFs.tgeTextTtext.call("hello", TTEXT_HELLO_HEX);
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "\"hello\" >= ttext(hello) must be true");
    }

    @Test @Order(8)
    void teqTextTtext_null_returns_null() throws Exception {
        assertNull(TTextUDFs.teqTextTtext.call(null, TTEXT_HELLO_HEX));
        assertNull(TTextUDFs.teqTextTtext.call("hello", null));
    }

    // ------------------------------------------------------------------
    // ttext op text
    // ------------------------------------------------------------------

    @Test @Order(9)
    void teqTtextText_equal_text_returns_true_tbool() throws Exception {
        String r = TTextUDFs.teqTtextText.call(TTEXT_HELLO_HEX, "hello");
        assertNotNull(r, "teqTtextText must return non-null");
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "ttext(hello) = \"hello\" must be true");
    }

    @Test @Order(10)
    void tneTtextText_different_text_returns_true_tbool() throws Exception {
        String r = TTextUDFs.tneTtextText.call(TTEXT_HELLO_HEX, "world");
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "ttext(hello) <> \"world\" must be true");
    }

    @Test @Order(11)
    void tltTtextText_ttext_less_than_text_returns_true_tbool() throws Exception {
        // "hello" < "xyz"
        String r = TTextUDFs.tltTtextText.call(TTEXT_HELLO_HEX, "xyz");
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "ttext(hello) < \"xyz\" must be true");
    }

    @Test @Order(12)
    void tleTtextText_equal_returns_true_tbool() throws Exception {
        String r = TTextUDFs.tleTtextText.call(TTEXT_HELLO_HEX, "hello");
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "ttext(hello) <= \"hello\" must be true");
    }

    @Test @Order(13)
    void tgtTtextText_ttext_greater_than_text_returns_true_tbool() throws Exception {
        // "hello" > "abc"
        String r = TTextUDFs.tgtTtextText.call(TTEXT_HELLO_HEX, "abc");
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "ttext(hello) > \"abc\" must be true");
    }

    @Test @Order(14)
    void tgeTtextText_equal_returns_true_tbool() throws Exception {
        String r = TTextUDFs.tgeTtextText.call(TTEXT_HELLO_HEX, "hello");
        assertNotNull(r);
        Boolean sv = AccessorUDFs.tboolStartValue.call(r);
        assertTrue(sv, "ttext(hello) >= \"hello\" must be true");
    }

    @Test @Order(15)
    void teqTtextText_null_returns_null() throws Exception {
        assertNull(TTextUDFs.teqTtextText.call(null, "hello"));
        assertNull(TTextUDFs.teqTtextText.call(TTEXT_HELLO_HEX, null));
    }
}
