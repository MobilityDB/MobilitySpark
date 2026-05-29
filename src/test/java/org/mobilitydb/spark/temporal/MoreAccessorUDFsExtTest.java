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

import java.sql.Timestamp;
import org.mobilitydb.spark.MeosTestBase;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MoreAccessorUDFs value_at_timestamptz UDFs:
 *   tboolValueAtTimestamptz, tintValueAtTimestamptz, tfloatValueAtTimestamptz,
 *   ttextValueAtTimestamptz.
 *
 * Timestamps are created as Spark java.sql.Timestamp (Unix-epoch ms).
 * The UDFs convert them to PG-epoch µs internally before calling MEOS.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MoreAccessorUDFsExtTest extends MeosTestBase {

    private static String TBOOL_SEQ;
    private static String TINT_SEQ;
    private static String TFLOAT_SEQ;
    private static String TTEXT_SEQ;

    // Timestamps inside the sequences (2020-01-01 00:00:00 UTC = Unix 1577836800 s)
    private static Timestamp TS_START;
    // Timestamp outside the sequences (2020-06-01 00:00:00 UTC = Unix 1590969600 s)
    private static Timestamp TS_OUTSIDE;

    @BeforeAll
    static void initMeos() {
        TBOOL_SEQ = temporal_as_hexwkb(
            tbool_in("Interp=Step;[true@2020-01-01 00:00:00+00, true@2020-01-03 00:00:00+00]"),
            (byte) 0);
        TINT_SEQ = temporal_as_hexwkb(
            tint_in("Interp=Step;[7@2020-01-01 00:00:00+00, 7@2020-01-03 00:00:00+00]"),
            (byte) 0);
        TFLOAT_SEQ = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01 00:00:00+00, 3.0@2020-01-03 00:00:00+00]"),
            (byte) 0);
        TTEXT_SEQ = temporal_as_hexwkb(
            ttext_in("Interp=Step;[hello@2020-01-01 00:00:00+00, hello@2020-01-03 00:00:00+00]"),
            (byte) 0);

        TS_START   = new Timestamp(1577836800L * 1000L);  // 2020-01-01 00:00:00 UTC
        TS_OUTSIDE = new Timestamp(1590969600L * 1000L);  // 2020-06-01 00:00:00 UTC
    }

    // ------------------------------------------------------------------
    // tboolValueAtTimestamptz
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tboolValueAtTimestamptz_at_start_returns_true() throws Exception {
        Boolean r = MoreAccessorUDFs.tboolValueAtTimestamptz.call(TBOOL_SEQ, TS_START);
        assertNotNull(r, "Value at start timestamp must be non-null");
        assertTrue(r, "tbool value at t0 must be true");
    }

    @Test @Order(2)
    void tboolValueAtTimestamptz_outside_range_returns_null() throws Exception {
        Boolean r = MoreAccessorUDFs.tboolValueAtTimestamptz.call(TBOOL_SEQ, TS_OUTSIDE);
        assertNull(r, "Value outside sequence range must be null");
    }

    @Test @Order(3)
    void tboolValueAtTimestamptz_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.tboolValueAtTimestamptz.call(null, TS_START));
        assertNull(MoreAccessorUDFs.tboolValueAtTimestamptz.call(TBOOL_SEQ, null));
    }

    // ------------------------------------------------------------------
    // tintValueAtTimestamptz
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tintValueAtTimestamptz_at_start_returns_correct_value() throws Exception {
        Integer r = MoreAccessorUDFs.tintValueAtTimestamptz.call(TINT_SEQ, TS_START);
        assertNotNull(r, "Value at start timestamp must be non-null");
        assertEquals(7, r.intValue(), "tint value at t0 must be 7");
    }

    @Test @Order(5)
    void tintValueAtTimestamptz_outside_range_returns_null() throws Exception {
        Integer r = MoreAccessorUDFs.tintValueAtTimestamptz.call(TINT_SEQ, TS_OUTSIDE);
        assertNull(r, "Value outside sequence range must be null");
    }

    @Test @Order(6)
    void tintValueAtTimestamptz_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.tintValueAtTimestamptz.call(null, TS_START));
        assertNull(MoreAccessorUDFs.tintValueAtTimestamptz.call(TINT_SEQ, null));
    }

    // ------------------------------------------------------------------
    // tfloatValueAtTimestamptz
    // ------------------------------------------------------------------

    @Test @Order(7)
    void tfloatValueAtTimestamptz_at_start_returns_1_0() throws Exception {
        Double r = MoreAccessorUDFs.tfloatValueAtTimestamptz.call(TFLOAT_SEQ, TS_START);
        assertNotNull(r, "Value at start timestamp must be non-null");
        assertEquals(1.0, r, 1e-9, "tfloat value at t0 must be 1.0");
    }

    @Test @Order(8)
    void tfloatValueAtTimestamptz_outside_range_returns_null() throws Exception {
        Double r = MoreAccessorUDFs.tfloatValueAtTimestamptz.call(TFLOAT_SEQ, TS_OUTSIDE);
        assertNull(r, "Value outside sequence range must be null");
    }

    @Test @Order(9)
    void tfloatValueAtTimestamptz_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.tfloatValueAtTimestamptz.call(null, TS_START));
        assertNull(MoreAccessorUDFs.tfloatValueAtTimestamptz.call(TFLOAT_SEQ, null));
    }

    // ------------------------------------------------------------------
    // ttextValueAtTimestamptz
    // ------------------------------------------------------------------

    @Test @Order(10)
    void ttextValueAtTimestamptz_at_start_returns_correct_value() throws Exception {
        String r = MoreAccessorUDFs.ttextValueAtTimestamptz.call(TTEXT_SEQ, TS_START);
        assertNotNull(r, "Value at start timestamp must be non-null");
        assertEquals("\"hello\"", r, "ttext value at t0 must be '\"hello\"'");
    }

    @Test @Order(11)
    void ttextValueAtTimestamptz_outside_range_returns_null() throws Exception {
        String r = MoreAccessorUDFs.ttextValueAtTimestamptz.call(TTEXT_SEQ, TS_OUTSIDE);
        assertNull(r, "Value outside sequence range must be null");
    }

    @Test @Order(12)
    void ttextValueAtTimestamptz_null_returns_null() throws Exception {
        assertNull(MoreAccessorUDFs.ttextValueAtTimestamptz.call(null, TS_START));
        assertNull(MoreAccessorUDFs.ttextValueAtTimestamptz.call(TTEXT_SEQ, null));
    }
}
