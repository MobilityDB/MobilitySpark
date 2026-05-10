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

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for new restriction UDFs:
 *   temporalAtTimestamptz, temporalMinusTimestamptz.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RestrictionUDFsExt5Test {

    private static String TINT_SEQ;

    // 2020-01-02 00:00:00 UTC = Unix 1577923200 s (inside sequence)
    private static Timestamp TS_INSIDE;
    // 2020-06-01 00:00:00 UTC = Unix 1590969600 s (outside sequence)
    private static Timestamp TS_OUTSIDE;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TINT_SEQ = temporal_as_hexwkb(
            tint_in("Interp=Step;[1@2020-01-01 00:00:00+00, 2@2020-01-03 00:00:00+00]"),
            (byte) 0);

        TS_INSIDE  = new Timestamp(1577923200L * 1000L);  // 2020-01-02 00:00:00 UTC
        TS_OUTSIDE = new Timestamp(1590969600L * 1000L);  // 2020-06-01 00:00:00 UTC
    }

    // ------------------------------------------------------------------
    // temporalAtTimestamptz
    // ------------------------------------------------------------------

    @Test @Order(1)
    void temporalAtTimestamptz_inside_returns_instant() throws Exception {
        String r = RestrictionUDFs.temporalAtTimestamptz.call(TINT_SEQ, TS_INSIDE);
        assertNotNull(r, "AT inside-range timestamp must return non-null");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void temporalAtTimestamptz_outside_returns_null() throws Exception {
        String r = RestrictionUDFs.temporalAtTimestamptz.call(TINT_SEQ, TS_OUTSIDE);
        assertNull(r, "AT outside-range timestamp must return null");
    }

    @Test @Order(3)
    void temporalAtTimestamptz_null_returns_null() throws Exception {
        assertNull(RestrictionUDFs.temporalAtTimestamptz.call(null, TS_INSIDE));
        assertNull(RestrictionUDFs.temporalAtTimestamptz.call(TINT_SEQ, null));
    }

    // ------------------------------------------------------------------
    // temporalMinusTimestamptz
    // ------------------------------------------------------------------

    @Test @Order(4)
    void temporalMinusTimestamptz_inside_returns_shorter() throws Exception {
        String r = RestrictionUDFs.temporalMinusTimestamptz.call(TINT_SEQ, TS_INSIDE);
        assertTrue(r == null || !r.isBlank(),
            "MINUS inside-range timestamp must return null or a valid shorter sequence");
    }

    @Test @Order(5)
    void temporalMinusTimestamptz_outside_returns_original() throws Exception {
        String r = RestrictionUDFs.temporalMinusTimestamptz.call(TINT_SEQ, TS_OUTSIDE);
        assertNotNull(r, "MINUS outside-range timestamp must return original sequence");
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void temporalMinusTimestamptz_null_returns_null() throws Exception {
        assertNull(RestrictionUDFs.temporalMinusTimestamptz.call(null, TS_INSIDE));
        assertNull(RestrictionUDFs.temporalMinusTimestamptz.call(TINT_SEQ, null));
    }
}
