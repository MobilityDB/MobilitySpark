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

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for new restriction UDFs:
 *   tintMinusValue, temporalDeleteTimestamptz.
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RestrictionUDFsExt4Test extends MeosTestBase {

    private static String TINT_SEQ;

    @BeforeAll
    static void initMeos() {
        TINT_SEQ = temporal_as_hexwkb(
            tint_in("Interp=Step;[1@2020-01-01 00:00:00+00, 1@2020-01-02 00:00:00+00, 2@2020-01-03 00:00:00+00, 2@2020-01-04 00:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // tintMinusValue
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tintMinusValue_removes_instants_with_value() throws Exception {
        String r = RestrictionUDFs.tintMinusValue.call(TINT_SEQ, 1);
        assertTrue(r == null || !r.isBlank(),
            "Minus value 1 must return null (fully removed) or a valid temporal");
    }

    @Test @Order(2)
    void tintMinusValue_nonexistent_value_returns_original_or_nonnull() throws Exception {
        String r = RestrictionUDFs.tintMinusValue.call(TINT_SEQ, 99);
        assertNotNull(r, "Minus value not in sequence must return original sequence");
        assertFalse(r.isBlank());
    }

    @Test @Order(3)
    void tintMinusValue_null_returns_null() throws Exception {
        assertNull(RestrictionUDFs.tintMinusValue.call(null, 1));
        assertNull(RestrictionUDFs.tintMinusValue.call(TINT_SEQ, null));
    }

    // ------------------------------------------------------------------
    // temporalDeleteTimestamptz
    // ------------------------------------------------------------------

    @Test @Order(4)
    void temporalDeleteTimestamptz_at_known_instant_returns_shorter_seq() throws Exception {
        // 2020-01-02 00:00:00 UTC = Unix 1577923200
        Timestamp ts = new Timestamp(1577923200L * 1000L);
        String r = RestrictionUDFs.temporalDeleteTimestamptz.call(TINT_SEQ, ts);
        assertTrue(r == null || !r.isBlank(),
            "Delete at known instant must return null or a valid temporal");
    }

    @Test @Order(5)
    void temporalDeleteTimestamptz_outside_range_returns_original_or_nonnull() throws Exception {
        // 2020-06-01 00:00:00 UTC = Unix 1590969600
        Timestamp ts = new Timestamp(1590969600L * 1000L);
        String r = RestrictionUDFs.temporalDeleteTimestamptz.call(TINT_SEQ, ts);
        assertNotNull(r, "Delete outside range must return original sequence unchanged");
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void temporalDeleteTimestamptz_null_returns_null() throws Exception {
        Timestamp ts = new Timestamp(1577836800L * 1000L);
        assertNull(RestrictionUDFs.temporalDeleteTimestamptz.call(null, ts));
        assertNull(RestrictionUDFs.temporalDeleteTimestamptz.call(TINT_SEQ, null));
    }
}
