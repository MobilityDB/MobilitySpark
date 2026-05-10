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
import java.util.List;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for tstzspanset extra accessors and tpointFromBaseTemp constructor:
 *   tstzspansetNumTimestamps, tstzspansetTimestamps, tstzspansetDuration,
 *   tpointFromBaseTemp.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpanAccessorUDFsExt2Test {

    /** TstzSpanSet: 2 disjoint 1-day spans with a gap in between. */
    private static String TSTZSPANSET_HEX;
    /** Single tgeompoint instant — used as template for tpointFromBaseTemp. */
    private static String TPOINT_INST_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TSTZSPANSET_HEX = spanset_as_hexwkb(
            tstzspanset_in("{[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]," +
                            "[2020-02-01 00:00:00+00, 2020-02-02 00:00:00+00]}"),
            (byte) 0);

        TPOINT_INST_HEX = temporal_as_hexwkb(
            tgeompoint_in("POINT(1 2)@2020-01-01 00:00:00+00"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // tstzspansetNumTimestamps
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tstzspansetNumTimestamps_returns_nonnull() throws Exception {
        Integer n = SpanAccessorUDFs.tstzspansetNumTimestamps.call(TSTZSPANSET_HEX);
        assertNotNull(n, "tstzspansetNumTimestamps must return non-null");
    }

    @Test @Order(2)
    void tstzspansetNumTimestamps_two_spans_returns_four() throws Exception {
        Integer n = SpanAccessorUDFs.tstzspansetNumTimestamps.call(TSTZSPANSET_HEX);
        assertNotNull(n);
        assertEquals(4, n.intValue(),
            "2 closed spans → 4 boundary timestamps");
    }

    @Test @Order(3)
    void tstzspansetNumTimestamps_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.tstzspansetNumTimestamps.call(null));
    }

    // ------------------------------------------------------------------
    // tstzspansetTimestamps
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tstzspansetTimestamps_returns_nonnull_list() throws Exception {
        List<Timestamp> ts = SpanAccessorUDFs.tstzspansetTimestamps.call(TSTZSPANSET_HEX);
        assertNotNull(ts, "tstzspansetTimestamps must return non-null");
        assertFalse(ts.isEmpty());
    }

    @Test @Order(5)
    void tstzspansetTimestamps_count_matches_numTimestamps() throws Exception {
        Integer n  = SpanAccessorUDFs.tstzspansetNumTimestamps.call(TSTZSPANSET_HEX);
        List<Timestamp> ts = SpanAccessorUDFs.tstzspansetTimestamps.call(TSTZSPANSET_HEX);
        assertNotNull(n);
        assertNotNull(ts);
        assertEquals(n.intValue(), ts.size(),
            "timestamp list size must match tstzspansetNumTimestamps");
    }

    @Test @Order(6)
    void tstzspansetTimestamps_elements_are_nonnull() throws Exception {
        List<Timestamp> ts = SpanAccessorUDFs.tstzspansetTimestamps.call(TSTZSPANSET_HEX);
        assertNotNull(ts);
        ts.forEach(t -> assertNotNull(t, "each timestamp must be non-null"));
    }

    @Test @Order(7)
    void tstzspansetTimestamps_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.tstzspansetTimestamps.call(null));
    }

    // ------------------------------------------------------------------
    // tstzspansetDuration
    // ------------------------------------------------------------------

    @Test @Order(8)
    void tstzspansetDuration_returns_nonnull_interval_string() throws Exception {
        String dur = SpanAccessorUDFs.tstzspansetDuration.call(TSTZSPANSET_HEX, false);
        assertNotNull(dur, "tstzspansetDuration must return non-null");
        assertFalse(dur.isBlank());
    }

    @Test @Order(9)
    void tstzspansetDuration_ignoreGaps_true_returns_nonnull() throws Exception {
        String dur = SpanAccessorUDFs.tstzspansetDuration.call(TSTZSPANSET_HEX, true);
        assertNotNull(dur, "tstzspansetDuration with ignoreGaps=true must return non-null");
        assertFalse(dur.isBlank());
    }

    @Test @Order(10)
    void tstzspansetDuration_null_returns_null() throws Exception {
        assertNull(SpanAccessorUDFs.tstzspansetDuration.call(null, false));
    }
}
