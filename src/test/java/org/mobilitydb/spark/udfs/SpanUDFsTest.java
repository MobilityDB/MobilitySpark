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

package org.mobilitydb.spark.udfs;

import org.junit.jupiter.api.*;
import org.mobilitydb.spark.temporal.SpanUDFs;

import java.util.HexFormat;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for span/spanset TemporalParquet UDFs — runs without a Spark session.
 *
 * Each test constructs a span/spanset via the MEOS _in function, serialises it
 * to hex-WKB with span_as_hexwkb / spanset_as_hexwkb, hex-decodes to bytes,
 * then verifies that the corresponding fromBinary UDF recovers the original
 * hex-WKB string (lossless round-trip).
 *
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpanUDFsTest {

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");
    }

    // ------------------------------------------------------------------ spans

    @Test @Order(1)
    void tstzspanFromBinary_round_trips() throws Exception {
        String hex = span_as_hexwkb(
            tstzspan_in("[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.tstzspanFromBinary.call(bytes),
            "tstzspanFromBinary must round-trip through MEOS span WKB");
    }

    @Test @Order(2)
    void intspanFromBinary_round_trips() throws Exception {
        String hex = span_as_hexwkb(intspan_in("[1, 10]"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.intspanFromBinary.call(bytes),
            "intspanFromBinary must round-trip through MEOS span WKB");
    }

    @Test @Order(3)
    void floatspanFromBinary_round_trips() throws Exception {
        String hex = span_as_hexwkb(floatspan_in("[1.5, 2.5]"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.floatspanFromBinary.call(bytes),
            "floatspanFromBinary must round-trip through MEOS span WKB");
    }

    @Test @Order(4)
    void bigintspanFromBinary_round_trips() throws Exception {
        String hex = span_as_hexwkb(bigintspan_in("[1000000000, 2000000000]"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.bigintspanFromBinary.call(bytes),
            "bigintspanFromBinary must round-trip through MEOS span WKB");
    }

    @Test @Order(5)
    void datespanFromBinary_round_trips() throws Exception {
        String hex = span_as_hexwkb(datespan_in("[2020-01-01, 2020-01-10]"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.datespanFromBinary.call(bytes),
            "datespanFromBinary must round-trip through MEOS span WKB");
    }

    // -------------------------------------------------------------- spansets

    @Test @Order(6)
    void tstzspansetFromBinary_round_trips() throws Exception {
        String hex = spanset_as_hexwkb(
            tstzspanset_in("{[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00]," +
                           "[2020-02-01 00:00:00+00, 2020-02-02 00:00:00+00]}"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.tstzspansetFromBinary.call(bytes),
            "tstzspansetFromBinary must round-trip through MEOS spanset WKB");
    }

    @Test @Order(7)
    void intspansetFromBinary_round_trips() throws Exception {
        String hex = spanset_as_hexwkb(intspanset_in("{[1, 5], [10, 20]}"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.intspansetFromBinary.call(bytes),
            "intspansetFromBinary must round-trip through MEOS spanset WKB");
    }

    @Test @Order(8)
    void floatspansetFromBinary_round_trips() throws Exception {
        String hex = spanset_as_hexwkb(floatspanset_in("{[1.5, 2.5], [3.5, 4.5]}"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.floatspansetFromBinary.call(bytes),
            "floatspansetFromBinary must round-trip through MEOS spanset WKB");
    }

    @Test @Order(9)
    void bigintspansetFromBinary_round_trips() throws Exception {
        String hex = spanset_as_hexwkb(
            bigintspanset_in("{[1000000000, 2000000000], [3000000000, 4000000000]}"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.bigintspansetFromBinary.call(bytes),
            "bigintspansetFromBinary must round-trip through MEOS spanset WKB");
    }

    @Test @Order(10)
    void datespansetFromBinary_round_trips() throws Exception {
        String hex = spanset_as_hexwkb(
            datespanset_in("{[2020-01-01, 2020-01-10], [2020-02-01, 2020-02-10]}"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(hex.toLowerCase());
        assertEquals(hex, SpanUDFs.datespansetFromBinary.call(bytes),
            "datespansetFromBinary must round-trip through MEOS spanset WKB");
    }

    // --------------------------------------------------------------- nulls

    @Test @Order(11)
    void fromBinary_null_returns_null() throws Exception {
        assertNull(SpanUDFs.tstzspanFromBinary.call(null));
        assertNull(SpanUDFs.intspanFromBinary.call(null));
        assertNull(SpanUDFs.floatspanFromBinary.call(null));
        assertNull(SpanUDFs.bigintspanFromBinary.call(null));
        assertNull(SpanUDFs.datespanFromBinary.call(null));
        assertNull(SpanUDFs.tstzspansetFromBinary.call(null));
        assertNull(SpanUDFs.intspansetFromBinary.call(null));
        assertNull(SpanUDFs.floatspansetFromBinary.call(null));
        assertNull(SpanUDFs.bigintspansetFromBinary.call(null));
        assertNull(SpanUDFs.datespansetFromBinary.call(null));
    }
}
