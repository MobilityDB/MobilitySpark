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

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AggregateUDAFs — temporal aggregate functions exercised by
 * driving each Aggregator's zero/reduce/merge/finish lifecycle directly,
 * without a SparkSession.
 *
 * MEOS function authority: meos/include/meos.h (temporal aggregate transfns)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AggregateUDAFsTest extends MeosTestBase {

    private static String TRIP1;
    private static String TRIP2;
    private static String TINT1;
    private static String TINT2;
    private static String TFLOAT1;
    private static String TFLOAT2;
    private static String TBOOL_T;
    private static String TBOOL_F;
    private static String TTEXT1;
    private static String TTEXT2;

    @BeforeAll
    static void initMeos() {
        TRIP1 = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(1 0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TRIP2 = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 1)@2020-01-01 00:00:00+00, POINT(1 1)@2020-01-01 01:00:00+00]"),
            (byte) 0);
        TINT1   = temporal_as_hexwkb(tint_in("[1@2020-01-01, 2@2020-01-02]"),     (byte) 0);
        TINT2   = temporal_as_hexwkb(tint_in("[3@2020-01-01, 4@2020-01-02]"),     (byte) 0);
        TFLOAT1 = temporal_as_hexwkb(tfloat_in("[1.0@2020-01-01, 2.0@2020-01-02]"), (byte) 0);
        TFLOAT2 = temporal_as_hexwkb(tfloat_in("[3.0@2020-01-01, 4.0@2020-01-02]"), (byte) 0);
        TBOOL_T = temporal_as_hexwkb(tbool_in("[t@2020-01-01, t@2020-01-02]"),    (byte) 0);
        TBOOL_F = temporal_as_hexwkb(tbool_in("[f@2020-01-01, f@2020-01-02]"),    (byte) 0);
        TTEXT1  = temporal_as_hexwkb(ttext_in("[AAA@2020-01-01, BBB@2020-01-02]"), (byte) 0);
        TTEXT2  = temporal_as_hexwkb(ttext_in("[CCC@2020-01-01, DDD@2020-01-02]"), (byte) 0);
    }

    // ------------------------------------------------------------------
    // tCount
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tCount_two_overlapping_trips_returns_nonnull_tint() {
        AggregateUDAFs.TCountFn agg = new AggregateUDAFs.TCountFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TRIP1);
        buf = agg.reduce(buf, TRIP2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(2)
    void tCount_single_trip_returns_nonnull() {
        AggregateUDAFs.TCountFn agg = new AggregateUDAFs.TCountFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TRIP1);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(3)
    void tCount_empty_input_returns_null() {
        AggregateUDAFs.TCountFn agg = new AggregateUDAFs.TCountFn();
        assertNull(agg.finish(agg.zero()));
    }

    // ------------------------------------------------------------------
    // tAnd / tOr
    // ------------------------------------------------------------------

    @Test @Order(4)
    void tAnd_all_true_returns_nonnull_tbool() {
        AggregateUDAFs.TAndFn agg = new AggregateUDAFs.TAndFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TBOOL_T);
        buf = agg.reduce(buf, TBOOL_T);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(5)
    void tOr_all_false_returns_nonnull_tbool() {
        AggregateUDAFs.TOrFn agg = new AggregateUDAFs.TOrFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TBOOL_F);
        buf = agg.reduce(buf, TBOOL_F);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    // ------------------------------------------------------------------
    // tIntMin / tIntMax / tIntSum
    // ------------------------------------------------------------------

    @Test @Order(6)
    void tIntMin_returns_nonnull_tint() {
        AggregateUDAFs.TMinAggFn agg = new AggregateUDAFs.TMinAggFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TINT1);
        buf = agg.reduce(buf, TINT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(7)
    void tIntMax_returns_nonnull_tint() {
        AggregateUDAFs.TMaxAggFn agg = new AggregateUDAFs.TMaxAggFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TINT1);
        buf = agg.reduce(buf, TINT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(8)
    void tIntSum_returns_nonnull_tint() {
        AggregateUDAFs.TSumAggFn agg = new AggregateUDAFs.TSumAggFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TINT1);
        buf = agg.reduce(buf, TINT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    // ------------------------------------------------------------------
    // tFloatMin / tFloatMax / tFloatSum
    // ------------------------------------------------------------------

    @Test @Order(9)
    void tFloatMin_returns_nonnull_tfloat() {
        AggregateUDAFs.TMinAggFn agg = new AggregateUDAFs.TMinAggFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TFLOAT1);
        buf = agg.reduce(buf, TFLOAT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(10)
    void tFloatMax_returns_nonnull_tfloat() {
        AggregateUDAFs.TMaxAggFn agg = new AggregateUDAFs.TMaxAggFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TFLOAT1);
        buf = agg.reduce(buf, TFLOAT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(11)
    void tFloatSum_returns_nonnull_tfloat() {
        AggregateUDAFs.TSumAggFn agg = new AggregateUDAFs.TSumAggFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TFLOAT1);
        buf = agg.reduce(buf, TFLOAT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    // ------------------------------------------------------------------
    // tTextMin / tTextMax
    // ------------------------------------------------------------------

    @Test @Order(12)
    void tTextMin_returns_nonnull_ttext() {
        AggregateUDAFs.TMinAggFn agg = new AggregateUDAFs.TMinAggFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TTEXT1);
        buf = agg.reduce(buf, TTEXT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(13)
    void tTextMax_returns_nonnull_ttext() {
        AggregateUDAFs.TMaxAggFn agg = new AggregateUDAFs.TMaxAggFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TTEXT1);
        buf = agg.reduce(buf, TTEXT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    // ------------------------------------------------------------------
    // tCentroid
    // ------------------------------------------------------------------

    @Test @Order(14)
    void tCentroid_two_parallel_trips_returns_nonnull() {
        AggregateUDAFs.TCentroidFn agg = new AggregateUDAFs.TCentroidFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TRIP1);
        buf = agg.reduce(buf, TRIP2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    // ------------------------------------------------------------------
    // tExtent
    // ------------------------------------------------------------------

    @Test @Order(15)
    void tExtent_returns_nonnull_stbox() {
        AggregateUDAFs.TExtentFn agg = new AggregateUDAFs.TExtentFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TRIP1);
        buf = agg.reduce(buf, TRIP2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    // ------------------------------------------------------------------
    // merge
    // ------------------------------------------------------------------

    @Test @Order(16)
    void merge_combines_two_buffers() {
        AggregateUDAFs.TCountFn agg = new AggregateUDAFs.TCountFn();
        String b1 = agg.reduce(agg.zero(), TRIP1);
        String b2 = agg.reduce(agg.zero(), TRIP2);
        String merged = agg.merge(b1, b2);
        String result = agg.finish(merged);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    // ------------------------------------------------------------------
    // tAvg
    // ------------------------------------------------------------------

    @Test @Order(17)
    void tAvg_two_tfloats_returns_nonnull() {
        AggregateUDAFs.TAvgFn agg = new AggregateUDAFs.TAvgFn();
        String buf = agg.zero();
        buf = agg.reduce(buf, TFLOAT1);
        buf = agg.reduce(buf, TFLOAT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(18)
    void tAvg_tint_returns_nonnull() {
        AggregateUDAFs.TAvgFn agg = new AggregateUDAFs.TAvgFn();
        String buf = agg.reduce(agg.zero(), TINT1);
        buf = agg.reduce(buf, TINT2);
        String result = agg.finish(buf);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(19)
    void tAvg_empty_input_returns_null() {
        AggregateUDAFs.TAvgFn agg = new AggregateUDAFs.TAvgFn();
        assertNull(agg.finish(agg.zero()));
    }

    // ------------------------------------------------------------------
    // setUnion / spanUnion / merge
    // ------------------------------------------------------------------

    @Test @Order(20)
    void setUnion_two_intsets_returns_nonnull() {
        AggregateUDAFs.SetUnionFn agg = new AggregateUDAFs.SetUnionFn();
        String s1 = set_as_hexwkb(intset_in("{1, 2, 3}"), (byte) 0);
        String s2 = set_as_hexwkb(intset_in("{3, 4, 5}"), (byte) 0);
        String result = agg.finish(agg.reduce(agg.reduce(agg.zero(), s1), s2));
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(21)
    void spanUnion_two_intspans_returns_nonnull() {
        AggregateUDAFs.SpanUnionFn agg = new AggregateUDAFs.SpanUnionFn();
        String a = span_as_hexwkb(intspan_in("[1, 5]"), (byte) 0);
        String b = span_as_hexwkb(intspan_in("[10, 20]"), (byte) 0);
        String result = agg.finish(agg.reduce(agg.reduce(agg.zero(), a), b));
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(22)
    void merge_two_disjoint_tints_returns_nonnull() {
        AggregateUDAFs.MergeFn agg = new AggregateUDAFs.MergeFn();
        String m1 = temporal_as_hexwkb(tint_in("[1@2020-01-01, 2@2020-01-02]"), (byte) 0);
        String m2 = temporal_as_hexwkb(tint_in("[3@2020-01-03, 4@2020-01-04]"), (byte) 0);
        String result = agg.finish(agg.reduce(agg.reduce(agg.zero(), m1), m2));
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(23)
    void setUnion_empty_input_returns_null() {
        AggregateUDAFs.SetUnionFn agg = new AggregateUDAFs.SetUnionFn();
        assertNull(agg.finish(agg.zero()));
    }

    // ------------------------------------------------------------------
    // windowed aggregates (input encodes "temporalHex|intervalText")
    // ------------------------------------------------------------------

    @Test @Order(24)
    void wIntSum_returns_nonnull() {
        AggregateUDAFs.WIntSumFn agg = new AggregateUDAFs.WIntSumFn();
        String enc = TINT1 + "|1 day";
        String result = agg.finish(agg.reduce(agg.zero(), enc));
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(25)
    void wFloatMax_returns_nonnull() {
        AggregateUDAFs.WFloatMaxFn agg = new AggregateUDAFs.WFloatMaxFn();
        String enc = TFLOAT1 + "|1 day";
        String result = agg.finish(agg.reduce(agg.zero(), enc));
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(26)
    void wAvg_returns_nonnull() {
        AggregateUDAFs.WAvgFn agg = new AggregateUDAFs.WAvgFn();
        String enc = TFLOAT1 + "|1 day";
        String result = agg.finish(agg.reduce(agg.zero(), enc));
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test @Order(27)
    void windowed_empty_input_returns_null() {
        AggregateUDAFs.WIntSumFn agg = new AggregateUDAFs.WIntSumFn();
        assertNull(agg.finish(agg.zero()));
    }
}
