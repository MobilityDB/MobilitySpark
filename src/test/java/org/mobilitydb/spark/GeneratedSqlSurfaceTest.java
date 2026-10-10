/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby retained provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS
 * TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.spark;

import functions.GeneratedFunctions;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mobilitydb.spark.sql.MobilitySparkSql;
import org.mobilitydb.spark.sql.types.TFloat;
import org.mobilitydb.spark.sql.types.TGeomPoint;
import org.mobilitydb.spark.sql.types.TInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The typed Spark SQL surface the spark-sql engine of JMEOS's codegen_jvm.py generates from the
 * overloads the Flink SQL surface uses: the queries MobilityFlink's GeneratedSqlSurfaceTest
 * asserts, answered alike, and the cases of one Spark name over overloads whose result types
 * differ, which the type of each MEOS value lets Spark resolve while it plans the call.
 */
class GeneratedSqlSurfaceTest {

    private static SparkSession spark;
    private static String tfloat;
    private static String tint;

    @BeforeAll
    static void init() {
        tfloat = "tfloatFromHexWKB('" + TFloat.encode(GeneratedFunctions.tfloat_in(
                "[1@2020-01-01 00:00:00+00, 3@2020-01-03 00:00:00+00]")) + "')";
        tint = "tintFromHexWKB('" + TInt.encode(GeneratedFunctions.tint_in(
                "[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00, 1@2020-01-03 00:00:00+00]"))
                + "')";
        spark = SparkSession.builder().appName("sql-surface").master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.datetime.java8API.enabled", "true")
                .getOrCreate();
        MobilitySparkSql.registerAll(spark);
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static Object scalar(String sql) {
        return spark.sql(sql).collectAsList().get(0).get(0);
    }

    private static String type(String sql) {
        return spark.sql(sql).schema().fields()[0].dataType().simpleString();
    }

    private static String tgeompoint(String text) {
        return "tgeompointFromHexEWKB('" + TGeomPoint.encode(GeneratedFunctions.tgeompoint_in(text)) + "')";
    }

    @Test
    void accessorsReadTheValue() {
        assertEquals("tfloat", type("SELECT " + tfloat));
        assertEquals(2, scalar("SELECT numInstants(" + tfloat + ")"));
        assertEquals(1.0, scalar("SELECT startValue(" + tfloat + ")"));
    }

    @Test
    void overloadsResolveByArgumentType() {
        assertEquals(3.0, scalar("SELECT startValue(tAdd(" + tfloat + ", 2.0))"));
        assertEquals(2, scalar("SELECT numInstants(tAdd(" + tfloat + ", " + tfloat + "))"));
        // one name over overloads whose result types differ: an int over a tint, a double over
        // a tfloat, as PostgreSQL answers them
        assertEquals(2, scalar("SELECT maxValue(" + tint + ")"));
        assertEquals("int", type("SELECT maxValue(" + tint + ")"));
        assertEquals(3.0, scalar("SELECT maxValue(" + tfloat + ")"));
        assertEquals("double", type("SELECT maxValue(" + tfloat + ")"));
        assertEquals(2, scalar("SELECT max(maxValue(v)) FROM (SELECT " + tint + " AS v)"));
        assertThrows(Exception.class, () -> scalar("SELECT maxValue(CAST('x' AS BINARY))"));
    }

    @Test
    void sparkKeepsItsOwnFunctionsUnderASharedName() {
        assertEquals(1.0, scalar("SELECT lower(floatspan_in('[1, 3]'))"));
        assertEquals("abc", scalar("SELECT lower('ABC')"));
        assertEquals(new java.math.BigDecimal("3"), scalar("SELECT round(2.5)"));
    }

    @Test
    void timeValuesCrossAsSparkTypes() {
        assertEquals(Duration.ofHours(48), scalar("SELECT duration(" + tfloat + ")"));
        assertEquals(Instant.parse("2020-01-01T00:00:00Z"),
                scalar("SELECT startTimestamp(" + tfloat + ")"));
        assertEquals(Instant.parse("2020-01-02T00:00:00Z"),
                scalar("SELECT startTimestamp(shiftTime(" + tfloat + ", INTERVAL '1' DAY))"));
        assertEquals(1, scalar("SELECT numInstants(atTime(" + tfloat
                + ", TIMESTAMP '2020-01-01 00:00:00'))"));
    }

    @Test
    void constructorsTakeSparkScalars() {
        String inst = "tfloat(CAST(1.5 AS DOUBLE), TIMESTAMP '2020-01-01 00:00:00')";
        assertEquals(1, scalar("SELECT numInstants(" + inst + ")"));
        assertEquals(1.5, scalar("SELECT startValue(" + inst + ")"));
        assertEquals(Instant.parse("2020-01-01T00:00:00Z"), scalar("SELECT startTimestamp(" + inst + ")"));
    }

    @Test
    void textConstructorsAndOutput() {
        String box = (String) scalar(
                "SELECT asText(tbox_in('TBOXFLOAT XT([1, 2],[2020-01-01, 2020-01-02])'))");
        assertTrue(box.startsWith("TBOXFLOAT XT([1, 2]"), box);
        assertEquals(true, scalar("SELECT spanOverlaps(floatspan_in('[1, 3]'), floatspan_in('[2, 4]'))"));
    }

    @Test
    void anArgumentLeftToItsNullDefaultIsWhatTheWrapperPasses() {
        // tintSeqSetGaps without maxdist splits on time alone (the wrapper's -1.0), and
        // without maxt either it splits on nothing (the wrapper's NULL)
        String instants = "array(tintFromText('1@2020-01-01'), tintFromText('2@2020-01-02'), "
                + "tintFromText('3@2020-01-05'))";
        assertEquals(2, scalar("SELECT numSequences(tintSeqSetGaps(" + instants
                + ", INTERVAL '1' DAY))"));
        assertEquals(1, scalar("SELECT numSequences(tintSeqSetGaps(" + instants + "))"));
        // tfloatSeq without an interpolation keeps the value's own (the wrapper's
        // INTERP_NONE) and makes an instant a sequence
        assertEquals("Sequence", scalar("SELECT tempSubtype(tfloatSeq("
                + "tfloatFromText('1@2020-01-01')))"));
    }

    @Test
    void anOmittedInterpolationIsTheOneOfTheType() {
        // appendInstant and the sequence set constructors without an interpolation take the
        // one the temporal type supports: linear for a tfloat, step for a tint. A linear
        // sequence drops an instant on the line of its neighbours, a step one keeps it.
        String tfloat = "tfloatFromText('[1@2020-01-01, 2@2020-01-02]')";
        String tint = "tintFromText('[1@2020-01-01, 2@2020-01-02]')";
        assertEquals(3, scalar("SELECT numInstants(appendInstant(" + tfloat
                + ", tfloatFromText('5@2020-01-03')))"));
        assertEquals(2, scalar("SELECT numInstants(appendInstant(" + tfloat
                + ", tfloatFromText('3@2020-01-03')))"));
        assertEquals(3, scalar("SELECT numInstants(appendInstant(" + tint
                + ", tintFromText('3@2020-01-03')))"));
        assertEquals("Linear", scalar("SELECT interp(appendInstant(" + tfloat
                + ", tfloatFromText('5@2020-01-03')))"));
        assertEquals("Step", scalar("SELECT interp(appendInstant(" + tint
                + ", tintFromText('3@2020-01-03')))"));
        // a gap of more than a day between instants starts a new sequence
        assertEquals(2, scalar("SELECT numSequences(tintSeqSetGaps(array("
                + "tintFromText('1@2020-01-01'), tintFromText('2@2020-01-02'), "
                + "tintFromText('3@2020-01-05')), INTERVAL '1' DAY, CAST(100.0 AS DOUBLE)))"));
    }

    @Test
    void arraysCrossAsSparkArrays() {
        assertEquals("{1, 2, 3}", scalar("SELECT intset_out(set(array(3, 1, 2)))"));
        assertEquals(Instant.parse("2020-01-01T00:00:00Z"), scalar("SELECT startValue(set(array("
                + "TIMESTAMP '2020-01-02 00:00:00', TIMESTAMP '2020-01-01 00:00:00')))"));
        String origin = tgeompoint("[Point(0 0)@2020-01-01, Point(0 0)@2020-01-02]");
        String far = tgeompoint("[Point(3 4)@2020-01-01, Point(3 4)@2020-01-02]");
        String near = tgeompoint("[Point(0 1)@2020-01-01, Point(0 1)@2020-01-02]");
        assertEquals(5.0, scalar("SELECT minDistance(array(" + origin + "), array(" + far + "))"));
        assertEquals(1.0, scalar("SELECT minDistance(array(" + origin + "), array(" + far + ", "
                + near + "))"));
        assertThrows(Exception.class,
                () -> scalar("SELECT intset_out(set(array(1, CAST(NULL AS INT))))"));
        // An array of fixed-size values reaches MEOS as the contiguous C array it reads
        assertEquals("{[1, 3), [5, 7)}", scalar("SELECT intspanset_out(spanset(array("
                + "intspan_in('[5, 7)'), intspan_in('[1, 3)'))))"));
        assertEquals(2, scalar("SELECT numValues(set(array(npoint_in('NPoint(1, 0.5)'), "
                + "npoint_in('NPoint(2, 0.3)'), npoint_in('NPoint(1, 0.5)'))))"));
        assertEquals(2, scalar("SELECT numValues(set(array(cbuffer_in('Cbuffer(Point(1 1), 0.5)'), "
                + "cbuffer_in('Cbuffer(Point(2 2), 1)'))))"));
    }

    @Test
    void setReturningRowsUnfoldThroughExplode() {
        // A set-returning function answers its rows as an array, which explode and inline unfold.
        List<Row> elems = spark.sql("SELECT explode(unnest(set(array(3, 1, 2))))").collectAsList();
        assertEquals(List.of(1, 2, 3), elems.stream().map(r -> r.get(0)).toList());
        // unnest of a tint: one row per distinct value, with the time it holds that value
        List<Row> values = spark.sql("SELECT inline(unnest(" + tint + "))").collectAsList();
        assertEquals(List.of(1, 2), values.stream().map(r -> (Integer) r.get(0)).sorted().toList());
        // timeSplit by a day from the first instant: the fragments of the two days and the
        // instant on the last border
        assertEquals(3, spark.sql("SELECT inline(timeSplit(" + tint + ", INTERVAL '1' DAY, "
                + "TIMESTAMP '2020-01-01 00:00:00'))").collectAsList().size());
        // eDwithinPairs: the index pairs, 1-based, of the trips ever within the distance
        String origin = tgeompoint("[Point(0 0)@2020-01-01, Point(0 0)@2020-01-02]");
        String far = tgeompoint("[Point(3 4)@2020-01-01, Point(3 4)@2020-01-02]");
        String near = tgeompoint("[Point(0 1)@2020-01-01, Point(0 1)@2020-01-02]");
        List<Row> pairs = spark.sql("SELECT inline(eDwithinPairs(array(" + origin + "), array(" + far
                + ", " + near + "), CAST(2.0 AS DOUBLE)))").collectAsList();
        assertEquals(1, pairs.size());
        assertEquals(1, pairs.get(0).get(0));
        assertEquals(2, pairs.get(0).get(1));
    }

    /**
     * The text of an aggregate over typed values, each value in a partition of its own, so the
     * transition runs in every partition and the combine joins the partial states the shuffle
     * carries between them; mirrors GeneratedSurfaceTest's aggregate over binary values.
     */
    private static String aggregate(String agg, String in, String... values) {
        StringBuilder rows = new StringBuilder();
        for (String v : values) {
            rows.append(rows.length() == 0 ? "" : ", ").append("('").append(v).append("')");
        }
        return (String) scalar("SELECT asText(" + agg + "(v)) FROM (SELECT /*+ REPARTITION("
                + values.length + ") */ " + in + "(s) AS v FROM VALUES " + rows + " AS t(s))");
    }

    @Test
    void temporalAggregatesAnswerWhatMobilityDBAnswers() {
        // The values and the expected texts of GeneratedSurfaceTest, what MobilityDB answers for
        // the same aggregate over the same values with the time zone set to UTC.
        String[] tints = {"[1@2001-01-01 00:00:00+00, 2@2001-01-03 00:00:00+00]", "[3@2001-01-02 00:00:00+00, 4@2001-01-04 00:00:00+00]",
                          "[5@2001-01-03 00:00:00+00, 5@2001-01-05 00:00:00+00]"};
        assertEquals("{[1@2001-01-01 00:00:00+00, 2@2001-01-02 00:00:00+00, "
            + "3@2001-01-03 00:00:00+00], (2@2001-01-03 00:00:00+00, 2@2001-01-04 00:00:00+00], "
            + "(1@2001-01-04 00:00:00+00, 1@2001-01-05 00:00:00+00]}",
            aggregate("tCount", "tintFromText", tints));
        assertEquals("{[1@2001-01-01 00:00:00+00, 2@2001-01-03 00:00:00+00], "
            + "(3@2001-01-03 00:00:00+00, 4@2001-01-04 00:00:00+00], "
            + "(5@2001-01-04 00:00:00+00, 5@2001-01-05 00:00:00+00]}",
            aggregate("tMinAgg", "tintFromText", tints));
        assertEquals("{[1@2001-01-01 00:00:00+00, 3@2001-01-02 00:00:00+00, "
            + "5@2001-01-03 00:00:00+00, 5@2001-01-05 00:00:00+00]}",
            aggregate("tMaxAgg", "tintFromText", tints));
        assertEquals("{[1@2001-01-01 00:00:00+00, 4@2001-01-02 00:00:00+00, "
            + "10@2001-01-03 00:00:00+00], (8@2001-01-03 00:00:00+00, 9@2001-01-04 00:00:00+00], "
            + "(5@2001-01-04 00:00:00+00, 5@2001-01-05 00:00:00+00]}",
            aggregate("tSum", "tintFromText", tints));
        assertEquals("{[1@2001-01-01 00:00:00+00, 2@2001-01-02 00:00:00+00], "
            + "[3@2001-01-03 00:00:00+00, 4@2001-01-04 00:00:00+00], "
            + "[5@2001-01-05 00:00:00+00, 6@2001-01-06 00:00:00+00]}",
            aggregate("mergeAgg", "tintFromText", "[1@2001-01-01 00:00:00+00, 2@2001-01-02 00:00:00+00]",
                      "[3@2001-01-03 00:00:00+00, 4@2001-01-04 00:00:00+00]", "[5@2001-01-05 00:00:00+00, 6@2001-01-06 00:00:00+00]"));
        String[] tbools = {"[true@2001-01-01 00:00:00+00, false@2001-01-03 00:00:00+00]",
                           "[true@2001-01-02 00:00:00+00, true@2001-01-04 00:00:00+00]"};
        assertEquals("{[t@2001-01-01 00:00:00+00, f@2001-01-03 00:00:00+00], "
            + "(t@2001-01-03 00:00:00+00, t@2001-01-04 00:00:00+00]}",
            aggregate("tAndAgg", "tboolFromText", tbools));
        assertEquals("{[t@2001-01-01 00:00:00+00, t@2001-01-04 00:00:00+00]}",
            aggregate("tOrAgg", "tboolFromText", tbools));
        // the result is a typed value the surface reads on: an int over the tint tCount answers
        assertEquals("tint", type("SELECT tCount(" + tint + ")"));
        assertEquals(3, scalar("SELECT maxValue(tCount(v)) FROM (SELECT /*+ REPARTITION(3) */ "
            + "tintFromText(s) AS v FROM VALUES ('" + String.join("'), ('", tints) + "') AS t(s))"));
    }

    @Test
    void aggregatesOfEveryStateAnswerWhatMobilityDBAnswers() {
        // What MobilityDB answers for the same aggregate over the same values with the time zone
        // set to UTC, each value in a partition of its own, so every partial state crosses the
        // shuffle: the skip list of tAvg and tCentroid as the bytes taggstate_serialize writes,
        // the set and span set of the unions as setstate_serialize and spansetstate_serialize
        // write them, and the box of extent as its WKB.
        assertEquals("{[1@2001-01-01 00:00:00+00, 2@2001-01-02 00:00:00+00), "
            + "[2.5@2001-01-02 00:00:00+00, 3.5@2001-01-03 00:00:00+00], "
            + "(4@2001-01-03 00:00:00+00, 5@2001-01-04 00:00:00+00]}",
            aggregate("tAvg", "tfloatFromText", "[1@2001-01-01 00:00:00+00, 3@2001-01-03 00:00:00+00]",
                      "[3@2001-01-02 00:00:00+00, 5@2001-01-04 00:00:00+00]"));
        assertEquals("{[POINT(1 0)@2001-01-01 00:00:00+00, POINT(3 2)@2001-01-03 00:00:00+00]}",
            aggregate("tCentroid", "tgeompointFromText",
                      "[Point(0 0)@2001-01-01 00:00:00+00, Point(2 2)@2001-01-03 00:00:00+00]",
                      "[Point(2 0)@2001-01-01 00:00:00+00, Point(4 2)@2001-01-03 00:00:00+00]"));
        assertEquals("{1, 2, 3, 5}", aggregate("setUnionAgg", "intsetFromText", "{1, 3}", "{2, 3, 5}"));
        assertEquals("{[1, 5), [7, 8)}",
            aggregate("spanUnionAgg", "intspanFromText", "[1, 3)", "[2, 5)", "[7, 8)"));
        assertEquals("STBOX X((0,1),(2,4))",
            aggregate("extent", "stboxFromText", "STBOX X((1,1),(2,2))", "STBOX X((0,3),(1,4))"));
        assertEquals("TBOXFLOAT XT([1, 5],[2001-01-01 00:00:00+00, 2001-01-04 00:00:00+00])",
            aggregate("extent", "tfloatFromText", "[1@2001-01-01 00:00:00+00, 3@2001-01-03 00:00:00+00]",
                      "[3@2001-01-02 00:00:00+00, 5@2001-01-04 00:00:00+00]"));
    }

    @Test
    void peakCountOverMergedPieces() {
        // The pieces of one trip are merged first, so the count counts trips, not pieces: two
        // trips overlap on the second day, one of them stored as two pieces.
        String rows = String.join(", ",
            "(1, '[Point(0 0)@2020-01-01 00:00:00+00, Point(1 1)@2020-01-02 00:00:00+00]')",
            "(1, '[Point(1 1)@2020-01-03 00:00:00+00, Point(2 2)@2020-01-04 00:00:00+00]')",
            "(2, '[Point(5 5)@2020-01-02 00:00:00+00, Point(6 6)@2020-01-03 00:00:00+00]')");
        assertEquals(2, scalar("SELECT maxValue(tCount(g)) FROM (SELECT k, mergeAgg(g) AS g FROM "
            + "(SELECT k, tgeompointFromText(s) AS g FROM VALUES " + rows + " AS t(k, s)) GROUP BY k)"));
        assertEquals(1, scalar("SELECT maxValue(tCount(g)) FROM (SELECT k, mergeAgg(g) AS g FROM "
            + "(SELECT k, tgeompointFromText(s) AS g FROM VALUES " + rows + " AS t(k, s)) "
            + "WHERE k = 1 GROUP BY k)"));
    }
}
