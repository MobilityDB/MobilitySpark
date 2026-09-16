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

package org.mobilitydb.spark.catalyst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The rule in a session: a join condition naming a user-defined function first is optimized into
 * one naming it last, and the query answers what it answered before.
 */
public class OrderConjunctsByCostTest {

    private static SparkSession spark;

    @BeforeAll
    static void session() {
        spark = SparkSession.builder().appName("order-conjuncts").master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.extensions", MobilitySparkExtensions.class.getName())
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.udf().register("near", (UDF2<Long, Long, Boolean>) (a, b) -> Math.abs(a - b) <= 1,
                DataTypes.BooleanType);
        spark.range(0, 8).createOrReplaceTempView("t");
    }

    @AfterAll
    static void stop() {
        if (spark != null) {
            spark.stop();
        }
    }

    /** The condition of a join, as the optimized plan prints it */
    private static String joinCondition(Dataset<Row> query) {
        for (String line : query.queryExecution().optimizedPlan().toString().split("\n")) {
            if (line.contains("Join ")) {
                return line;
            }
        }
        throw new AssertionError("no join in the optimized plan");
    }

    @Test
    void theCallGoesBehindTheComparisons() {
        Dataset<Row> query = spark.sql(
                "SELECT count(*) AS n FROM t a JOIN t b ON near(a.id, b.id) AND a.id < b.id");
        String condition = joinCondition(query);
        assertTrue(condition.indexOf("near(") > condition.indexOf(" < "),
                "the call to near stays ahead of the comparison: " + condition);
        // b - a <= 1 and a < b hold for b = a + 1 alone, so seven pairs of the eight ids
        assertEquals(7L, query.collectAsList().get(0).getLong(0),
                "the reordering answers what the query answered");
    }

    @Test
    void aConditionAlreadyOrderedIsLeftAlone() {
        Dataset<Row> query = spark.sql(
                "SELECT count(*) AS n FROM t a JOIN t b ON a.id < b.id AND near(a.id, b.id)");
        String condition = joinCondition(query);
        assertTrue(condition.indexOf("near(") > condition.indexOf(" < "),
                "the call to near moved ahead of the comparison: " + condition);
        assertEquals(7L, query.collectAsList().get(0).getLong(0),
                "the query answers what it answered");
    }
}
