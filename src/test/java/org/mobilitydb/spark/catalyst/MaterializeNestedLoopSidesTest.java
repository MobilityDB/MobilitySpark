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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * The rule in a session: a join with no equality between its sides, whose condition calls a
 * user-defined function, puts each side behind a boundary, and a join that has such an equality
 * is left alone. Both queries answer what they answer without the rule.
 */
public class MaterializeNestedLoopSidesTest {

    private static SparkSession spark;

    @BeforeAll
    static void session() {
        spark = SparkSession.builder().appName("materialize-sides").master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.extensions", MobilitySparkExtensions.class.getName())
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.udf().register("near", (UDF2<Long, Long, Boolean>) (a, b) -> Math.abs(a - b) <= 1,
                DataTypes.BooleanType);
        spark.range(0, 8).createOrReplaceTempView("t");
        spark.sql("SELECT id, id AS k FROM t").createOrReplaceTempView("u");
    }

    @AfterAll
    static void stop() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static String plan(Dataset<Row> query) {
        return query.queryExecution().optimizedPlan().toString();
    }

    @Test
    void aNestedLoopSideCallingTheFunctionGoesBehindABoundary() {
        // The condition reads each side's computed column, so column pruning keeps the call on
        // the side: an unused projection is pruned and the side optimizes to its bare source
        Dataset<Row> query = spark.sql(
                "SELECT count(*) AS n FROM (SELECT id, near(id, id) AS flag FROM t) a "
                        + "JOIN (SELECT id, near(id, id) AS flag FROM t) b "
                        + "ON a.id < b.id AND a.flag AND b.flag AND near(a.id, b.id)");
        assertTrue(plan(query).contains("Repartition"),
                "no boundary under the nested-loop join: " + plan(query));
        // b - a <= 1 and a < b hold for b = a + 1 alone, so seven pairs of the eight ids
        assertEquals(7L, query.collectAsList().get(0).getLong(0),
                "the boundary answers what the query answered");
    }

    @Test
    void aJoinByEqualityIsLeftAlone() {
        Dataset<Row> query = spark.sql(
                "SELECT count(*) AS n FROM (SELECT id, near(id, id) AS flag FROM t) a "
                        + "JOIN (SELECT id AS id2, k FROM u) b ON a.id = b.k");
        assertFalse(plan(query).contains("Repartition"),
                "a join by equality reads each side once already: " + plan(query));
        assertEquals(8L, query.collectAsList().get(0).getLong(0),
                "the query answers one row per id");
    }
}
