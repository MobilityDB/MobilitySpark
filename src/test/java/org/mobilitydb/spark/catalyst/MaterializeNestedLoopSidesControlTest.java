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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The control for {@link MaterializeNestedLoopSidesTest}: the same query in a session WITHOUT the
 * extension carries no boundary under the join, which is what makes the other class's assertion a
 * statement about the rule rather than about Spark's own optimizer. A session carries its
 * extensions from the builder and a JVM holds one Spark context, so the control needs its own
 * class: surefire forks per class and does not reuse a fork.
 */
public class MaterializeNestedLoopSidesControlTest {

    private static SparkSession spark;

    @BeforeAll
    static void session() {
        spark = SparkSession.builder().appName("materialize-sides-control").master("local[1]")
                .config("spark.ui.enabled", "false")
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

    @Test
    void withoutTheRuleNoBoundaryAppears() {
        // The same query the rule's own test runs, its condition reading each side's computed
        // column so column pruning keeps the call on the side
        Dataset<Row> query = spark.sql(
                "SELECT count(*) AS n FROM (SELECT id, near(id, id) AS flag FROM t) a "
                        + "JOIN (SELECT id, near(id, id) AS flag FROM t) b "
                        + "ON a.id < b.id AND a.flag AND b.flag AND near(a.id, b.id)");
        String plan = query.queryExecution().optimizedPlan().toString();
        assertFalse(plan.contains("Repartition"),
                "Spark puts a boundary there by itself, so the other test proves nothing: " + plan);
        // b - a <= 1 and a < b hold for b = a + 1 alone, so seven pairs of the eight ids
        assertEquals(7L, query.collectAsList().get(0).getLong(0),
                "the query answers seven pairs either way");
    }
}
