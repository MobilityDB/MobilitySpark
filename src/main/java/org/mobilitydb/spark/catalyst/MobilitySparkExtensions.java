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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;

import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/**
 * The Catalyst rules MobilitySpark adds to a Spark session, named in `spark.sql.extensions`:
 *
 * <pre>
 *   SparkSession.builder().config("spark.sql.extensions",
 *       "org.mobilitydb.spark.catalyst.MobilitySparkExtensions")
 * </pre>
 *
 * Spark applies the named class to the session's extensions, as a Scala function of one argument,
 * which this class provides through scala.runtime.AbstractFunction1.
 *
 * It injects {@link OrderConjunctsByCost}, which moves a MobilitySpark function behind the
 * comparisons beside it in one conjunction. Spark holds no cost for a user-defined function, so
 * its optimizer never sinks one: in a proximity join over trajectories the distance is evaluated
 * on every pair the join enumerates, while the box and time comparisons that reject almost all of
 * them run afterwards.
 */
public final class MobilitySparkExtensions
        extends AbstractFunction1<SparkSessionExtensions, BoxedUnit> {

    @Override
    public BoxedUnit apply(SparkSessionExtensions extensions) {
        extensions.injectOptimizerRule(new AbstractFunction1<SparkSession, Rule<LogicalPlan>>() {
            @Override
            public Rule<LogicalPlan> apply(SparkSession session) {
                return new OrderConjunctsByCost();
            }
        });
        extensions.injectOptimizerRule(new AbstractFunction1<SparkSession, Rule<LogicalPlan>>() {
            @Override
            public Rule<LogicalPlan> apply(SparkSession session) {
                return new MaterializeNestedLoopSides(
                        session.sparkContext().defaultParallelism());
            }
        });
        return BoxedUnit.UNIT;
    }
}
