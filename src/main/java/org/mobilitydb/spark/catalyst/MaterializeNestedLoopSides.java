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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Repartition;
import org.apache.spark.sql.catalyst.rules.Rule;

import scala.runtime.AbstractPartialFunction;

/**
 * Computes each side of a nested-loop join once, where a side calls a user-defined function.
 *
 * A join with no equality between its two sides is executed as a nested loop over pairs of
 * partitions, and a side that is a chain of scans and projections is RECOMPUTED for every
 * partition of the other side: with thirty-two partitions a side, each row is read, filtered and
 * projected thirty-two times. Where that projection calls a user-defined function, as a clip of a
 * trajectory does, the recomputation dominates the join. Repartitioning a side puts a boundary
 * under it, so its rows are computed once and every pair reads them back.
 *
 * The rule leaves alone a join that has an equality between its sides, which is executed by hash
 * and reads each side once already, and a side that carries no call, whose recomputation is a
 * scan Spark is good at. A side already behind a boundary is left as it is, so the rule reaches a
 * fixed point.
 */
public final class MaterializeNestedLoopSides extends Rule<LogicalPlan> {

    private final int partitions;

    public MaterializeNestedLoopSides(int partitions) {
        this.partitions = Math.max(1, partitions);
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(new AbstractPartialFunction<LogicalPlan, LogicalPlan>() {
            @Override
            public boolean isDefinedAt(LogicalPlan node) {
                return node instanceof Join;
            }

            @Override
            public LogicalPlan apply(LogicalPlan node) {
                Join join = (Join) node;
                if (join.condition().isEmpty() || joinsByEquality(join)) {
                    return join;
                }
                LogicalPlan left = materialize(join.left());
                LogicalPlan right = materialize(join.right());
                return left == join.left() && right == join.right() ? join
                        : new Join(left, right, join.joinType(), join.condition(), join.hint());
            }
        });
    }

    /** The side behind a boundary, or the side as it is */
    private LogicalPlan materialize(LogicalPlan side) {
        if (side instanceof Repartition || !callsUdf(side)) {
            return side;
        }
        return new Repartition(partitions, true, side);
    }

    /** Whether an equality relates the two sides, which Spark executes by hash */
    private static boolean joinsByEquality(Join join) {
        List<Expression> conjuncts = new ArrayList<>();
        split(join.condition().get(), conjuncts);
        for (Expression conjunct : conjuncts) {
            if (!(conjunct instanceof EqualTo)) {
                continue;
            }
            EqualTo equality = (EqualTo) conjunct;
            boolean leftThenRight = equality.left().references().subsetOf(join.left().outputSet())
                    && equality.right().references().subsetOf(join.right().outputSet());
            boolean rightThenLeft = equality.left().references().subsetOf(join.right().outputSet())
                    && equality.right().references().subsetOf(join.left().outputSet());
            if (leftThenRight || rightThenLeft) {
                return true;
            }
        }
        return false;
    }

    /** The conjuncts of an `AND` tree, left to right */
    private static void split(Expression condition, List<Expression> into) {
        if (condition instanceof And) {
            And and = (And) condition;
            split(and.left(), into);
            split(and.right(), into);
        } else {
            into.add(condition);
        }
    }

    /** Whether any expression of the plan, or of a plan under it, calls a user-defined function */
    private static boolean callsUdf(LogicalPlan plan) {
        scala.collection.Iterator<Expression> expressions = plan.expressions().iterator();
        while (expressions.hasNext()) {
            if (OrderConjunctsByCost.callsUdf(expressions.next())) {
                return true;
            }
        }
        scala.collection.Iterator<LogicalPlan> children = plan.children().iterator();
        while (children.hasNext()) {
            if (callsUdf(children.next())) {
                return true;
            }
        }
        return false;
    }
}
