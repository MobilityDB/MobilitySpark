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
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;

import scala.Option;
import scala.runtime.AbstractPartialFunction;

/**
 * Moves every conjunct that calls a user-defined function behind the conjuncts that do not, in a
 * join condition and in a filter.
 *
 * `AND` evaluates its operands in order and stops at the first false one, so a conjunct placed
 * later is evaluated on fewer rows: the reordering can only reduce the number of evaluations, and
 * it never introduces one, which is why it preserves both the answer and the exceptions a
 * user-defined function can raise. The relative order inside each of the two groups is kept, so a
 * condition already in this shape is returned unchanged.
 *
 * Spark attaches no cost to a user-defined function, so its optimizer leaves the order the query
 * produced. A proximity join over trajectories shows what that costs: the plan evaluates the
 * distance between two trajectories on every pair the join enumerates, while the box and time
 * comparisons in the same conjunction, which reject almost all of those pairs, run after it.
 */
public final class OrderConjunctsByCost extends Rule<LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(new AbstractPartialFunction<LogicalPlan, LogicalPlan>() {
            @Override
            public boolean isDefinedAt(LogicalPlan node) {
                return node instanceof Join || node instanceof Filter;
            }

            @Override
            public LogicalPlan apply(LogicalPlan node) {
                if (node instanceof Filter) {
                    Filter filter = (Filter) node;
                    Expression ordered = order(filter.condition());
                    return ordered == filter.condition() ? filter
                            : new Filter(ordered, filter.child());
                }
                Join join = (Join) node;
                if (join.condition().isEmpty()) {
                    return join;
                }
                Expression condition = join.condition().get();
                Expression ordered = order(condition);
                return ordered == condition ? join
                        : new Join(join.left(), join.right(), join.joinType(),
                                Option.apply(ordered), join.hint());
            }
        });
    }

    /** The same conjunction with the calls to a user-defined function last, or it unchanged */
    private static Expression order(Expression condition) {
        List<Expression> conjuncts = new ArrayList<>();
        split(condition, conjuncts);
        if (conjuncts.size() < 2) {
            return condition;
        }
        List<Expression> plain = new ArrayList<>();
        List<Expression> calls = new ArrayList<>();
        for (Expression conjunct : conjuncts) {
            // A conjunct that is not deterministic keeps its place: its position is observable
            if (!conjunct.deterministic()) {
                return condition;
            }
            (callsUdf(conjunct) ? calls : plain).add(conjunct);
        }
        if (calls.isEmpty() || plain.isEmpty()) {
            return condition;
        }
        plain.addAll(calls);
        Expression ordered = plain.get(0);
        for (int i = 1; i < plain.size(); i++) {
            ordered = new And(ordered, plain.get(i));
        }
        return ordered;
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

    /** Whether the expression calls a user-defined function anywhere inside it */
    private static boolean callsUdf(Expression expression) {
        if (expression instanceof ScalaUDF) {
            return true;
        }
        scala.collection.Iterator<Expression> children = expression.children().iterator();
        while (children.hasNext()) {
            if (callsUdf(children.next())) {
                return true;
            }
        }
        return false;
    }
}
