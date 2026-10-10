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

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mobilitydb.spark.sql.MobilitySparkSql;

/**
 * The typed SQL surface on every session of an application naming MobilitySparkExtensions in
 * `spark.sql.extensions`: the session the builder answers and each session newSession() answers,
 * as Spark Connect and the Thrift Server build one per client, resolve a MobilitySpark function and
 * type without a call to MobilitySparkSql.registerAll, and a call to it on such a session answers
 * alike.
 */
public class MobilitySparkExtensionsTest {

    private static final String INSTANT = "tfloat(CAST(1.5 AS DOUBLE), TIMESTAMP '2026-03-01 00:00:00')";

    private static SparkSession spark;

    @BeforeAll
    static void session() {
        spark = SparkSession.builder().appName("session-surface").master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.extensions", MobilitySparkExtensions.class.getName())
                .getOrCreate();
    }

    @AfterAll
    static void stop() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static void assertSurface(SparkSession session) {
        assertEquals("1.5@2026-03-01 00:00:00+00",
                session.sql("SELECT asText(" + INSTANT + ")").collectAsList().get(0).getString(0));
        assertEquals("tfloat",
                session.sql("SELECT " + INSTANT).schema().fields()[0].dataType().simpleString());
    }

    @Test
    void theBuiltSessionHoldsTheSurface() {
        assertSurface(spark);
    }

    @Test
    void aNewSessionHoldsTheSurface() {
        assertSurface(spark.newSession());
    }

    @Test
    void registerAllOnTheSessionAnswersAlike() {
        SparkSession session = spark.newSession();
        MobilitySparkSql.registerAll(session);
        assertSurface(session);
    }
}
