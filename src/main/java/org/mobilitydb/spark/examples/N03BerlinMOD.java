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

package org.mobilitydb.spark.examples;

import org.apache.spark.sql.SparkSession;
import org.mobilitydb.spark.MobilitySparkSession;
import org.mobilitydb.spark.demo.BerlinMODDemo;

/**
 * N03 BerlinMOD — portable SQL benchmark queries Q1/Q3/Q4/Q5/Q6.
 *
 * Delegates to {@link BerlinMODDemo} which runs the five BerlinMOD queries
 * in the portable named-function SQL dialect (identical to MobilityDB and
 * MobilityDuck). Mirrors meos/examples/03_berlinmod_assemble.c.
 *
 * Run with:
 *   spark-submit --class org.mobilitydb.spark.examples.N03BerlinMOD \
 *       target/mobilityspark-0.1.0-SNAPSHOT-spark.jar
 */
public final class N03BerlinMOD {

    public static void main(String[] args) {
        BerlinMODDemo.main(args);
    }
}
