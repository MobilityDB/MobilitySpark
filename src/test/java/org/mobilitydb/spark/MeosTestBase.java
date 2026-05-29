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

package org.mobilitydb.spark;

import org.junit.jupiter.api.BeforeAll;

import static functions.GeneratedFunctions.*;

/**
 * Shared base for all JUnit5 test classes that exercise MEOS.
 *
 * MEOS's default error handler calls {@code exit()} on any error, which in a
 * surefire fork silently kills the JVM ("forked VM terminated ... System.exit
 * called?") with no hs_err and no failing assertion.  Installing the noexit
 * error handler turns MEOS errors into recoverable Java-visible failures
 * instead.  Every test class extends this base so the handler is installed
 * exactly once per fork before any test runs.
 *
 * JUnit5 executes a superclass {@code @BeforeAll} before the subclass's own
 * {@code @BeforeAll}, so MEOS is initialised and the noexit handler is
 * installed before any subclass builds its fixtures.
 *
 * Initialisation order mirrors {@code MobilitySparkSession.create()}.
 */
public abstract class MeosTestBase {

    @BeforeAll
    static void __initMeosBase() {
        meos_initialize();
        meos_initialize_timezone("UTC");
        meos_initialize_noexit_error_handler();
    }
}
