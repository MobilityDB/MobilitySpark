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

import functions.GeneratedFunctions;
import functions.error_handler_fn;
import org.apache.spark.sql.api.java.*;

/**
 * Per-thread MEOS initialisation for Spark executor threads.
 *
 * In Spark's multi-threaded executor model every task thread initialises
 * MEOS independently.  meos_initialize() sets up the per-thread MEOS state
 * (session_timezone, timezone cache, GEOS context, PROJ context, GSL RNGs,
 * errno).  The ThreadLocal in MEOS_READY runs initialisation exactly once
 * per native thread.
 *
 * Usage — two patterns:
 *
 *   1. Wrap lambdas at registration time (preferred — no boilerplate in the
 *      lambda body and impossible to forget):
 *        spark.udf().register("foo", MeosThread.wrap((String s) -> ...), Type);
 *
 *   2. Call ensureReady() explicitly at the top of a lambda where wrapping is
 *      not convenient.
 */
public final class MeosThread {

    private MeosThread() {}

    /**
     * No-exit MEOS error handler.  MEOS's default handler calls
     * exit(EXIT_FAILURE) on an ERROR, which would tear down the whole JVM if a
     * MEOS error fired inside a Spark task.  This handler returns instead of
     * exiting; the error still surfaces because MEOS sets meos_errno, which the
     * generated wrappers check (MeosErrorHandler.checkError) and rethrow as a
     * Java exception.  Held as a static field so JNR keeps the native callback
     * alive for the process lifetime.
     */
    public static final error_handler_fn NOEXIT_ERROR_HANDLER =
        (errorLevel, errorCode, errorMessage) -> { /* do not exit the JVM */ };

    private static final ThreadLocal<Boolean> MEOS_READY = ThreadLocal.withInitial(() -> {
        GeneratedFunctions.meos_initialize();
        GeneratedFunctions.meos_initialize_timezone("UTC");
        GeneratedFunctions.meos_initialize_error_handler(NOEXIT_ERROR_HANDLER);
        return Boolean.TRUE;
    });

    /** Ensure MEOS is initialised for the calling thread. */
    public static void ensureReady() {
        MEOS_READY.get();
    }

    // ------------------------------------------------------------------
    // UDF wrappers — call ensureReady() before delegating to the lambda.
    // Use these in registerAll() instead of scattering ensureReady() calls
    // inside every individual UDF method body.
    // ------------------------------------------------------------------

    public static <R> UDF1<String, R> wrap(UDF1<String, R> udf) {
        return s -> { ensureReady(); return udf.call(s); };
    }

    public static <A, R> UDF2<String, A, R> wrap(UDF2<String, A, R> udf) {
        return (s, a) -> { ensureReady(); return udf.call(s, a); };
    }

    public static <A, B, R> UDF3<String, A, B, R> wrap(UDF3<String, A, B, R> udf) {
        return (s, a, b) -> { ensureReady(); return udf.call(s, a, b); };
    }
}
