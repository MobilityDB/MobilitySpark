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

import functions.functions;

/**
 * Per-thread MEOS initialisation for Spark executor threads.
 *
 * In Spark's multi-threaded executor model every task thread must initialise
 * MEOS independently because session_timezone and the timezone cache are
 * thread-local inside libmeos.  Calling ensureReady() at the start of every
 * UDF lambda guarantees that:
 *
 *   1. session_timezone is non-NULL before any timestamp formatting call
 *      (prevents the localsub SIGSEGV crash reported in hs_err_pid*.log).
 *   2. The no-exit error handler is installed, so MEOS logic errors (e.g.
 *      SRID mismatch) set meos_errno() and return rather than calling
 *      exit() which kills the entire JVM process.
 *
 * The ThreadLocal ensures the initialisation runs exactly once per native
 * thread — subsequent calls to ensureReady() are a single volatile read.
 */
public final class MeosThread {

    private MeosThread() {}

    private static final ThreadLocal<Boolean> MEOS_READY = ThreadLocal.withInitial(() -> {
        functions.meos_initialize();
        functions.meos_initialize_timezone("UTC");
        functions.meos_initialize_noexit_error_handler();
        return Boolean.TRUE;
    });

    /** Ensure MEOS is initialised for the calling thread. */
    public static void ensureReady() {
        MEOS_READY.get();
    }
}
