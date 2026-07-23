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

/**
 * MEOS initialisation for Spark executor threads.
 *
 * Spark runs tasks on a pool of executor threads, and MEOS setup has two
 * lifetimes: process-global state (the allocator and the error handler) is
 * installed once per JVM, while thread-local state (the timezone and collation
 * caches, and the PROJ, GEOS and GSL contexts) belongs to each thread. This
 * class installs the process-global part once and the thread-local part once
 * per native thread.
 *
 * The entire UDF surface is generated (GeneratedSpatioTemporalUDFs); every
 * generated entry point calls {@link #ensureReady()} before its first MEOS
 * call, so the executor thread running it is always initialised.  There are no
 * hand-registered UDFs, so this class exposes only the guard — no registration
 * helpers.
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

    /**
     * Process-global MEOS setup, installed exactly once per JVM: the allocator
     * and the error handler are process-global, not thread-local. The holder's
     * class initialiser runs under the JVM class-initialisation lock, so a
     * thread that reaches its per-thread setup always sees a fully-installed
     * no-exit handler — MEOS's exiting default is never observable to another
     * thread.
     */
    private static final class ProcessInit {
        static {
            GeneratedFunctions.meos_initialize();
            GeneratedFunctions.meos_initialize_error_handler(NOEXIT_ERROR_HANDLER);
        }
        /** Invoking this forces the class initialiser above to run once. */
        static void ensure() { /* side effect: class initialisation */ }
    }

    /**
     * Per-thread MEOS setup, run once per native thread: only the thread-local
     * caches. The timezone and collation caches are thread-local and set
     * explicitly per thread; the PROJ, GEOS and GSL contexts are thread-local
     * too and are created lazily by MEOS on first use. Full meos_initialize() is
     * NOT run per thread — it re-installs the exiting default error handler that
     * every other thread relies on being the no-exit one.
     */
    private static final ThreadLocal<Boolean> MEOS_READY = ThreadLocal.withInitial(() -> {
        ProcessInit.ensure();
        GeneratedFunctions.meos_initialize_timezone("UTC");
        GeneratedFunctions.meos_initialize_collation();
        return Boolean.TRUE;
    });

    /** Ensure MEOS is initialised for the calling thread. */
    public static void ensureReady() {
        MEOS_READY.get();
    }
}
