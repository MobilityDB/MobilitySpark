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
import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import org.apache.spark.sql.api.java.*;

/**
 * Per-thread MEOS initialisation for Spark executor threads.
 *
 * In Spark's multi-threaded executor model every task thread must initialise
 * MEOS independently because session_timezone and the timezone cache are
 * thread-local inside libmeos.  The ThreadLocal in MEOS_READY ensures
 * initialisation runs exactly once per native thread.
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
     * Minimal JNR binding to libgeos_c.so for the per-thread initGEOS call.
     *
     * MEOS spatial functions (eIntersects, eContains, ST_*) call into GEOS
     * through liblwgeom.  GEOS's C API requires `initGEOS(noticeFunc,
     * errorFunc)` to be invoked on every thread that will use the library
     * — without it the first GEOS call raises "context handle is
     * uninitialized, call initGEOS" and aborts the process.  MEOS's
     * internal spatial helpers call initGEOS lazily on the first GEOS
     * touch, but Spark's multi-thread executor races multiple task
     * threads through that initialiser concurrently and corrupts the
     * global GEOS state.  Pre-initialising per Spark task thread here
     * serialises the initialisation through the per-thread `MEOS_READY`
     * `ThreadLocal` and avoids the race.
     */
    public interface LibGeosC {
        /**
         * GEOS 3.x thread-safe initialiser.  Returns an opaque
         * GEOSContextHandle_t and stores it in TLS so subsequent
         * reentrant calls find it via thread-local lookup.  Without
         * this call, the first reentrant GEOS function on the thread
         * raises "context handle is uninitialized, call initGEOS"
         * (which is misleading — the modern fix is GEOS_init_r,
         * not the legacy initGEOS).
         */
        Pointer GEOS_init_r();
        void    GEOS_finish_r(Pointer handle);
    }
    private static final LibGeosC GEOS =
        LibraryLoader.create(LibGeosC.class).load("geos_c");

    private static final ThreadLocal<Boolean> MEOS_READY = ThreadLocal.withInitial(() -> {
        functions.meos_initialize();
        functions.meos_initialize_timezone("UTC");
        // Install the no-exit handler so MEOS errors do not call exit() and
        // tear down the JVM. Mainline MEOS (post PR #939) exports this symbol.
        functions.meos_initialize_noexit_error_handler();
        // Initialise GEOS for this thread.  MEOS's per-thread spatial
        // helpers expect an initialised GEOS thread-local context;
        // without this call, the first spatial UDF on this thread
        // crashes the JVM with "context handle is uninitialized".
        GEOS.GEOS_init_r();
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
