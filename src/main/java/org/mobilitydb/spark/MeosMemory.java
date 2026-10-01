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

package org.mobilitydb.spark;

import com.kenai.jffi.MemoryIO;
import jnr.ffi.Pointer;

/**
 * Native memory management for MEOS objects returned by JNR-FFI calls.
 *
 * MEOS standalone mode allocates temporal objects with the system malloc
 * (palloc/pfree map to malloc/free when not running inside PostgreSQL).
 * JNR-FFI Pointer values returned from MEOS functions are raw native
 * addresses — they are NOT tracked by the Java GC.  Callers must free
 * each Pointer explicitly after use, otherwise the native heap grows
 * without bound (one leaked Temporal* per UDF call × millions of rows
 * in cross-join queries like Q2/Q4/Q5/Q6).
 *
 * Implementation uses jffi's MemoryIO.freeMemory(), which calls the system
 * free() — safe for MEOS pointers since MEOS standalone mode uses the
 * system allocator.  jffi is the native layer JNR-FFI itself runs on, so it
 * shares the classloader of every MEOS call inside Spark, without loading
 * libc through LibraryLoader and without an internal JDK API.
 *
 * Usage:
 * <pre>
 *   Pointer tptr = GeneratedFunctions.temporal_from_wkb(wkb);
 *   try {
 *       // ... use tptr ...
 *   } finally {
 *       MeosMemory.free(tptr);
 *   }
 * </pre>
 */
public final class MeosMemory {

    private static final MemoryIO IO = MemoryIO.getInstance();

    private MeosMemory() {}

    /** Free a native pointer allocated by MEOS.  Null-safe. */
    public static void free(Pointer ptr) {
        if (ptr != null) IO.freeMemory(ptr.address());
    }

    /** Free multiple native pointers in one call.  Null-safe. */
    public static void free(Pointer... ptrs) {
        for (Pointer p : ptrs) {
            if (p != null) IO.freeMemory(p.address());
        }
    }
}
