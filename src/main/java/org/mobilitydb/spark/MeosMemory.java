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

import jnr.ffi.Pointer;
import sun.misc.Unsafe;
import java.lang.reflect.Field;

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
 * Implementation uses sun.misc.Unsafe.freeMemory() which calls the system
 * free() underneath — safe for MEOS pointers since MEOS standalone mode
 * uses the system allocator.  This avoids JNR-FFI classloader boundary
 * issues that arise when loading libc via LibraryLoader inside Spark.
 *
 * Usage:
 * <pre>
 *   Pointer tptr = functions.temporal_from_hexwkb(hex);
 *   try {
 *       // ... use tptr ...
 *   } finally {
 *       MeosMemory.free(tptr);
 *   }
 * </pre>
 */
public final class MeosMemory {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private MeosMemory() {}

    /** Free a native pointer allocated by MEOS.  Null-safe. */
    public static void free(Pointer ptr) {
        if (ptr != null) UNSAFE.freeMemory(ptr.address());
    }

    /** Free multiple native pointers in one call.  Null-safe. */
    public static void free(Pointer... ptrs) {
        for (Pointer p : ptrs) {
            if (p != null) UNSAFE.freeMemory(p.address());
        }
    }
}
