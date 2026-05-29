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

import org.junit.jupiter.api.*;
import org.mobilitydb.spark.geo.GeoUDFs;
import org.mobilitydb.spark.temporal.AnalyticsUDFs;
import org.mobilitydb.spark.temporal.TemporalUDFs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that MeosMemory.free() prevents native heap accumulation.
 *
 * Each test calls a UDF 5 000 times after a warmup run, then asserts
 * that VmRSS (process resident-set size from /proc/self/status) grew
 * by less than 10 MB.  This is the Java-binding equivalent of running
 * MEOS's C smoke tests (geo_test.c, temporal_test.c, setspan_test.c)
 * under {@code valgrind --leak-check=full}.
 *
 * Why VmRSS rather than the Java heap: MEOS allocates objects with the
 * system malloc; the JNR-FFI Pointer wrappers are tiny Java objects —
 * the underlying C memory is invisible to the garbage collector.
 * VmRSS is the only observable that reflects native-heap growth.
 *
 * Threshold rationale: the 10 MB limit accommodates glibc arena
 * fragmentation (~0.1 KB/call) plus the structural char* micro-leak
 * from JNR-FFI String-returning bindings (~0.4 KB/call × 5 000 = 2 MB).
 * Real Temporal* leaks (the Q02 OOM crash root cause) grow at ≥100 KB/call
 * and would produce ≥500 MB growth — far above the 10 MB limit.
 *
 * Tests are Linux-only (reads /proc/self/status).  On non-Linux the
 * vmRssKb() helper returns -1 and the growth check is skipped.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NativeMemoryLeakTest extends MeosTestBase {

    private static final int  WARMUP_ITERS  = 200;
    private static final int  TEST_ITERS    = 5_000;
    // 10 MB tolerates glibc fragmentation + JNR-FFI char* micro-leaks
    // (~0.4 KB/call for hex strings that JMEOS returns as Java String without
    // freeing the underlying C char*).  Real Temporal* leaks grow at ≥100 KB/call
    // (900 KB/call for full BerlinMOD trips) and would far exceed this limit.
    private static final long MAX_GROWTH_KB = 10_240;

    private static String TRIP_HEX;
    private static final String GEOM_WKT = "POINT(0.05 0.0)";
    private static final String PERIOD   = "[2020-01-01 00:00:00+00, 2020-01-01 00:30:00+00]";

    @BeforeAll
    static void initMeos() {
        TRIP_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0.0 0.0)@2020-01-01 00:00:00+00, POINT(0.1 0.0)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    // Intentionally no @AfterAll meos_finalize: calling it in a surefire
    // @AfterAll causes a JVM crash during shutdown hook execution.

    /** Read VmRSS from /proc/self/status in kB; returns -1 on non-Linux. */
    private static long vmRssKb() {
        try (BufferedReader br = new BufferedReader(new FileReader("/proc/self/status"))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.split("\\s+")[1]);
                }
            }
        } catch (IOException ignored) {}
        return -1;
    }

    private static void forceGc() {
        System.gc();
        System.runFinalization();
        System.gc();
    }

    private static void assertNoLeak(long beforeKb, long afterKb, String udfName) {
        if (beforeKb < 0 || afterKb < 0) return; // non-Linux: skip
        long growthKb = afterKb - beforeKb;
        assertTrue(growthKb < MAX_GROWTH_KB,
            udfName + " native heap grew " + growthKb + " KB over " + TEST_ITERS
                + " calls (limit " + MAX_GROWTH_KB + " KB); check MeosMemory.free() in UDF");
    }

    // ------------------------------------------------------------------
    // eIntersects — heaviest leaker in BerlinMOD Q02 before fix.
    // Allocates: Temporal* + STBox* + GSERIALIZED* per call.
    // ------------------------------------------------------------------
    @Test @Order(1)
    void eIntersects_noNativeLeak() throws Exception {
        for (int i = 0; i < WARMUP_ITERS; i++)
            GeoUDFs.eIntersects.call(TRIP_HEX, GEOM_WKT);
        forceGc();
        long before = vmRssKb();

        for (int i = 0; i < TEST_ITERS; i++)
            GeoUDFs.eIntersects.call(TRIP_HEX, GEOM_WKT);
        forceGc();
        assertNoLeak(before, vmRssKb(), "eIntersects");
    }

    // ------------------------------------------------------------------
    // atTime(span) — used by BerlinMOD Q07.
    // Allocates: Temporal* (input) + Span* + Temporal* (result) per call.
    // ------------------------------------------------------------------
    @Test @Order(2)
    void atTime_span_noNativeLeak() throws Exception {
        for (int i = 0; i < WARMUP_ITERS; i++)
            TemporalUDFs.atTime.call(TRIP_HEX, PERIOD);
        forceGc();
        long before = vmRssKb();

        for (int i = 0; i < TEST_ITERS; i++)
            TemporalUDFs.atTime.call(TRIP_HEX, PERIOD);
        forceGc();
        assertNoLeak(before, vmRssKb(), "atTime(span)");
    }

    // ------------------------------------------------------------------
    // tpointSpeed — used by BerlinMOD Q08.
    // Allocates: Temporal* (input) + Temporal* (tfloat result) per call.
    // ------------------------------------------------------------------
    @Test @Order(3)
    void tpointSpeed_noNativeLeak() throws Exception {
        for (int i = 0; i < WARMUP_ITERS; i++)
            AnalyticsUDFs.tpointSpeed.call(TRIP_HEX);
        forceGc();
        long before = vmRssKb();

        for (int i = 0; i < TEST_ITERS; i++)
            AnalyticsUDFs.tpointSpeed.call(TRIP_HEX);
        forceGc();
        assertNoLeak(before, vmRssKb(), "tpointSpeed");
    }

    // ------------------------------------------------------------------
    // tpointLength — used by BerlinMOD QRT.
    // Allocates: Temporal* per call; returns primitive double (no result ptr).
    // ------------------------------------------------------------------
    @Test @Order(4)
    void tpointLength_noNativeLeak() throws Exception {
        for (int i = 0; i < WARMUP_ITERS; i++)
            AnalyticsUDFs.tpointLength.call(TRIP_HEX);
        forceGc();
        long before = vmRssKb();

        for (int i = 0; i < TEST_ITERS; i++)
            AnalyticsUDFs.tpointLength.call(TRIP_HEX);
        forceGc();
        assertNoLeak(before, vmRssKb(), "tpointLength");
    }

    // ------------------------------------------------------------------
    // trajectory — used by BerlinMOD Q01.
    // Allocates: Temporal* + GSERIALIZED* result per call.
    // ------------------------------------------------------------------
    @Test @Order(5)
    void trajectory_noNativeLeak() throws Exception {
        for (int i = 0; i < WARMUP_ITERS; i++)
            GeoUDFs.trajectory.call(TRIP_HEX);
        forceGc();
        long before = vmRssKb();

        for (int i = 0; i < TEST_ITERS; i++)
            GeoUDFs.trajectory.call(TRIP_HEX);
        forceGc();
        assertNoLeak(before, vmRssKb(), "trajectory");
    }
}
