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

package org.mobilitydb.spark.temporal;

import org.junit.jupiter.api.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.mobilitydb.spark.cbuffer.CbufferUDFs;
import org.mobilitydb.spark.pose.PoseUDFs;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ComparisonUDFs — the equality / ordering / three-way-compare
 * / hash UDFs for every scalar, set, span and box type plus generic temporal.
 *
 * <p>The MEOS sort order is not hard-coded; instead each type is checked for
 * the ordering invariants every total order must satisfy: reflexivity,
 * antisymmetry of cmp, and lt/le/gt/ge agreeing with the sign of cmp.  Hashes
 * are checked for determinism, and every entry point for null-safety.
 *
 * MEOS function authority: meos/include/meos.h, meos_cbuffer.h,
 * meos_npoint.h, meos_pose.h.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ComparisonUDFsTest {

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");
    }

    /** Assert the total-order + hash + null invariants for one type. */
    private static void checkOrdering(
            String type,
            UDF2<String, String, Boolean> eq, UDF2<String, String, Boolean> ne,
            UDF2<String, String, Boolean> lt, UDF2<String, String, Boolean> le,
            UDF2<String, String, Boolean> gt, UDF2<String, String, Boolean> ge,
            UDF2<String, String, Integer> cmp,
            UDF1<String, Integer> hash, UDF2<String, Long, Long> hashExtended,
            String a, String b) throws Exception {
        assertNotNull(a, type + ": sample a was not constructed");
        assertNotNull(b, type + ": sample b was not constructed");

        // Reflexive.
        assertTrue(eq.call(a, a),  type + " eq(a,a)");
        assertFalse(ne.call(a, a), type + " ne(a,a)");
        assertFalse(lt.call(a, a), type + " lt(a,a)");
        assertFalse(gt.call(a, a), type + " gt(a,a)");
        assertTrue(le.call(a, a),  type + " le(a,a)");
        assertTrue(ge.call(a, a),  type + " ge(a,a)");
        assertEquals(0, cmp.call(a, a).intValue(), type + " cmp(a,a)==0");

        // Distinct values.
        assertFalse(eq.call(a, b), type + " eq(a,b)");
        assertTrue(ne.call(a, b),  type + " ne(a,b)");
        int c = cmp.call(a, b);
        assertNotEquals(0, c, type + " cmp(a,b)!=0");
        assertEquals(Integer.signum(c), -Integer.signum(cmp.call(b, a)),
            type + " cmp antisymmetry");

        // Ordering predicates agree with the sign of cmp.
        assertEquals(c < 0,  lt.call(a, b), type + " lt agrees with cmp");
        assertEquals(c > 0,  gt.call(a, b), type + " gt agrees with cmp");
        assertEquals(c <= 0, le.call(a, b), type + " le agrees with cmp");
        assertEquals(c >= 0, ge.call(a, b), type + " ge agrees with cmp");

        // Hashing is deterministic.
        if (hash != null)
            assertEquals(hash.call(a), hash.call(a), type + " hash deterministic");
        if (hashExtended != null)
            assertEquals(hashExtended.call(a, 1L), hashExtended.call(a, 1L),
                type + " hashExtended deterministic");

        // Null-safety.
        assertNull(eq.call(null, a), type + " eq(null,a)");
        assertNull(eq.call(a, null), type + " eq(a,null)");
        assertNull(cmp.call(null, b), type + " cmp(null,b)");
        if (hash != null) assertNull(hash.call(null), type + " hash(null)");
    }

    @Test @Order(1)
    void cbuffer_ordering() throws Exception {
        String a = CbufferUDFs.cbuffer.call("POINT(1 1)", 3.0);
        String b = CbufferUDFs.cbuffer.call("POINT(1 1)", 5.0);
        // cbuffer_hash / cbuffer_hash_extended are not bound: MEOS hashes the
        // embedded geometry's raw bytes incl. uninitialized padding, which is
        // non-deterministic under malloc-based standalone MEOS. Pending an
        // upstream MEOS determinism fix; the seven ordering ops are exact.
        checkOrdering("cbuffer",
            ComparisonUDFs.cbufferEq, ComparisonUDFs.cbufferNe, ComparisonUDFs.cbufferLt,
            ComparisonUDFs.cbufferLe, ComparisonUDFs.cbufferGt, ComparisonUDFs.cbufferGe,
            ComparisonUDFs.cbufferCmp, null, null, a, b);
    }

    // npoint / nsegment ordering is NOT unit-tested here because *constructing*
    // a sample requires a loaded `ways` network table: npoint_make / nsegment_make
    // call ensure_route_exists(rid) -> route_lookup() against the ways cache, which
    // is absent in the network-less unit / CI environment (so npoint_make returns
    // null). The npointEq/Cmp/... and nsegmentEq/Cmp/... UDFs are still registered
    // and parse-and-compare correctly given network-valid inputs (parsing does not
    // validate the route). Restoring coverage needs a minimal ways fixture — tracked
    // separately. The eight network-free types below exercise the same code paths.

    @Test @Order(4)
    void pose_ordering() throws Exception {
        String a = PoseUDFs.pose.call(1.0, 2.0, 0.5);
        String b = PoseUDFs.pose.call(3.0, 4.0, 0.5);
        checkOrdering("pose",
            ComparisonUDFs.poseEq, ComparisonUDFs.poseNe, ComparisonUDFs.poseLt,
            ComparisonUDFs.poseLe, ComparisonUDFs.poseGt, ComparisonUDFs.poseGe,
            ComparisonUDFs.poseCmp, ComparisonUDFs.poseHash,
            ComparisonUDFs.poseHashExtended, a, b);
    }

    @Test @Order(5)
    void set_ordering() throws Exception {
        String a = set_as_hexwkb(intset_in("{1, 2, 3}"), (byte) 0);
        String b = set_as_hexwkb(intset_in("{4, 5, 6}"), (byte) 0);
        checkOrdering("set",
            ComparisonUDFs.setEq, ComparisonUDFs.setNe, ComparisonUDFs.setLt,
            ComparisonUDFs.setLe, ComparisonUDFs.setGt, ComparisonUDFs.setGe,
            ComparisonUDFs.setCmp, ComparisonUDFs.setHash,
            ComparisonUDFs.setHashExtended, a, b);
    }

    @Test @Order(6)
    void span_ordering() throws Exception {
        String a = span_as_hexwkb(intspan_in("[1, 10]"), (byte) 0);
        String b = span_as_hexwkb(intspan_in("[20, 30]"), (byte) 0);
        checkOrdering("span",
            ComparisonUDFs.spanEq, ComparisonUDFs.spanNe, ComparisonUDFs.spanLt,
            ComparisonUDFs.spanLe, ComparisonUDFs.spanGt, ComparisonUDFs.spanGe,
            ComparisonUDFs.spanCmp, ComparisonUDFs.spanHash,
            ComparisonUDFs.spanHashExtended, a, b);
    }

    @Test @Order(7)
    void spanset_ordering() throws Exception {
        String a = spanset_as_hexwkb(intspanset_in("{[1, 5], [10, 20]}"), (byte) 0);
        String b = spanset_as_hexwkb(intspanset_in("{[100, 200]}"), (byte) 0);
        checkOrdering("spanset",
            ComparisonUDFs.spansetEq, ComparisonUDFs.spansetNe, ComparisonUDFs.spansetLt,
            ComparisonUDFs.spansetLe, ComparisonUDFs.spansetGt, ComparisonUDFs.spansetGe,
            ComparisonUDFs.spansetCmp, ComparisonUDFs.spansetHash,
            ComparisonUDFs.spansetHashExtended, a, b);
    }

    @Test @Order(8)
    void tbox_ordering() throws Exception {
        String a = ConstructorUDFs.tbox.call(
            "TBOXINT XT([1,4],[2020-01-01 00:00:00+00, 2020-01-01 03:00:00+00])");
        String b = ConstructorUDFs.tbox.call(
            "TBOXINT XT([10,40],[2020-01-01 00:00:00+00, 2020-01-01 03:00:00+00])");
        checkOrdering("tbox",
            ComparisonUDFs.tboxEq, ComparisonUDFs.tboxNe, ComparisonUDFs.tboxLt,
            ComparisonUDFs.tboxLe, ComparisonUDFs.tboxGt, ComparisonUDFs.tboxGe,
            ComparisonUDFs.tboxCmp, ComparisonUDFs.tboxHash,
            ComparisonUDFs.tboxHashExtended, a, b);
    }

    @Test @Order(9)
    void stbox_ordering() throws Exception {
        String a = ConstructorUDFs.stbox.call(
            "STBOX XT(((1,1),(2,2)),[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00])");
        String b = ConstructorUDFs.stbox.call(
            "STBOX XT(((10,10),(20,20)),[2020-01-01 00:00:00+00, 2020-01-02 00:00:00+00])");
        checkOrdering("stbox",
            ComparisonUDFs.stboxEq, ComparisonUDFs.stboxNe, ComparisonUDFs.stboxLt,
            ComparisonUDFs.stboxLe, ComparisonUDFs.stboxGt, ComparisonUDFs.stboxGe,
            ComparisonUDFs.stboxCmp, ComparisonUDFs.stboxHash,
            ComparisonUDFs.stboxHashExtended, a, b);
    }

    @Test @Order(10)
    void temporal_cmp_and_hash() throws Exception {
        String a = temporal_as_hexwkb(tint_in("[1@2020-01-01, 2@2020-01-02]"), (byte) 0);
        String b = temporal_as_hexwkb(tint_in("[3@2020-01-01, 4@2020-01-02]"), (byte) 0);
        assertNotNull(a);
        assertNotNull(b);
        assertEquals(0, ComparisonUDFs.temporalCmp.call(a, a).intValue(), "temporalCmp(a,a)");
        int c = ComparisonUDFs.temporalCmp.call(a, b);
        assertNotEquals(0, c, "temporalCmp(a,b)");
        assertEquals(Integer.signum(c), -Integer.signum(ComparisonUDFs.temporalCmp.call(b, a)),
            "temporalCmp antisymmetry");
        assertEquals(ComparisonUDFs.temporalHash.call(a), ComparisonUDFs.temporalHash.call(a),
            "temporalHash deterministic");
        assertNull(ComparisonUDFs.temporalCmp.call(null, a), "temporalCmp(null,a)");
        assertNull(ComparisonUDFs.temporalHash.call(null), "temporalHash(null)");
    }
}
