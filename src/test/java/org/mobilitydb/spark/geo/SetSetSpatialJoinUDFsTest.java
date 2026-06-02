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

package org.mobilitydb.spark.geo;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.*;
import org.mobilitydb.spark.MeosTestBase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the set-set spatial-join UDFs (eDwithinPairs, tDwithinPairs,
 * aDisjointPairs) that wrap the MEOS edwithin/tdwithin/adisjoint_tgeoarr_tgeoarr
 * kernel family.  Each UDF resolves the whole N×M trip pairing inside one native
 * call and returns the qualifying 0-based (i, j) index pairs.
 *
 * The trips are stationary points so the distances are exact and time-invariant:
 *   A — origin,        window 1
 *   B — 0.5 from A,    window 1   (within 1.0 of A)
 *   C — 100 from A,    window 1   (outside 1.0 of A and B)
 *   D — origin,        window 2   (temporally disjoint from A)
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SetSetSpatialJoinUDFsTest extends MeosTestBase {

    private static String A, B, C, D;

    @BeforeAll
    static void initTrips() {
        A = trip("POINT(0 0)",   "POINT(0 0)",   "2020-01-01");
        B = trip("POINT(0 0.5)", "POINT(0 0.5)", "2020-01-01");
        C = trip("POINT(0 100)", "POINT(0 100)", "2020-01-01");
        D = trip("POINT(0 0)",   "POINT(0 0)",   "2020-01-02");
    }

    private static String trip(String p0, String p1, String day) {
        return temporal_as_hexwkb(tgeompoint_in(
            "[" + p0 + "@" + day + " 00:00:00+00, "
                + p1 + "@" + day + " 01:00:00+00]"), (byte) 0);
    }

    /** Collect the qualifying i&lt;j index pairs as "i,j" strings. */
    private static Set<String> pairsLt(List<Row> rows) {
        Set<String> s = new HashSet<>();
        assertNotNull(rows, "set-set UDFs never return null (empty list instead)");
        for (Row r : rows) {
            int i = r.getInt(0), j = r.getInt(1);
            if (i < j) s.add(i + "," + j);
        }
        return s;
    }

    @Test @Order(1)
    void eDwithinPairs_keeps_only_the_close_overlapping_pair() throws Exception {
        Set<String> pairs = pairsLt(
            DistanceUDFs.eDwithinPairs.call(List.of(A, B, C), List.of(A, B, C), 1.0));
        // A-B (0,1) are 0.5 apart; A-C and B-C are far → excluded.
        assertEquals(Set.of("0,1"), pairs);
    }

    @Test @Order(2)
    void aDisjointPairs_returns_every_never_intersecting_cross_pair() throws Exception {
        Set<String> pairs = pairsLt(
            DistanceUDFs.aDisjointPairs.call(List.of(A, B, C), List.of(A, B, C)));
        // None of A/B/C ever share a location at a common time → all disjoint.
        assertEquals(Set.of("0,1", "0,2", "1,2"), pairs);
    }

    @Test @Order(3)
    void tDwithinPairs_returns_the_pair_with_a_whenTrue_spanset() throws Exception {
        List<Row> rows = DistanceUDFs.tDwithinPairs.call(List.of(A, B), List.of(A, B), 1.0);
        Set<String> pairs = pairsLt(rows);
        assertEquals(Set.of("0,1"), pairs);
        for (Row r : rows) {
            if (r.getInt(0) == 0 && r.getInt(1) == 1) {
                assertNotNull(r.getString(2), "qualifying pair carries a whenTrue spanset");
                assertFalse(r.getString(2).isEmpty());
            }
        }
    }

    @Test @Order(4)
    void temporally_disjoint_trips_yield_no_pair() throws Exception {
        // A (window 1) and D (window 2) share a location but never a common time.
        assertTrue(DistanceUDFs.eDwithinPairs.call(List.of(A), List.of(D), 1.0).isEmpty());
        assertTrue(DistanceUDFs.tDwithinPairs.call(List.of(A), List.of(D), 1.0).isEmpty());
    }

    @Test @Order(5)
    void null_and_empty_inputs_yield_empty() throws Exception {
        assertTrue(DistanceUDFs.eDwithinPairs.call(null, List.of(A), 1.0).isEmpty());
        assertTrue(DistanceUDFs.eDwithinPairs.call(List.of(A), List.of(), 1.0).isEmpty());
        assertTrue(DistanceUDFs.aDisjointPairs.call(List.of(A), null).isEmpty());
    }
}
