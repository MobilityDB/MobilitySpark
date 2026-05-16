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
import org.mobilitydb.spark.MeosTestBase;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SimilarityUDFs — Fréchet and DTW trajectory distances.
 *
 * MEOS function authority: meos/include/meos.h (038_temporal_similarity)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SimilarityUDFsTest extends MeosTestBase {

    private static String TRIP1;
    private static String TRIP2;

    @BeforeAll
    static void initMeos() {
        TRIP1 = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01, POINT(1 0)@2020-01-02, POINT(2 0)@2020-01-03]"),
            (byte) 0);
        TRIP2 = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 1)@2020-01-01, POINT(1 1)@2020-01-02, POINT(2 1)@2020-01-03]"),
            (byte) 0);
    }

    @Test @Order(1)
    void frechetDistance_returns_positive_double() throws Exception {
        Double d = SimilarityUDFs.frechetDistance.call(TRIP1, TRIP2);
        assertNotNull(d, "Fréchet distance should not be null for valid trips");
        assertTrue(d >= 0.0, "Fréchet distance must be non-negative");
    }

    @Test @Order(2)
    void frechetDistance_identical_trips_is_zero() throws Exception {
        Double d = SimilarityUDFs.frechetDistance.call(TRIP1, TRIP1);
        assertNotNull(d);
        assertEquals(0.0, d, 1e-9);
    }

    @Test @Order(3)
    void dynamicTimeWarp_returns_positive_double() throws Exception {
        Double d = SimilarityUDFs.dynamicTimeWarp.call(TRIP1, TRIP2);
        assertNotNull(d, "DTW distance should not be null for valid trips");
        assertTrue(d >= 0.0, "DTW distance must be non-negative");
    }

    @Test @Order(4)
    void dynamicTimeWarp_identical_trips_is_zero() throws Exception {
        Double d = SimilarityUDFs.dynamicTimeWarp.call(TRIP1, TRIP1);
        assertNotNull(d);
        assertEquals(0.0, d, 1e-9);
    }

    @Test @Order(5)
    void frechetDistance_null_input_returns_null() throws Exception {
        assertNull(SimilarityUDFs.frechetDistance.call(null, TRIP2));
        assertNull(SimilarityUDFs.frechetDistance.call(TRIP1, null));
    }

    @Test @Order(6)
    void dynamicTimeWarp_null_input_returns_null() throws Exception {
        assertNull(SimilarityUDFs.dynamicTimeWarp.call(null, TRIP2));
        assertNull(SimilarityUDFs.dynamicTimeWarp.call(TRIP1, null));
    }
}
