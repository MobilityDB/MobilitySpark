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

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BucketUDFsTest extends MeosTestBase {

    @Test @Order(1)
    void floatBucket_aligns_to_origin() throws Exception {
        // 7.3 with size 1.0, origin 0 → bucket [7.0, 8.0) → 7.0
        assertEquals(7.0, BucketUDFs.floatBucket.call(7.3, 1.0, 0.0), 1e-9);
    }

    @Test @Order(2)
    void floatBucket_negative_value() throws Exception {
        // -0.5 with size 1.0, origin 0 → bucket [-1.0, 0.0) → -1.0
        assertEquals(-1.0, BucketUDFs.floatBucket.call(-0.5, 1.0, 0.0), 1e-9);
    }

    @Test @Order(3)
    void floatBucket_origin_offset() throws Exception {
        // 7.3 with size 1.0, origin 0.5 → bucket [6.5, 7.5) → 6.5
        assertEquals(6.5, BucketUDFs.floatBucket.call(7.3, 1.0, 0.5), 1e-9);
    }

    @Test @Order(4)
    void floatBucket_null_value_returns_null() throws Exception {
        assertNull(BucketUDFs.floatBucket.call(null, 1.0, 0.0));
    }

    @Test @Order(5)
    void floatBucket_null_size_returns_null() throws Exception {
        assertNull(BucketUDFs.floatBucket.call(7.3, null, 0.0));
    }

    @Test @Order(6)
    void intBucket_basic() throws Exception {
        // 17 with size 5, origin 0 → bucket [15, 20) → 15
        assertEquals(15, BucketUDFs.intBucket.call(17, 5, 0));
    }

    @Test @Order(7)
    void intBucket_exact_boundary() throws Exception {
        // 20 with size 5, origin 0 → bucket [20, 25) → 20
        assertEquals(20, BucketUDFs.intBucket.call(20, 5, 0));
    }

    @Test @Order(8)
    void intBucket_negative() throws Exception {
        assertEquals(-5, BucketUDFs.intBucket.call(-1, 5, 0));
    }

    @Test @Order(9)
    void intBucket_null_returns_null() throws Exception {
        assertNull(BucketUDFs.intBucket.call(null, 5, 0));
        assertNull(BucketUDFs.intBucket.call(17, null, 0));
        assertNull(BucketUDFs.intBucket.call(17, 5, null));
    }
}
