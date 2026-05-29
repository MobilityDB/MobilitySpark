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

import jnr.ffi.Pointer;
import org.junit.jupiter.api.*;
import org.mobilitydb.spark.MeosTestBase;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ConstructorUDFs MFJSON round-trip constructors:
 *   tboolFromMfjson, tintFromMfjson, tfloatFromMfjson, ttextFromMfjson,
 *   tgeompointFromMfjson, tgeogpointFromMfjson.
 *
 * Each test converts a known temporal value to MFJSON then reconstructs it,
 * verifying that the round-trip produces a valid non-null hex-WKB.
 *
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConstructorUDFsExtTest extends MeosTestBase {

    private static String TBOOL_MFJSON;
    private static String TINT_MFJSON;
    private static String TFLOAT_MFJSON;
    private static String TTEXT_MFJSON;
    private static String TGEOMPOINT_MFJSON;
    private static String TGEOGPOINT_MFJSON;

    @BeforeAll
    static void initMeos() {
        Pointer tbool = tbool_in("[true@2020-01-01 00:00:00+00, false@2020-01-02 00:00:00+00]");
        TBOOL_MFJSON = temporal_as_mfjson(tbool, false, 0, 6, null);

        Pointer tint = tint_in("[1@2020-01-01 00:00:00+00, 3@2020-01-03 00:00:00+00]");
        TINT_MFJSON = temporal_as_mfjson(tint, false, 0, 6, null);

        Pointer tfloat = tfloat_in("[1.5@2020-01-01 00:00:00+00, 2.5@2020-01-03 00:00:00+00]");
        TFLOAT_MFJSON = temporal_as_mfjson(tfloat, false, 0, 6, null);

        Pointer ttext = ttext_in("[hello@2020-01-01 00:00:00+00, world@2020-01-02 00:00:00+00]");
        TTEXT_MFJSON = temporal_as_mfjson(ttext, false, 0, 6, null);

        Pointer tgeompoint = tgeompoint_in(
            "[POINT(0 0)@2020-01-01 00:00:00+00, POINT(1 0)@2020-01-01 01:00:00+00]");
        TGEOMPOINT_MFJSON = temporal_as_mfjson(tgeompoint, false, 0, 6, null);

        Pointer tgeogpoint = tgeogpoint_in(
            "[POINT(4.35 50.85)@2020-01-01 00:00:00+00, POINT(4.36 50.86)@2020-01-01 01:00:00+00]");
        TGEOGPOINT_MFJSON = temporal_as_mfjson(tgeogpoint, false, 0, 6, null);
    }

    // ------------------------------------------------------------------
    // tboolFromMfjson
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tboolFromMfjson_round_trip_returns_nonnull() throws Exception {
        assertNotNull(TBOOL_MFJSON, "MFJSON source must be non-null");
        String r = ConstructorUDFs.tboolFromMfjson.call(TBOOL_MFJSON);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void tboolFromMfjson_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.tboolFromMfjson.call(null));
    }

    // ------------------------------------------------------------------
    // tintFromMfjson
    // ------------------------------------------------------------------

    @Test @Order(3)
    void tintFromMfjson_round_trip_returns_nonnull() throws Exception {
        assertNotNull(TINT_MFJSON, "MFJSON source must be non-null");
        String r = ConstructorUDFs.tintFromMfjson.call(TINT_MFJSON);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void tintFromMfjson_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.tintFromMfjson.call(null));
    }

    // ------------------------------------------------------------------
    // tfloatFromMfjson
    // ------------------------------------------------------------------

    @Test @Order(5)
    void tfloatFromMfjson_round_trip_returns_nonnull() throws Exception {
        assertNotNull(TFLOAT_MFJSON, "MFJSON source must be non-null");
        String r = ConstructorUDFs.tfloatFromMfjson.call(TFLOAT_MFJSON);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(6)
    void tfloatFromMfjson_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.tfloatFromMfjson.call(null));
    }

    // ------------------------------------------------------------------
    // ttextFromMfjson
    // ------------------------------------------------------------------

    @Test @Order(7)
    void ttextFromMfjson_round_trip_returns_nonnull() throws Exception {
        assertNotNull(TTEXT_MFJSON, "MFJSON source must be non-null");
        String r = ConstructorUDFs.ttextFromMfjson.call(TTEXT_MFJSON);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(8)
    void ttextFromMfjson_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.ttextFromMfjson.call(null));
    }

    // ------------------------------------------------------------------
    // tgeompointFromMfjson
    // ------------------------------------------------------------------

    @Test @Order(9)
    void tgeompointFromMfjson_round_trip_returns_nonnull() throws Exception {
        assertNotNull(TGEOMPOINT_MFJSON, "MFJSON source must be non-null");
        String r = ConstructorUDFs.tgeompointFromMfjson.call(TGEOMPOINT_MFJSON);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(10)
    void tgeompointFromMfjson_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.tgeompointFromMfjson.call(null));
    }

    // ------------------------------------------------------------------
    // tgeogpointFromMfjson
    // ------------------------------------------------------------------

    @Test @Order(11)
    void tgeogpointFromMfjson_round_trip_returns_nonnull() throws Exception {
        assertNotNull(TGEOGPOINT_MFJSON, "MFJSON source must be non-null");
        String r = ConstructorUDFs.tgeogpointFromMfjson.call(TGEOGPOINT_MFJSON);
        assertNotNull(r);
        assertFalse(r.isBlank());
    }

    @Test @Order(12)
    void tgeogpointFromMfjson_null_returns_null() throws Exception {
        assertNull(ConstructorUDFs.tgeogpointFromMfjson.call(null));
    }
}
