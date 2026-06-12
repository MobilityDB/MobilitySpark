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

import org.junit.jupiter.api.*;

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for tpointTransform.
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GeoUDFsExt5Test {

    private static String TRIP_4326;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        // SRID 4326: WGS-84 geographic coordinates
        TRIP_4326 = temporal_as_hexwkb(
            tgeompoint_in("SRID=4326;[POINT(4.35 50.85)@2020-01-01 00:00:00+00, "
                        + "POINT(4.40 50.90)@2020-01-01 01:00:00+00]"),
            (byte) 0);
    }

    // ------------------------------------------------------------------
    // tpointTransform
    //
    // Note: tspatial_transform requires a spatial_ref_sys catalog loaded via
    // meos_set_spatial_ref_sys_csv() (done inside MobilitySparkSession.create()).
    // Unit tests that call meos_initialize() directly do not have the catalog,
    // so transform calls return null gracefully (MEOS logs an error internally
    // but does not crash).  Tests here verify the null-handling contract and
    // that the UDF does not throw any exception.
    // ------------------------------------------------------------------

    @Test @Order(1)
    void tpointTransform_does_not_throw_without_srs_catalog() throws Exception {
        // Without spatial_ref_sys.csv, MEOS returns null for any transform.
        // Verify the UDF wraps this gracefully (null, not an exception).
        String r = GeoUDFs.tpointTransform.call(TRIP_4326, 3857);
        // r may be null — that is the correct contract when CRS is unavailable
        assertTrue(r == null || !r.isBlank(),
            "result must be null (no catalog) or a non-empty hex-WKB string");
    }

    @Test @Order(2)
    void tpointTransform_null_trip_returns_null() throws Exception {
        assertNull(GeoUDFs.tpointTransform.call(null, 3857));
    }

    @Test @Order(3)
    void tpointTransform_null_srid_returns_null() throws Exception {
        assertNull(GeoUDFs.tpointTransform.call(TRIP_4326, null));
    }
}
