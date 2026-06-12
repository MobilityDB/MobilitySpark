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
 * Unit tests for geo I/O UDFs:
 *   geoAsEwkt, geoAsGeojson, geoFromGeojson.
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GeoUDFsExt3Test {

    private static final String POINT_WKT   = "POINT(4.35 50.85)";
    private static final String POLYGON_WKT = "POLYGON((0 0,1 0,1 1,0 1,0 0))";

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");
    }

    // ------------------------------------------------------------------
    // geoAsEwkt
    // ------------------------------------------------------------------

    @Test @Order(1)
    void geoAsEwkt_point_returns_nonnull() throws Exception {
        String r = GeoUDFs.geoAsEwkt.call(POINT_WKT, 6);
        assertNotNull(r, "geoAsEwkt must return non-null for a valid WKT point");
        assertFalse(r.isBlank());
    }

    @Test @Order(2)
    void geoAsEwkt_point_contains_keyword() throws Exception {
        String r = GeoUDFs.geoAsEwkt.call(POINT_WKT, 6);
        assertNotNull(r);
        assertTrue(r.toUpperCase().contains("POINT"), "EWKT must contain POINT keyword");
    }

    @Test @Order(3)
    void geoAsEwkt_polygon_returns_nonnull() throws Exception {
        String r = GeoUDFs.geoAsEwkt.call(POLYGON_WKT, 6);
        assertNotNull(r, "geoAsEwkt must return non-null for a polygon");
        assertFalse(r.isBlank());
    }

    @Test @Order(4)
    void geoAsEwkt_null_wkt_returns_null() throws Exception {
        assertNull(GeoUDFs.geoAsEwkt.call(null, 6));
    }

    @Test @Order(5)
    void geoAsEwkt_null_precision_uses_default() throws Exception {
        String r = GeoUDFs.geoAsEwkt.call(POINT_WKT, null);
        assertNotNull(r, "null precision must fall back to default (15)");
        assertFalse(r.isBlank());
    }

    // ------------------------------------------------------------------
    // geoAsGeojson
    // ------------------------------------------------------------------

    @Test @Order(6)
    void geoAsGeojson_point_returns_nonnull() throws Exception {
        String r = GeoUDFs.geoAsGeojson.call(POINT_WKT, 0, 6);
        assertNotNull(r, "geoAsGeojson must return non-null for a valid WKT point");
        assertFalse(r.isBlank());
    }

    @Test @Order(7)
    void geoAsGeojson_point_contains_type_key() throws Exception {
        String r = GeoUDFs.geoAsGeojson.call(POINT_WKT, 0, 6);
        assertNotNull(r);
        assertTrue(r.contains("\"type\""), "GeoJSON output must contain 'type' key");
    }

    @Test @Order(8)
    void geoAsGeojson_polygon_returns_nonnull() throws Exception {
        String r = GeoUDFs.geoAsGeojson.call(POLYGON_WKT, 0, 6);
        assertNotNull(r, "geoAsGeojson must return non-null for a polygon");
        assertFalse(r.isBlank());
    }

    @Test @Order(9)
    void geoAsGeojson_null_returns_null() throws Exception {
        assertNull(GeoUDFs.geoAsGeojson.call(null, 0, 6));
    }

    @Test @Order(10)
    void geoAsGeojson_null_options_uses_default() throws Exception {
        String r = GeoUDFs.geoAsGeojson.call(POINT_WKT, null, 6);
        assertNotNull(r, "null options must fall back to default (0)");
    }

    // ------------------------------------------------------------------
    // geoFromGeojson
    // ------------------------------------------------------------------

    @Test @Order(11)
    void geoFromGeojson_point_returns_wkt() throws Exception {
        String geojson = "{\"type\":\"Point\",\"coordinates\":[4.35,50.85]}";
        String r = GeoUDFs.geoFromGeojson.call(geojson);
        assertNotNull(r, "geoFromGeojson must return non-null for valid GeoJSON");
        assertFalse(r.isBlank());
    }

    @Test @Order(12)
    void geoFromGeojson_roundtrip_contains_point() throws Exception {
        String geojson = "{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}";
        String r = GeoUDFs.geoFromGeojson.call(geojson);
        assertNotNull(r);
        assertTrue(r.toUpperCase().contains("POINT"), "round-trip WKT must contain POINT");
    }

    @Test @Order(13)
    void geoFromGeojson_null_returns_null() throws Exception {
        assertNull(GeoUDFs.geoFromGeojson.call(null));
    }

    @Test @Order(14)
    void geoAsGeojson_then_fromGeojson_roundtrip_consistent() throws Exception {
        String geojson = GeoUDFs.geoAsGeojson.call(POINT_WKT, 0, 9);
        assertNotNull(geojson);
        String wkt = GeoUDFs.geoFromGeojson.call(geojson);
        assertNotNull(wkt, "round-trip WKT→GeoJSON→WKT must not be null");
        assertTrue(wkt.toUpperCase().contains("POINT"));
    }
}
