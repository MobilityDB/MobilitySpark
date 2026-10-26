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

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mobilitydb.spark.sql.MobilitySparkSql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEOS's raster and Apache Sedona's raster in one Spark session, each converted to the other by
 * the functions the two register: a Sedona raster is built from the georeferencing a MEOS raster
 * states and the values of its band ({@code RS_MakeEmptyRaster}, {@code RS_AddBandFromArray} over
 * {@code dumpValues}), and a MEOS raster from the georeferencing and the values of a Sedona
 * raster ({@code raster(pixels, ...)} over the bytes {@code rasterPixels} reads of each value as
 * the pixel of a one-pixel band, {@code addBand} over {@code makeEmptyRaster}). Pixel type and
 * nodata value, or its absence, cross both ways. Every Sedona name carries the prefix
 * {@code RS_} or {@code ST_}, every MobilityDB name none, so the two never share a name.
 */
class SedonaRasterBridgeTest {

    private static SparkSession spark;
    private static Set<String> sedonaNames;
    private static Set<String> mobilityNames;

    /** The pixel type of a MEOS band as the band data type RS_MakeEmptyRaster takes. */
    private static final String SEDONA_TYPE = "CASE rasterBandPixelType(r, 1) "
        + "WHEN 'uint8' THEN 'B' WHEN 'int16' THEN 'S' WHEN 'uint16' THEN 'US' "
        + "WHEN 'int32' THEN 'I' WHEN 'float32' THEN 'F' WHEN 'float64' THEN 'D' END";

    /** The pixel type RS_BandPixelType answers as the pixel type addBand takes. */
    private static final String MEOS_TYPE = "CASE RS_BandPixelType(s, 1) "
        + "WHEN 'UNSIGNED_8BITS' THEN '8BUI' WHEN 'SIGNED_16BITS' THEN '16BSI' "
        + "WHEN 'UNSIGNED_16BITS' THEN '16BUI' WHEN 'SIGNED_32BITS' THEN '32BSI' "
        + "WHEN 'REAL_32BITS' THEN '32BF' WHEN 'REAL_64BITS' THEN '64BF' END";

    /** The Sedona raster of the MEOS raster of column r. */
    private static final String TO_SEDONA = "RS_AddBandFromArray(RS_MakeEmptyRaster(1, "
        + SEDONA_TYPE + ", rasterWidth(r), rasterHeight(r), upperLeftX(r), upperLeftY(r), "
        + "scaleX(r), scaleY(r), skewX(r), skewY(r), SRID(r)), flatten(dumpValues(r, 1, false)), 1, "
        + "rasterBandNoDataValue(r, 1))";

    /** The georeferencing, the pixel type and the nodata value of a Sedona raster s. */
    private static final String GRID = "w, h, ulx, uly, sx, sy, kx, ky, srid, pixtype";

    /**
     * The query answering the MEOS raster r of each Sedona raster s of the relation {@code from}
     * with its identifier id: the pixels are the bytes of the values of the band in their order,
     * each value written as the pixel of a band of one pixel of the pixel type ({@code addBand}
     * over {@code makeEmptyRaster}), and the raster is built from them, the georeferencing and
     * the nodata value, without one where the Sedona band has none.
     */
    private static String toMeos(String from) {
        return "SELECT id, CASE WHEN nodata IS NULL THEN raster(pixels, " + GRID + ") "
            + "ELSE raster(pixels, " + GRID + ", nodata) END AS r FROM (SELECT id, " + GRID
            + ", nodata, aggregate(transform(sort_array(collect_list(struct(pos, bytes))), "
            + "p -> p.bytes), X'', (acc, b) -> concat(acc, b)) AS pixels FROM (SELECT id, " + GRID
            + ", nodata, pos, rasterPixels(addBand(makeEmptyRaster(1, 1, 0.0, 0.0, 1.0, -1.0, 0.0, "
            + "0.0, 0), 1, pixtype, v), 1) AS bytes FROM (SELECT id, RS_Width(s) AS w, "
            + "RS_Height(s) AS h, RS_UpperLeftX(s) AS ulx, RS_UpperLeftY(s) AS uly, "
            + "RS_ScaleX(s) AS sx, RS_ScaleY(s) AS sy, RS_SkewX(s) AS kx, RS_SkewY(s) AS ky, "
            + "RS_SRID(s) AS srid, " + MEOS_TYPE + " AS pixtype, RS_BandNoDataValue(s, 1) AS nodata, "
            + "posexplode(RS_BandAsArray(s, 1)) AS (pos, v) FROM " + from + ")) GROUP BY id, " + GRID
            + ", nodata)";
    }

    /**
     * Rasters of 3x2 pixels from (10, 20), pixels of 2x-1 skewed by 0.5 and 0.25 in SRID 3857,
     * one per pixel type Sedona carries, with and without a nodata value: its pixels row by row
     * and little-endian, its pixel type and its nodata value.
     */
    private static final String[][] RASTERS = {
        // 1.5, nodata, 0.1f / 4, 5, 6
        {"0000C03F003C1CC6CDCCCC3D000080400000A0400000C040", "32BF", "-9999.0"},
        {"0000C03F003C1CC6CDCCCC3D000080400000A0400000C040", "32BF", null},
        // 1, 2, 255 / 0, 254, 7
        {"0102FF00FE07", "8BUI", "0.0"},
        {"0102FF00FE07", "8BUI", null},
        // 1, -2, 300 / -32768, nodata, 7
        {"0100FEFF2C010080FFFF0700", "16BSI", "-1.0"},
        // 0, 65535, 1 / 2, 3, 4
        {"0000FFFF0100020003000400", "16BUI", null},
        // -2147483648, 2147483647, nodata / 1, 2, 3
        {"00000080FFFFFF7F00000000010000000200000003000000", "32BSI", "0.0"},
        // 0.1, 1, 2 / 0.5, -1, 3
        {"9A9999999999B93F000000000000F03F0000000000000040000000000000E03F000000000000F0BF"
            + "0000000000000840", "64BF", null},
    };

    @BeforeAll
    static void init() {
        spark = SparkSession.builder().appName("sedona-raster-bridge").master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
        Set<String> builtin = names(spark.newSession());
        sedonaNames = names(SedonaContext.create(spark.newSession()));
        sedonaNames.removeAll(builtin);
        MobilitySparkSql.registerAll(spark);
        mobilityNames = names(spark);
        mobilityNames.removeAll(builtin);
        spark = SedonaContext.create(spark);
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static Set<String> names(SparkSession session) {
        Set<String> names = new TreeSet<>();
        for (Function f : session.catalog().listFunctions().collectAsList()) {
            names.add(f.name().toLowerCase());
        }
        return names;
    }

    private static String meos(String[] raster) {
        return "raster(X'" + raster[0] + "', 3, 2, 10.0, 20.0, 2.0, -1.0, 0.5, 0.25, 3857, '"
            + raster[1] + "'" + (raster[2] == null ? "" : ", " + raster[2]) + ")";
    }

    private static Row row(String sql) {
        List<Row> rows = spark.sql(sql).collectAsList();
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    @Test
    void theSessionCarriesBothRastersUnderNamesTheyNeverShare() {
        Set<String> shared = new TreeSet<>(sedonaNames);
        shared.retainAll(mobilityNames);
        assertEquals(Set.of(), shared);
        assertTrue(sedonaNames.contains("rs_makeemptyraster"));
        assertTrue(mobilityNames.contains("makeemptyraster"));
        assertTrue(mobilityNames.stream().noneMatch(n -> n.startsWith("rs_") || n.startsWith("st_")));
    }

    @Test
    void aSedonaRasterStatesTheGridAndTheValuesOfTheMeosRasterItIsBuiltFrom() {
        for (String[] raster : RASTERS) {
            Row r = row("SELECT RS_Width(s), RS_Height(s), RS_UpperLeftX(s), RS_UpperLeftY(s), "
                + "RS_ScaleX(s), RS_ScaleY(s), RS_SkewX(s), RS_SkewY(s), RS_SRID(s), RS_NumBands(s), "
                + "RS_BandNoDataValue(s, 1), RS_BandAsArray(s, 1), flatten(dumpValues(r, 1, false)) "
                + "FROM (SELECT r, " + TO_SEDONA + " AS s FROM (SELECT " + meos(raster) + " AS r))");
            assertEquals(List.of(3, 2, 10.0, 20.0, 2.0, -1.0, 0.5, 0.25, 3857, 1),
                List.of(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6),
                    r.get(7), r.get(8), r.get(9)), raster[1]);
            assertEquals(raster[2] == null ? null : Double.valueOf(raster[2]), r.get(10), raster[1]);
            assertEquals(r.getList(12), r.getList(11), raster[1]);
        }
    }

    @Test
    void aMeosRasterIsTheRasterItsSedonaRasterGivesBack() {
        for (String[] raster : RASTERS) {
            String sedona = "(SELECT 1 AS id, " + TO_SEDONA + " AS s FROM (SELECT " + meos(raster)
                + " AS r))";
            Row r = row("SELECT rasterAsHexWKB(r, 'NDR'), rasterAsHexWKB(" + meos(raster)
                + ", 'NDR') FROM (" + toMeos(sedona) + ")");
            assertEquals(r.get(1), r.get(0), raster[1] + " nodata " + raster[2]);
        }
    }

    @Test
    void aSedonaRasterIsTheRasterItsMeosRasterGivesBack() {
        String[][] sedonas = {
            {"F", "array(1.5d, -9999d, 0.25d, 4d, 5d, 6d)", "-9999d"},
            {"F", "array(1.5d, -9999d, 0.25d, 4d, 5d, 6d)", "CAST(NULL AS DOUBLE)"},
            {"B", "array(1d, 2d, 255d, 0d, 254d, 7d)", "0d"},
            {"B", "array(1d, 2d, 255d, 0d, 254d, 7d)", "CAST(NULL AS DOUBLE)"},
            {"S", "array(1d, -2d, 300d, -32768d, -1d, 7d)", "-1d"},
            {"US", "array(0d, 65535d, 1d, 2d, 3d, 4d)", "CAST(NULL AS DOUBLE)"},
            {"I", "array(-2147483648d, 2147483647d, 0d, 1d, 2d, 3d)", "0d"},
            {"D", "array(0.1d, 1d, 2d, 0.5d, -1d, 3d)", "CAST(NULL AS DOUBLE)"},
        };
        for (String[] sedona : sedonas) {
            String s = "RS_AddBandFromArray(RS_MakeEmptyRaster(1, '" + sedona[0] + "', 3, 2, 10.0, "
                + "20.0, 2.0, -1.0, 0.5, 0.25, 3857), " + sedona[1] + ", 1, " + sedona[2] + ")";
            String from = "(SELECT 1 AS id, " + s + " AS s)";
            String back = "(SELECT id, " + TO_SEDONA + " AS t FROM (" + toMeos(from) + "))";
            Row r = row("SELECT RS_MetaData(s), RS_MetaData(t), RS_BandPixelType(s, 1), "
                + "RS_BandPixelType(t, 1), RS_BandNoDataValue(s, 1), RS_BandNoDataValue(t, 1), "
                + "RS_BandAsArray(s, 1), RS_BandAsArray(t, 1) FROM " + from + " JOIN " + back
                + " USING (id)");
            for (int i = 0; i < 8; i += 2) {
                assertEquals(r.get(i), r.get(i + 1), sedona[0] + " " + sedona[2] + " column " + i);
            }
        }
    }
}
