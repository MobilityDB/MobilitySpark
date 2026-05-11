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

import functions.functions;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF13;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for affine transformations on tgeo: translate, rotate
 * (2D), rotateX/Y/Z (3D), transscale.
 *
 * MobilityDB SQL composes these from MEOS's single tgeo_affine(Temporal*,
 * AFFINE*) primitive. We build the AFFINE struct in direct memory and call
 * tgeo_affine
 *
 * AFFINE layout (96 bytes, all doubles):
 *   afac bfac cfac    — row 0 spatial coefficients
 *   dfac efac ffac    — row 1
 *   gfac hfac ifac    — row 2
 *   xoff yoff zoff    — translation offsets
 *
 * Identity matrix: afac=efac=ifac=1, all other coefficients 0.
 */
public final class GeoAffineUDFs {

    private GeoAffineUDFs() {}

    private static Pointer makeAffine(double afac, double bfac, double cfac,
            double dfac, double efac, double ffac,
            double gfac, double hfac, double ifac,
            double xoff, double yoff, double zoff) {
        Pointer aff = Runtime.getSystemRuntime().getMemoryManager().allocateDirect(96);
        aff.putDouble(0,   afac);
        aff.putDouble(8,   bfac);
        aff.putDouble(16,  cfac);
        aff.putDouble(24,  dfac);
        aff.putDouble(32,  efac);
        aff.putDouble(40,  ffac);
        aff.putDouble(48,  gfac);
        aff.putDouble(56,  hfac);
        aff.putDouble(64,  ifac);
        aff.putDouble(72,  xoff);
        aff.putDouble(80,  yoff);
        aff.putDouble(88,  zoff);
        return aff;
    }

    private static String applyAffine(String hex, Pointer affine) {
        Pointer t = functions.temporal_from_hexwkb(hex);
        if (t == null) return null;
        try {
            Pointer r = functions.tgeo_affine(t, affine);
            if (r == null) return null;
            try { return functions.temporal_as_hexwkb(r, (byte) 0); }
            finally { MeosMemory.free(r); }
        } finally { MeosMemory.free(t); }
    }

    // translate(temporal, dx, dy) — 2D translation (z unchanged)
    public static final UDF3<String, Double, Double, String> translate2 =
        (hex, dx, dy) -> {
            if (hex == null || dx == null || dy == null) return null;
            MeosThread.ensureReady();
            return applyAffine(hex, makeAffine(1,0,0, 0,1,0, 0,0,1, dx, dy, 0));
        };

    // translate(temporal, dx, dy, dz) — 3D translation
    public static final UDF4<String, Double, Double, Double, String> translate3 =
        (hex, dx, dy, dz) -> {
            if (hex == null || dx == null || dy == null || dz == null) return null;
            MeosThread.ensureReady();
            return applyAffine(hex, makeAffine(1,0,0, 0,1,0, 0,0,1, dx, dy, dz));
        };

    // rotate(temporal, angleRadians) — 2D rotation around origin (z unchanged)
    public static final UDF2<String, Double, String> rotate =
        (hex, angle) -> {
            if (hex == null || angle == null) return null;
            MeosThread.ensureReady();
            double c = Math.cos(angle), s = Math.sin(angle);
            return applyAffine(hex, makeAffine(c,-s,0, s,c,0, 0,0,1, 0, 0, 0));
        };

    // rotateX(temporal, angleRadians) — 3D rotation about x-axis
    public static final UDF2<String, Double, String> rotateX =
        (hex, angle) -> {
            if (hex == null || angle == null) return null;
            MeosThread.ensureReady();
            double c = Math.cos(angle), s = Math.sin(angle);
            return applyAffine(hex, makeAffine(1,0,0, 0,c,-s, 0,s,c, 0, 0, 0));
        };

    // rotateY(temporal, angleRadians) — 3D rotation about y-axis
    public static final UDF2<String, Double, String> rotateY =
        (hex, angle) -> {
            if (hex == null || angle == null) return null;
            MeosThread.ensureReady();
            double c = Math.cos(angle), s = Math.sin(angle);
            return applyAffine(hex, makeAffine(c,0,s, 0,1,0, -s,0,c, 0, 0, 0));
        };

    // rotateZ(temporal, angleRadians) — 3D rotation about z-axis
    public static final UDF2<String, Double, String> rotateZ =
        (hex, angle) -> {
            if (hex == null || angle == null) return null;
            MeosThread.ensureReady();
            double c = Math.cos(angle), s = Math.sin(angle);
            return applyAffine(hex, makeAffine(c,-s,0, s,c,0, 0,0,1, 0, 0, 0));
        };

    // transscale(temporal, dx, dy, sx, sy) — translate then scale
    // Equivalent affine: ((px+dx)*sx, (py+dy)*sy) = sx*px + sx*dx, sy*py + sy*dy
    public static final UDF5<String, Double, Double, Double, Double, String> transscale =
        (hex, dx, dy, sx, sy) -> {
            if (hex == null || dx == null || dy == null || sx == null || sy == null) return null;
            MeosThread.ensureReady();
            return applyAffine(hex, makeAffine(sx,0,0, 0,sy,0, 0,0,1, sx*dx, sy*dy, 0));
        };

    // affine(temporal, afac, bfac, cfac, dfac, efac, ffac, gfac, hfac, ifac, xoff, yoff, zoff)
    public static final UDF13<String, Double, Double, Double, Double, Double, Double,
                              Double, Double, Double, Double, Double, Double, String> affine =
        (hex, afac, bfac, cfac, dfac, efac, ffac, gfac, hfac, ifac, xoff, yoff, zoff) -> {
            if (hex == null || afac == null || bfac == null || cfac == null || dfac == null
                || efac == null || ffac == null || gfac == null || hfac == null || ifac == null
                || xoff == null || yoff == null || zoff == null) return null;
            MeosThread.ensureReady();
            return applyAffine(hex, makeAffine(afac, bfac, cfac, dfac, efac, ffac,
                gfac, hfac, ifac, xoff, yoff, zoff));
        };

    public static void registerAll(SparkSession spark) {
        spark.udf().register("translate",  translate2, DataTypes.StringType);
        spark.udf().register("translate3", translate3, DataTypes.StringType);
        spark.udf().register("rotate",     rotate,     DataTypes.StringType);
        spark.udf().register("rotateX",    rotateX,    DataTypes.StringType);
        spark.udf().register("rotateY",    rotateY,    DataTypes.StringType);
        spark.udf().register("rotateZ",    rotateZ,    DataTypes.StringType);
        spark.udf().register("transscale", transscale, DataTypes.StringType);
        spark.udf().register("affine",     affine,     DataTypes.StringType);
    }
}
