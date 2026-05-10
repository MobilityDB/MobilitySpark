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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;

/**
 * Spark SQL UDFs for static (non-temporal) geometry operations.
 *
 * All inputs and WKT/WKB outputs use WKT string encoding via geo_from_text /
 * geo_as_text.  Scalar outputs (Double, Boolean) are returned as Java primitives.
 *
 * Memory management: every Pointer returned by MEOS must be freed via
 * MeosMemory.free() in a finally block.
 *
 * MEOS function authority: meos/include/meos_geo.h
 */
public final class StaticGeoUDFs {

    private StaticGeoUDFs() {}

    // ------------------------------------------------------------------
    // Geometry predicates  (WKT × WKT → Boolean)
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Boolean> geomContains =
        (wkt1, wkt2) -> {
            if (wkt1 == null || wkt2 == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(wkt1, 0);
            if (g1 == null) return null;
            try {
                Pointer g2 = functions.geo_from_text(wkt2, 0);
                if (g2 == null) return null;
                try {
                    return functions.geom_contains(g1, g2);
                } finally {
                    MeosMemory.free(g2);
                }
            } finally {
                MeosMemory.free(g1);
            }
        };

    public static final UDF2<String, String, Boolean> geomCovers =
        (wkt1, wkt2) -> {
            if (wkt1 == null || wkt2 == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(wkt1, 0);
            if (g1 == null) return null;
            try {
                Pointer g2 = functions.geo_from_text(wkt2, 0);
                if (g2 == null) return null;
                try {
                    return functions.geom_covers(g1, g2);
                } finally {
                    MeosMemory.free(g2);
                }
            } finally {
                MeosMemory.free(g1);
            }
        };

    public static final UDF2<String, String, Boolean> geomDisjoint =
        (wkt1, wkt2) -> {
            if (wkt1 == null || wkt2 == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(wkt1, 0);
            if (g1 == null) return null;
            try {
                Pointer g2 = functions.geo_from_text(wkt2, 0);
                if (g2 == null) return null;
                try {
                    return functions.geom_disjoint2d(g1, g2);
                } finally {
                    MeosMemory.free(g2);
                }
            } finally {
                MeosMemory.free(g1);
            }
        };

    public static final UDF2<String, String, Boolean> geomIntersects =
        (wkt1, wkt2) -> {
            if (wkt1 == null || wkt2 == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(wkt1, 0);
            if (g1 == null) return null;
            try {
                Pointer g2 = functions.geo_from_text(wkt2, 0);
                if (g2 == null) return null;
                try {
                    return functions.geom_intersects2d(g1, g2);
                } finally {
                    MeosMemory.free(g2);
                }
            } finally {
                MeosMemory.free(g1);
            }
        };

    public static final UDF2<String, String, Boolean> geomTouches =
        (wkt1, wkt2) -> {
            if (wkt1 == null || wkt2 == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(wkt1, 0);
            if (g1 == null) return null;
            try {
                Pointer g2 = functions.geo_from_text(wkt2, 0);
                if (g2 == null) return null;
                try {
                    return functions.geom_touches(g1, g2);
                } finally {
                    MeosMemory.free(g2);
                }
            } finally {
                MeosMemory.free(g1);
            }
        };

    public static final UDF3<String, String, Number, Boolean> geomDwithin =
        (wkt1, wkt2, dist) -> {
            if (wkt1 == null || wkt2 == null || dist == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(wkt1, 0);
            if (g1 == null) return null;
            try {
                Pointer g2 = functions.geo_from_text(wkt2, 0);
                if (g2 == null) return null;
                try {
                    return functions.geom_dwithin2d(g1, g2, dist.doubleValue());
                } finally {
                    MeosMemory.free(g2);
                }
            } finally {
                MeosMemory.free(g1);
            }
        };

    // ------------------------------------------------------------------
    // Geometry metrics  (WKT → Double)
    // ------------------------------------------------------------------

    public static final UDF2<String, String, Double> geomDistance =
        (wkt1, wkt2) -> {
            if (wkt1 == null || wkt2 == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(wkt1, 0);
            if (g1 == null) return null;
            try {
                Pointer g2 = functions.geo_from_text(wkt2, 0);
                if (g2 == null) return null;
                try {
                    return functions.geom_distance2d(g1, g2);
                } finally {
                    MeosMemory.free(g2);
                }
            } finally {
                MeosMemory.free(g1);
            }
        };

    public static final UDF1<String, Double> geomLength =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(wkt, 0);
            if (g == null) return null;
            try {
                return functions.geom_length(g);
            } finally {
                MeosMemory.free(g);
            }
        };

    public static final UDF1<String, Double> geomPerimeter =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(wkt, 0);
            if (g == null) return null;
            try {
                return functions.geom_perimeter(g);
            } finally {
                MeosMemory.free(g);
            }
        };

    // ------------------------------------------------------------------
    // Geometry transforms  (WKT → WKT)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> geomCentroid =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(wkt, 0);
            if (g == null) return null;
            try {
                Pointer r = functions.geom_centroid(g);
                if (r == null) return null;
                try {
                    return functions.geo_as_text(r, 15);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(g);
            }
        };

    public static final UDF1<String, String> geomBoundary =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(wkt, 0);
            if (g == null) return null;
            try {
                Pointer r = functions.geom_boundary(g);
                if (r == null) return null;
                try {
                    return functions.geo_as_text(r, 15);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(g);
            }
        };

    public static final UDF2<String, String, String> geomDifference =
        (wkt1, wkt2) -> {
            if (wkt1 == null || wkt2 == null) return null;
            MeosThread.ensureReady();
            Pointer g1 = functions.geo_from_text(wkt1, 0);
            if (g1 == null) return null;
            try {
                Pointer g2 = functions.geo_from_text(wkt2, 0);
                if (g2 == null) return null;
                try {
                    Pointer r = functions.geom_difference2d(g1, g2);
                    if (r == null) return null;
                    try {
                        return functions.geo_as_text(r, 15);
                    } finally {
                        MeosMemory.free(r);
                    }
                } finally {
                    MeosMemory.free(g2);
                }
            } finally {
                MeosMemory.free(g1);
            }
        };

    public static final UDF2<String, Double, String> geomUnaryUnion =
        (wkt, prec) -> {
            if (wkt == null || prec == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(wkt, 0);
            if (g == null) return null;
            try {
                Pointer r = functions.geom_unary_union(g, prec);
                if (r == null) return null;
                try {
                    return functions.geo_as_text(r, 15);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(g);
            }
        };

    public static final UDF1<String, String> geoReverse =
        (wkt) -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(wkt, 0);
            if (g == null) return null;
            try {
                Pointer r = functions.geo_reverse(g);
                if (r == null) return null;
                try {
                    return functions.geo_as_text(r, 15);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(g);
            }
        };

    public static final UDF2<String, Integer, String> geoRound =
        (wkt, maxdd) -> {
            if (wkt == null || maxdd == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(wkt, 0);
            if (g == null) return null;
            try {
                Pointer r = functions.geo_round(g, maxdd);
                if (r == null) return null;
                try {
                    return functions.geo_as_text(r, maxdd);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(g);
            }
        };

    // ------------------------------------------------------------------
    // Line functions  (WKT → WKT)
    // ------------------------------------------------------------------

    public static final UDF2<String, Double, String> lineInterpolatePoint =
        (wkt, fraction) -> {
            if (wkt == null || fraction == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(wkt, 0);
            if (g == null) return null;
            try {
                Pointer r = functions.line_interpolate_point(g, fraction, false);
                if (r == null) return null;
                try {
                    return functions.geo_as_text(r, 15);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(g);
            }
        };

    public static final UDF3<String, Double, Double, String> lineSubstring =
        (wkt, from, to) -> {
            if (wkt == null || from == null || to == null) return null;
            MeosThread.ensureReady();
            Pointer g = functions.geo_from_text(wkt, 0);
            if (g == null) return null;
            try {
                Pointer r = functions.line_substring(g, from, to);
                if (r == null) return null;
                try {
                    return functions.geo_as_text(r, 15);
                } finally {
                    MeosMemory.free(r);
                }
            } finally {
                MeosMemory.free(g);
            }
        };

    // ------------------------------------------------------------------
    // Registration
    // ------------------------------------------------------------------

    public static void registerAll(SparkSession spark) {
        spark.udf().register("geomContains",        geomContains,        DataTypes.BooleanType);
        spark.udf().register("geomCovers",          geomCovers,          DataTypes.BooleanType);
        spark.udf().register("geomDisjoint",        geomDisjoint,        DataTypes.BooleanType);
        spark.udf().register("geomIntersects",      geomIntersects,      DataTypes.BooleanType);
        spark.udf().register("geomTouches",         geomTouches,         DataTypes.BooleanType);
        spark.udf().register("geomDwithin",         geomDwithin,         DataTypes.BooleanType);
        spark.udf().register("geomDistance",        geomDistance,        DataTypes.DoubleType);
        spark.udf().register("geomLength",          geomLength,          DataTypes.DoubleType);
        spark.udf().register("geomPerimeter",       geomPerimeter,       DataTypes.DoubleType);
        spark.udf().register("geomCentroid",        geomCentroid,        DataTypes.StringType);
        spark.udf().register("geomBoundary",        geomBoundary,        DataTypes.StringType);
        spark.udf().register("geomDifference",      geomDifference,      DataTypes.StringType);
        spark.udf().register("geomUnaryUnion",      geomUnaryUnion,      DataTypes.StringType);
        spark.udf().register("geoReverse",          geoReverse,          DataTypes.StringType);
        spark.udf().register("geoRound",            geoRound,            DataTypes.StringType);
        spark.udf().register("lineInterpolatePoint", lineInterpolatePoint, DataTypes.StringType);
        spark.udf().register("lineSubstring",       lineSubstring,       DataTypes.StringType);
    }
}
