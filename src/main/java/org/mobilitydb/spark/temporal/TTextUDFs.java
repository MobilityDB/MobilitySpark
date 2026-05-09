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

import functions.functions;
import jnr.ffi.Pointer;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark SQL UDFs for ttext case-conversion operations.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class TTextUDFs {

    private TTextUDFs() {}

    // ttext_upper(ttext hex-WKB) → ttext hex-WKB
    public static final UDF1<String, String> ttextUpper =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            Pointer r = functions.ttext_upper(p);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ttext_lower(ttext hex-WKB) → ttext hex-WKB
    public static final UDF1<String, String> ttextLower =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            Pointer r = functions.ttext_lower(p);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ttext_initcap(ttext hex-WKB) → ttext hex-WKB
    public static final UDF1<String, String> ttextInitcap =
        (hex) -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            Pointer r = functions.ttext_initcap(p);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static void registerAll(SparkSession spark) {
        spark.udf().register("ttextUpper",   ttextUpper,   DataTypes.StringType);
        spark.udf().register("ttextLower",   ttextLower,   DataTypes.StringType);
        spark.udf().register("ttextInitcap", ttextInitcap, DataTypes.StringType);
    }
}
