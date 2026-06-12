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

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.function.BiFunction;

/**
 * Spark SQL UDFs for set × set positional, topological, and distance
 * operations. Set hex carries the element type so a single UDF dispatches
 * across all set types.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class SetOpsUDFs {

    private SetOpsUDFs() {}

    private static UDF2<String, String, Boolean> setSet(BiFunction<Pointer, Pointer, Boolean> fn) {
        return (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.set_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.set_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return fn.apply(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };
    }

    public static final UDF2<String, String, Boolean> setLeft       = setSet(GeneratedFunctions::left_set_set);
    public static final UDF2<String, String, Boolean> setRight      = setSet(GeneratedFunctions::right_set_set);
    public static final UDF2<String, String, Boolean> setOverleft   = setSet(GeneratedFunctions::overleft_set_set);
    public static final UDF2<String, String, Boolean> setOverright  = setSet(GeneratedFunctions::overright_set_set);
    public static final UDF2<String, String, Boolean> setContains   = setSet(GeneratedFunctions::contains_set_set);
    public static final UDF2<String, String, Boolean> setContained  = setSet(GeneratedFunctions::contained_set_set);
    public static final UDF2<String, String, Boolean> setOverlaps   = setSet(GeneratedFunctions::overlaps_set_set);

    // Per-type distance (set×set must be of matching element type).
    public static final UDF2<String, String, Integer> distanceIntsetIntset =
        (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.set_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.set_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return GeneratedFunctions.distance_intset_intset(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };

    public static final UDF2<String, String, Long> distanceBigintsetBigintset =
        (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.set_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.set_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return GeneratedFunctions.distance_bigintset_bigintset(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };

    public static final UDF2<String, String, Double> distanceFloatsetFloatset =
        (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.set_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.set_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return GeneratedFunctions.distance_floatset_floatset(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };

    public static final UDF2<String, String, Double> distanceTstzsetTstzset =
        (h1, h2) -> {
            if (h1 == null || h2 == null) return null;
            MeosThread.ensureReady();
            Pointer p1 = GeneratedFunctions.set_from_hexwkb(h1); if (p1 == null) return null;
            Pointer p2 = GeneratedFunctions.set_from_hexwkb(h2);
            if (p2 == null) { MeosMemory.free(p1); return null; }
            try { return GeneratedFunctions.distance_tstzset_tstzset(p1, p2); }
            finally { MeosMemory.free(p1, p2); }
        };

    public static void registerAll(SparkSession spark) {
        spark.udf().register("setLeft",      setLeft,      DataTypes.BooleanType);
        spark.udf().register("setRight",     setRight,     DataTypes.BooleanType);
        spark.udf().register("setOverleft",  setOverleft,  DataTypes.BooleanType);
        spark.udf().register("setOverright", setOverright, DataTypes.BooleanType);
        spark.udf().register("setContains",  setContains,  DataTypes.BooleanType);
        spark.udf().register("setContained", setContained, DataTypes.BooleanType);
        spark.udf().register("setOverlaps",  setOverlaps,  DataTypes.BooleanType);

        spark.udf().register("distanceIntsetIntset",       distanceIntsetIntset,       DataTypes.IntegerType);
        spark.udf().register("distanceBigintsetBigintset", distanceBigintsetBigintset, DataTypes.LongType);
        spark.udf().register("distanceFloatsetFloatset",   distanceFloatsetFloatset,   DataTypes.DoubleType);
        spark.udf().register("distanceTstzsetTstzset",     distanceTstzsetTstzset,     DataTypes.DoubleType);
        // MobilityDB SQL bare-name aliases
        spark.udf().register("setDistance", distanceFloatsetFloatset, DataTypes.DoubleType);
    }
}
