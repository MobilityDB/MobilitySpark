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

package org.mobilitydb.spark.portable;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.mobilitydb.spark.geo.DistanceUDFs;
import org.mobilitydb.spark.temporal.PosOpsUDFs;
import org.mobilitydb.spark.temporal.TemporalBoxOpsUDFs;
import org.mobilitydb.spark.temporal.TemporalCompUDFs;
import org.mobilitydb.spark.temporal.PredicateUDFs;

/**
 * Portable bare-name operator aliases — the cross-engine SQL dialect.
 *
 * <p>Single source of truth: {@code MobilityDB/MEOS-API}
 * {@code meta/portable-aliases.json#/families} (RFC #920, discussion
 * MobilityDB#861, native in MobilityDB#1075). That contract maps 29 SQL
 * operator symbols to 29 portable bare function names, type-agnostically,
 * so a user learns one reference and assumes every engine behaves
 * identically. Spark SQL has no infix-operator extension API, so every
 * operator is exposed as its portable <em>bare</em> named function — and
 * the bare name, not a type-qualified spelling, is the portable contract.
 *
 * <p><b>Equivalence by construction.</b> Every alias here reuses the
 * operator's own existing backing UDF field verbatim (the exact same
 * {@code GeneratedFunctions.*} MEOS C symbol the typed UDF dispatches to). No
 * operator logic is reimplemented; the alias cannot drift from the
 * operator because it <em>is</em> the operator's backing.
 *
 * <p><b>Six families, no headline exclusion.</b> The chosen backings are
 * the MEOS superclass entrypoints — {@code *_temporal_temporal} (time /
 * topology / temporal-comparison), {@code *_tspatial_tspatial} (spatial
 * position), {@code tdistance_tgeo_tgeo}, {@code nad_tgeo_*}. libmeos
 * dispatches these internally for <em>any</em> temporal value carried in
 * the type-erased hex-WKB string, so the four spatial sibling families
 * {@code tcbuffer} / {@code tnpoint} / {@code tpose} / {@code trgeometry}
 * are covered by construction alongside {@code temporal} and {@code geo}
 * — none is excluded.
 *
 * <p>{@code left} / {@code right} / {@code overleft} / {@code overright}
 * are the only operators whose MEOS symbol differs by argument
 * <em>class</em> (the {@code tnumber} value-axis vs. the {@code tspatial}
 * X-axis are distinct C operators). A thin runtime classifier
 * <em>selects</em> between the two existing backing fields; it contains no
 * operator logic, so equivalence by construction still holds.
 *
 * <p>Parity is gated by {@code scripts/portable_parity.py} (the same
 * prefix logic as {@code MobilityDB/MEOS-API portable_parity.py}):
 * 29/29 bare names registered, 0 unbacked.
 */
public final class PortableOperatorAliasUDFs {

    private PortableOperatorAliasUDFs() {}

    /**
     * True iff the hex-WKB value is a {@code tnumber} (tint/tfloat): only
     * those have a TBox value extent. Under MEOS's no-exit error handler a
     * non-tnumber temporal yields a null TBox. Used solely to select which
     * existing backing field to delegate to — never to compute a result.
     */
    private static boolean isTnumber(String hex) {
        Pointer p = GeneratedFunctions.temporal_from_hexwkb(hex);
        if (p == null) return false;
        try {
            Pointer box = GeneratedFunctions.tnumber_to_tbox(p);
            if (box == null) return false;
            MeosMemory.free(box);
            return true;
        } finally {
            MeosMemory.free(p);
        }
    }

    /**
     * Bare value/space-axis operator: delegate to the {@code tnumber}
     * backing for a tnumber left argument, otherwise to the
     * {@code tspatial} backing. Both delegates are the operator's own
     * existing backing fields.
     */
    private static UDF2<String, String, Boolean> axis(
            UDF2<String, String, Boolean> tnumber,
            UDF2<String, String, Boolean> tspatial) {
        return (s1, s2) -> {
            if (s1 == null || s2 == null) return null;
            MeosThread.ensureReady();
            return isTnumber(s1) ? tnumber.call(s1, s2) : tspatial.call(s1, s2);
        };
    }

    public static void registerAll(SparkSession spark) {
        // ── topology (&&, @>, <@, -|-) ── superclass *_temporal_temporal,
        // all six families ────────────────────────────────────────────
        spark.udf().register("overlaps",   TemporalBoxOpsUDFs.temporalOverlapsTemporal,  DataTypes.BooleanType);
        spark.udf().register("contains",   TemporalBoxOpsUDFs.temporalContainsTemporal,  DataTypes.BooleanType);
        spark.udf().register("contained",  TemporalBoxOpsUDFs.temporalContainedTemporal, DataTypes.BooleanType);
        spark.udf().register("adjacent",   TemporalBoxOpsUDFs.temporalAdjacentTemporal,  DataTypes.BooleanType);

        // ── same (~=) ─────────────────────────────────────────────────
        spark.udf().register("same",       TemporalBoxOpsUDFs.temporalSameTemporal,      DataTypes.BooleanType);

        // ── time position (<<#, #>>, &<#, #&>) ── superclass, all six ──
        spark.udf().register("before",     PosOpsUDFs.temporalBefore,     DataTypes.BooleanType);
        spark.udf().register("after",      PosOpsUDFs.temporalAfter,      DataTypes.BooleanType);
        spark.udf().register("overbefore", PosOpsUDFs.temporalOverbefore, DataTypes.BooleanType);
        spark.udf().register("overafter",  PosOpsUDFs.temporalOverafter,  DataTypes.BooleanType);

        // ── space X (<<, >>, &<, &>) ── tnumber value-axis OR tspatial ─
        spark.udf().register("left",      axis(PosOpsUDFs.tnumberLeft,      PosOpsUDFs.tpointLeft),      DataTypes.BooleanType);
        spark.udf().register("right",     axis(PosOpsUDFs.tnumberRight,     PosOpsUDFs.tpointRight),     DataTypes.BooleanType);
        spark.udf().register("overleft",  axis(PosOpsUDFs.tnumberOverleft,  PosOpsUDFs.tpointOverleft),  DataTypes.BooleanType);
        spark.udf().register("overright", axis(PosOpsUDFs.tnumberOverright, PosOpsUDFs.tpointOverright), DataTypes.BooleanType);

        // ── space Y (<<|, |>>, &<|, |&>) ── tspatial, all spatial fams ─
        spark.udf().register("below",      PosOpsUDFs.tpointBelow,     DataTypes.BooleanType);
        spark.udf().register("above",      PosOpsUDFs.tpointAbove,     DataTypes.BooleanType);
        spark.udf().register("overbelow",  PosOpsUDFs.tpointOverbelow, DataTypes.BooleanType);
        spark.udf().register("overabove",  PosOpsUDFs.tpointOverabove, DataTypes.BooleanType);

        // ── space Z / 3D (<</, />>, &</, /&>) ── tspatial ──────────────
        spark.udf().register("front",      PosOpsUDFs.tpointFront,     DataTypes.BooleanType);
        spark.udf().register("back",       PosOpsUDFs.tpointBack,      DataTypes.BooleanType);
        spark.udf().register("overfront",  PosOpsUDFs.tpointOverfront, DataTypes.BooleanType);
        spark.udf().register("overback",   PosOpsUDFs.tpointOverback,  DataTypes.BooleanType);

        // ── temporal comparison (#=, #<>, #<, #<=, #>, #>=) ──
        // superclass t*_temporal_temporal → temporal tbool (hex-WKB) ──
        spark.udf().register("tempEq",     TemporalCompUDFs.teqTemporal, DataTypes.StringType);
        spark.udf().register("tempNe",     TemporalCompUDFs.tneTemporal, DataTypes.StringType);
        spark.udf().register("tempLt",     TemporalCompUDFs.tltTemporal, DataTypes.StringType);
        spark.udf().register("tempLe",     TemporalCompUDFs.tleTemporal, DataTypes.StringType);
        spark.udf().register("tempGt",     TemporalCompUDFs.tgtTemporal, DataTypes.StringType);
        spark.udf().register("tempGe",     TemporalCompUDFs.tgeTemporal, DataTypes.StringType);

        // ── ever comparison (?=, ?<>, ?<, ?<=, ?>, ?>=) ──
        // ever_*_temporal_temporal → scalar boolean ──
        spark.udf().register("everEq",     PredicateUDFs.everEqTemporal, DataTypes.BooleanType);
        spark.udf().register("everNe",     PredicateUDFs.everNeTemporal, DataTypes.BooleanType);
        spark.udf().register("everLt",     PredicateUDFs.everLtTemporal, DataTypes.BooleanType);
        spark.udf().register("everLe",     PredicateUDFs.everLeTemporal, DataTypes.BooleanType);
        spark.udf().register("everGt",     PredicateUDFs.everGtTemporal, DataTypes.BooleanType);
        spark.udf().register("everGe",     PredicateUDFs.everGeTemporal, DataTypes.BooleanType);

        // ── always comparison (%=, %<>, %<, %<=, %>, %>=) ──
        // always_*_temporal_temporal → scalar boolean ──
        spark.udf().register("alwaysEq",   PredicateUDFs.alwaysEqTemporal, DataTypes.BooleanType);
        spark.udf().register("alwaysNe",   PredicateUDFs.alwaysNeTemporal, DataTypes.BooleanType);
        spark.udf().register("alwaysLt",   PredicateUDFs.alwaysLtTemporal, DataTypes.BooleanType);
        spark.udf().register("alwaysLe",   PredicateUDFs.alwaysLeTemporal, DataTypes.BooleanType);
        spark.udf().register("alwaysGt",   PredicateUDFs.alwaysGtTemporal, DataTypes.BooleanType);
        spark.udf().register("alwaysGe",   PredicateUDFs.alwaysGeTemporal, DataTypes.BooleanType);

        // ── distance (<->, |=|) ───────────────────────────────────────
        spark.udf().register("tdistance",               DistanceUDFs.tdistanceTgeoTgeo, DataTypes.StringType);
        spark.udf().register("nearestApproachDistance", DistanceUDFs.nadTgeoGeo,        DataTypes.DoubleType);
    }
}
