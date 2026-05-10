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
import org.mobilitydb.spark.MeosMemory;
import org.mobilitydb.spark.MeosThread;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.HexFormat;

/**
 * Spark SQL UDFs for typed alternative I/O constructors of set, span, and
 * spanset types. MobilityDB SQL exposes typed names like
 * `intsetFromHexWKB`, `floatspanFromHexWKB` etc. for type-safety in the
 * SQL layer; in MobilitySpark a single generic constructor suffices since
 * the WKB carries the element type, but we register typed aliases for
 * MobilityDB SQL parity.
 *
 * MEOS function authority: meos/include/meos.h — set_from_hexwkb,
 *   span_from_hexwkb, spanset_from_hexwkb (generic).
 */
public final class IOAliasUDFs {

    private IOAliasUDFs() {}

    // ------------------------------------------------------------------
    // Generic helpers — round-trip a hex-WKB through deserializer +
    // re-serializer to produce a normalised hex form (also validates it).
    // ------------------------------------------------------------------

    private static UDF1<String, String> setRoundtrip() {
        return hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    private static UDF1<String, String> spanRoundtrip() {
        return hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.span_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.span_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    private static UDF1<String, String> spansetRoundtrip() {
        return hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.spanset_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.spanset_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    private static UDF1<String, String> temporalRoundtrip() {
        return hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    private static UDF1<String, String> mfjsonToHex(java.util.function.Function<String, Pointer> ctor) {
        return mfjson -> {
            if (mfjson == null) return null;
            MeosThread.ensureReady();
            Pointer p = ctor.apply(mfjson);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    // ------------------------------------------------------------------
    // Set typed FromHexWKB
    // ------------------------------------------------------------------

    public static final UDF1<String, String> intsetFromHexWKB    = setRoundtrip();
    public static final UDF1<String, String> bigintsetFromHexWKB = setRoundtrip();
    public static final UDF1<String, String> floatsetFromHexWKB  = setRoundtrip();
    public static final UDF1<String, String> textsetFromHexWKB   = setRoundtrip();
    public static final UDF1<String, String> tstzsetFromHexWKB   = setRoundtrip();
    public static final UDF1<String, String> datesetFromHexWKB   = setRoundtrip();

    // ------------------------------------------------------------------
    // Span typed FromHexWKB
    // ------------------------------------------------------------------

    public static final UDF1<String, String> intspanFromHexWKB    = spanRoundtrip();
    public static final UDF1<String, String> bigintspanFromHexWKB = spanRoundtrip();
    public static final UDF1<String, String> floatspanFromHexWKB  = spanRoundtrip();
    public static final UDF1<String, String> tstzspanFromHexWKB   = spanRoundtrip();
    public static final UDF1<String, String> datespanFromHexWKB   = spanRoundtrip();

    // ------------------------------------------------------------------
    // Spanset typed FromHexWKB
    // ------------------------------------------------------------------

    public static final UDF1<String, String> intspansetFromHexWKB    = spansetRoundtrip();
    public static final UDF1<String, String> bigintspansetFromHexWKB = spansetRoundtrip();
    public static final UDF1<String, String> floatspansetFromHexWKB  = spansetRoundtrip();
    public static final UDF1<String, String> tstzspansetFromHexWKB   = spansetRoundtrip();
    public static final UDF1<String, String> datespansetFromHexWKB   = spansetRoundtrip();

    // ------------------------------------------------------------------
    // Temporal scalar typed FromHexWKB (tbool/tint/tfloat/ttext)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tboolFromHexWKB  = temporalRoundtrip();
    public static final UDF1<String, String> tintFromHexWKB   = temporalRoundtrip();
    public static final UDF1<String, String> tfloatFromHexWKB = temporalRoundtrip();
    public static final UDF1<String, String> ttextFromHexWKB  = temporalRoundtrip();

    // ------------------------------------------------------------------
    // Temporal-geo typed FromHexEWKB (tgeometry/tgeography)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tgeometryFromHexEWKB  = temporalRoundtrip();
    public static final UDF1<String, String> tgeographyFromHexEWKB = temporalRoundtrip();
    public static final UDF1<String, String> tgeompointFromHexEWKB = temporalRoundtrip();
    public static final UDF1<String, String> tgeogpointFromHexEWKB = temporalRoundtrip();

    // ------------------------------------------------------------------
    // Temporal-geo MFJSON constructors
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tgeometryFromMFJSON  = mfjsonToHex(org.mobilitydb.spark.MeosNative.INSTANCE::tgeometry_from_mfjson);
    public static final UDF1<String, String> tgeographyFromMFJSON = mfjsonToHex(org.mobilitydb.spark.MeosNative.INSTANCE::tgeography_from_mfjson);
    public static final UDF1<String, String> tgeompointFromMFJSON = mfjsonToHex(functions::tgeompoint_from_mfjson);
    public static final UDF1<String, String> tgeogpointFromMFJSON = mfjsonToHex(functions::tgeogpoint_from_mfjson);

    // ------------------------------------------------------------------
    // Temporal scalar text constructors (FromText / FromEWKT)
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tboolFromText  = mfjsonToHex(functions::tbool_in);
    public static final UDF1<String, String> tintFromText   = mfjsonToHex(functions::tint_in);
    public static final UDF1<String, String> tfloatFromText = mfjsonToHex(functions::tfloat_in);
    public static final UDF1<String, String> ttextFromText  = mfjsonToHex(functions::ttext_in);

    // Geo temporal text/EWKT constructors
    public static final UDF1<String, String> tgeompointFromText  = mfjsonToHex(functions::tgeompoint_in);
    public static final UDF1<String, String> tgeogpointFromText  = mfjsonToHex(functions::tgeogpoint_in);
    public static final UDF1<String, String> tgeompointFromEWKT  = mfjsonToHex(functions::tgeompoint_in);
    public static final UDF1<String, String> tgeogpointFromEWKT  = mfjsonToHex(functions::tgeogpoint_in);
    public static final UDF1<String, String> tgeometryFromText   = mfjsonToHex(org.mobilitydb.spark.MeosNative.INSTANCE::tgeometry_in);
    public static final UDF1<String, String> tgeographyFromText  = mfjsonToHex(org.mobilitydb.spark.MeosNative.INSTANCE::tgeography_in);
    public static final UDF1<String, String> tgeometryFromEWKT   = mfjsonToHex(org.mobilitydb.spark.MeosNative.INSTANCE::tgeometry_in);
    public static final UDF1<String, String> tgeographyFromEWKT  = mfjsonToHex(org.mobilitydb.spark.MeosNative.INSTANCE::tgeography_in);

    // ------------------------------------------------------------------
    // Binary I/O — bytes ↔ temporal via hex round-trip
    // (matches the SpanUDFs pattern: byte[] → hex → from_hexwkb → as_hexwkb)
    // ------------------------------------------------------------------

    private static UDF1<byte[], String> temporalFromBinary() {
        return bytes -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    public static final UDF1<byte[], String> tboolFromBinary       = temporalFromBinary();
    public static final UDF1<byte[], String> tintFromBinary        = temporalFromBinary();
    public static final UDF1<byte[], String> tfloatFromBinary      = temporalFromBinary();
    public static final UDF1<byte[], String> ttextFromBinary       = temporalFromBinary();
    public static final UDF1<byte[], String> tgeompointFromBinary  = temporalFromBinary();
    public static final UDF1<byte[], String> tgeogpointFromBinary  = temporalFromBinary();
    public static final UDF1<byte[], String> tgeometryFromBinary   = temporalFromBinary();
    public static final UDF1<byte[], String> tgeographyFromBinary  = temporalFromBinary();
    // EWKB and WKB go through the same generic constructor.
    public static final UDF1<byte[], String> tgeompointFromEWKB    = temporalFromBinary();
    public static final UDF1<byte[], String> tgeogpointFromEWKB    = temporalFromBinary();
    public static final UDF1<byte[], String> tgeometryFromEWKB     = temporalFromBinary();
    public static final UDF1<byte[], String> tgeographyFromEWKB    = temporalFromBinary();

    // asBinary / asEWKB: hex → byte[] inverse round-trip
    public static final UDF1<String, byte[]> asBinary =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            return HexFormat.of().parseHex(hex);
        };

    public static final UDF1<String, byte[]> asEWKB = asBinary;

    // asHexEWKB: re-emit hex with WKB_EXTENDED (variant 4) so SRID is preserved
    public static final UDF1<String, String> asHexEWKB =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.temporal_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.temporal_as_hexwkb(p, (byte) 4); }
            finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // Geoset typed I/O aliases (geomset / geogset)
    // ------------------------------------------------------------------

    private static UDF1<String, String> geomsetTextCtor() {
        return wkt -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.geomset_in(wkt);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    private static UDF1<String, String> geogsetTextCtor() {
        return wkt -> {
            if (wkt == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.geogset_in(wkt);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    private static UDF1<byte[], String> setFromBinary() {
        return bytes -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = functions.set_from_hexwkb(hex);
            if (p == null) return null;
            try { return functions.set_as_hexwkb(p, (byte) 0); }
            finally { MeosMemory.free(p); }
        };
    }

    public static final UDF1<String, String> geomsetFromText      = geomsetTextCtor();
    public static final UDF1<String, String> geogsetFromText      = geogsetTextCtor();
    public static final UDF1<String, String> geomsetFromEWKT      = geomsetTextCtor();
    public static final UDF1<String, String> geogsetFromEWKT      = geogsetTextCtor();
    public static final UDF1<String, String> geomsetFromHexWKB    = setRoundtrip();
    public static final UDF1<String, String> geogsetFromHexWKB    = setRoundtrip();
    public static final UDF1<byte[], String> geomsetFromBinary    = setFromBinary();
    public static final UDF1<byte[], String> geogsetFromBinary    = setFromBinary();
    public static final UDF1<byte[], String> geomsetFromEWKB      = setFromBinary();
    public static final UDF1<byte[], String> geogsetFromEWKB      = setFromBinary();

    // ------------------------------------------------------------------
    // TBox typed I/O aliases — generic tbox_from_hexwkb covers all.
    // ------------------------------------------------------------------

    public static final UDF1<String, String> tboxFromHexWKB =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.tbox_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer sizeOut = jnr.ffi.Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                return functions.tbox_as_hexwkb(p, (byte) 0, sizeOut);
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<byte[], String> tboxFromBinary =
        bytes -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = functions.tbox_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer sizeOut = jnr.ffi.Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                return functions.tbox_as_hexwkb(p, (byte) 0, sizeOut);
            } finally { MeosMemory.free(p); }
        };

    // ------------------------------------------------------------------
    // STBox typed I/O aliases
    // ------------------------------------------------------------------

    public static final UDF1<String, String> stboxFromHexWKB =
        hex -> {
            if (hex == null) return null;
            MeosThread.ensureReady();
            Pointer p = functions.stbox_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer sizeOut = jnr.ffi.Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                return functions.stbox_as_hexwkb(p, (byte) 0, sizeOut);
            } finally { MeosMemory.free(p); }
        };

    public static final UDF1<byte[], String> stboxFromBinary =
        bytes -> {
            if (bytes == null) return null;
            MeosThread.ensureReady();
            String hex = HexFormat.of().formatHex(bytes).toUpperCase();
            Pointer p = functions.stbox_from_hexwkb(hex);
            if (p == null) return null;
            try {
                Pointer sizeOut = jnr.ffi.Runtime.getSystemRuntime().getMemoryManager().allocateDirect(8);
                return functions.stbox_as_hexwkb(p, (byte) 0, sizeOut);
            } finally { MeosMemory.free(p); }
        };

    public static void registerAll(SparkSession spark) {
        // Set
        spark.udf().register("intsetFromHexWKB",    intsetFromHexWKB,    DataTypes.StringType);
        spark.udf().register("bigintsetFromHexWKB", bigintsetFromHexWKB, DataTypes.StringType);
        spark.udf().register("floatsetFromHexWKB",  floatsetFromHexWKB,  DataTypes.StringType);
        spark.udf().register("textsetFromHexWKB",   textsetFromHexWKB,   DataTypes.StringType);
        spark.udf().register("tstzsetFromHexWKB",   tstzsetFromHexWKB,   DataTypes.StringType);
        spark.udf().register("datesetFromHexWKB",   datesetFromHexWKB,   DataTypes.StringType);
        // Span
        spark.udf().register("intspanFromHexWKB",    intspanFromHexWKB,    DataTypes.StringType);
        spark.udf().register("bigintspanFromHexWKB", bigintspanFromHexWKB, DataTypes.StringType);
        spark.udf().register("floatspanFromHexWKB",  floatspanFromHexWKB,  DataTypes.StringType);
        spark.udf().register("tstzspanFromHexWKB",   tstzspanFromHexWKB,   DataTypes.StringType);
        spark.udf().register("datespanFromHexWKB",   datespanFromHexWKB,   DataTypes.StringType);
        // Spanset
        spark.udf().register("intspansetFromHexWKB",    intspansetFromHexWKB,    DataTypes.StringType);
        spark.udf().register("bigintspansetFromHexWKB", bigintspansetFromHexWKB, DataTypes.StringType);
        spark.udf().register("floatspansetFromHexWKB",  floatspansetFromHexWKB,  DataTypes.StringType);
        spark.udf().register("tstzspansetFromHexWKB",   tstzspansetFromHexWKB,   DataTypes.StringType);
        spark.udf().register("datespansetFromHexWKB",   datespansetFromHexWKB,   DataTypes.StringType);
        // Temporal scalar
        spark.udf().register("tboolFromHexWKB",  tboolFromHexWKB,  DataTypes.StringType);
        spark.udf().register("tintFromHexWKB",   tintFromHexWKB,   DataTypes.StringType);
        spark.udf().register("tfloatFromHexWKB", tfloatFromHexWKB, DataTypes.StringType);
        spark.udf().register("ttextFromHexWKB",  ttextFromHexWKB,  DataTypes.StringType);
        // Temporal-geo
        spark.udf().register("tgeometryFromHexEWKB",  tgeometryFromHexEWKB,  DataTypes.StringType);
        spark.udf().register("tgeographyFromHexEWKB", tgeographyFromHexEWKB, DataTypes.StringType);
        spark.udf().register("tgeompointFromHexEWKB", tgeompointFromHexEWKB, DataTypes.StringType);
        spark.udf().register("tgeogpointFromHexEWKB", tgeogpointFromHexEWKB, DataTypes.StringType);
        spark.udf().register("tgeometryFromMFJSON",   tgeometryFromMFJSON,   DataTypes.StringType);
        spark.udf().register("tgeographyFromMFJSON",  tgeographyFromMFJSON,  DataTypes.StringType);
        spark.udf().register("tgeompointFromMFJSON",  tgeompointFromMFJSON,  DataTypes.StringType);
        spark.udf().register("tgeogpointFromMFJSON",  tgeogpointFromMFJSON,  DataTypes.StringType);
        // Temporal scalar text
        spark.udf().register("tboolFromText",  tboolFromText,  DataTypes.StringType);
        spark.udf().register("tintFromText",   tintFromText,   DataTypes.StringType);
        spark.udf().register("tfloatFromText", tfloatFromText, DataTypes.StringType);
        spark.udf().register("ttextFromText",  ttextFromText,  DataTypes.StringType);
        // Geo temporal text/EWKT
        spark.udf().register("tgeompointFromText",  tgeompointFromText,  DataTypes.StringType);
        spark.udf().register("tgeogpointFromText",  tgeogpointFromText,  DataTypes.StringType);
        spark.udf().register("tgeompointFromEWKT",  tgeompointFromEWKT,  DataTypes.StringType);
        spark.udf().register("tgeogpointFromEWKT",  tgeogpointFromEWKT,  DataTypes.StringType);
        spark.udf().register("tgeometryFromText",   tgeometryFromText,   DataTypes.StringType);
        spark.udf().register("tgeographyFromText",  tgeographyFromText,  DataTypes.StringType);
        spark.udf().register("tgeometryFromEWKT",   tgeometryFromEWKT,   DataTypes.StringType);
        spark.udf().register("tgeographyFromEWKT",  tgeographyFromEWKT,  DataTypes.StringType);
        // Binary I/O
        spark.udf().register("tboolFromBinary",       tboolFromBinary,       DataTypes.StringType);
        spark.udf().register("tintFromBinary",        tintFromBinary,        DataTypes.StringType);
        spark.udf().register("tfloatFromBinary",      tfloatFromBinary,      DataTypes.StringType);
        spark.udf().register("ttextFromBinary",       ttextFromBinary,       DataTypes.StringType);
        spark.udf().register("tgeompointFromBinary",  tgeompointFromBinary,  DataTypes.StringType);
        spark.udf().register("tgeogpointFromBinary",  tgeogpointFromBinary,  DataTypes.StringType);
        spark.udf().register("tgeometryFromBinary",   tgeometryFromBinary,   DataTypes.StringType);
        spark.udf().register("tgeographyFromBinary",  tgeographyFromBinary,  DataTypes.StringType);
        spark.udf().register("tgeompointFromEWKB",    tgeompointFromEWKB,    DataTypes.StringType);
        spark.udf().register("tgeogpointFromEWKB",    tgeogpointFromEWKB,    DataTypes.StringType);
        spark.udf().register("tgeometryFromEWKB",     tgeometryFromEWKB,     DataTypes.StringType);
        spark.udf().register("tgeographyFromEWKB",    tgeographyFromEWKB,    DataTypes.StringType);
        spark.udf().register("asBinary",              asBinary,              DataTypes.BinaryType);
        spark.udf().register("asEWKB",                asEWKB,                DataTypes.BinaryType);
        spark.udf().register("asHexEWKB",             asHexEWKB,             DataTypes.StringType);
        // Geoset typed I/O
        spark.udf().register("geomsetFromText",     geomsetFromText,     DataTypes.StringType);
        spark.udf().register("geogsetFromText",     geogsetFromText,     DataTypes.StringType);
        spark.udf().register("geomsetFromEWKT",     geomsetFromEWKT,     DataTypes.StringType);
        spark.udf().register("geogsetFromEWKT",     geogsetFromEWKT,     DataTypes.StringType);
        spark.udf().register("geomsetFromHexWKB",   geomsetFromHexWKB,   DataTypes.StringType);
        spark.udf().register("geogsetFromHexWKB",   geogsetFromHexWKB,   DataTypes.StringType);
        spark.udf().register("geomsetFromBinary",   geomsetFromBinary,   DataTypes.StringType);
        spark.udf().register("geogsetFromBinary",   geogsetFromBinary,   DataTypes.StringType);
        spark.udf().register("geomsetFromEWKB",     geomsetFromEWKB,     DataTypes.StringType);
        spark.udf().register("geogsetFromEWKB",     geogsetFromEWKB,     DataTypes.StringType);
        // TBox typed I/O
        spark.udf().register("tboxFromHexWKB",      tboxFromHexWKB,      DataTypes.StringType);
        spark.udf().register("tboxFromBinary",      tboxFromBinary,      DataTypes.StringType);
        // STBox typed I/O
        spark.udf().register("stboxFromHexWKB",     stboxFromHexWKB,     DataTypes.StringType);
        spark.udf().register("stboxFromBinary",     stboxFromBinary,     DataTypes.StringType);
    }
}
