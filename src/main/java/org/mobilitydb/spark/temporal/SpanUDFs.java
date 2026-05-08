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
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.HexFormat;

/**
 * Spark SQL UDFs for span and spanset TemporalParquet readers.
 *
 * Each xFromBinary UDF converts a Parquet BYTE_ARRAY column (written by
 * MobilityDuck's asBinary()) to the internal hex-WKB string used throughout
 * MobilitySpark.
 *
 * Implementation uses the type-agnostic span_from_hexwkb / spanset_from_hexwkb
 * MEOS functions — the WKB type-code embedded in the byte stream identifies the
 * concrete span type. Type-specific names exist for SQL discoverability and to
 * match the MobilityDuck surface (tstzspanFromBinary, intspanFromBinary, ...).
 *
 * Write-back (span → Parquet BINARY) uses the existing TemporalUDFs.asBinary,
 * which hex-decodes any hex-WKB string regardless of the underlying MEOS type.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class SpanUDFs {

    private SpanUDFs() {}

    // ------------------------------------------------------------------
    // Span fromBinary helpers
    //
    // MEOS: span_from_hexwkb(const char *) → Span *
    //       span_as_hexwkb(const Span *, uint8_t variant) → char *
    // ------------------------------------------------------------------
    private static String spanFromBinaryImpl(byte[] bytes) throws Exception {
        if (bytes == null) return null;
        String hex = HexFormat.of().formatHex(bytes).toUpperCase();
        Pointer ptr = functions.span_from_hexwkb(hex);
        if (ptr == null) return null;
        return functions.span_as_hexwkb(ptr, (byte) 0);
    }

    // ------------------------------------------------------------------
    // Spanset fromBinary helpers
    //
    // MEOS: spanset_from_hexwkb(const char *) → SpanSet *
    //       spanset_as_hexwkb(const SpanSet *, uint8_t variant) → char *
    // ------------------------------------------------------------------
    private static String spansetFromBinaryImpl(byte[] bytes) throws Exception {
        if (bytes == null) return null;
        String hex = HexFormat.of().formatHex(bytes).toUpperCase();
        Pointer ptr = functions.spanset_from_hexwkb(hex);
        if (ptr == null) return null;
        return functions.spanset_as_hexwkb(ptr, (byte) 0);
    }

    // ------------------------------------------------------------------
    // Span fromBinary UDFs
    // ------------------------------------------------------------------
    public static final UDF1<byte[], String> tstzspanFromBinary  = SpanUDFs::spanFromBinaryImpl;
    public static final UDF1<byte[], String> intspanFromBinary   = SpanUDFs::spanFromBinaryImpl;
    public static final UDF1<byte[], String> floatspanFromBinary = SpanUDFs::spanFromBinaryImpl;
    public static final UDF1<byte[], String> bigintspanFromBinary= SpanUDFs::spanFromBinaryImpl;
    public static final UDF1<byte[], String> datespanFromBinary  = SpanUDFs::spanFromBinaryImpl;

    // ------------------------------------------------------------------
    // Spanset fromBinary UDFs
    // ------------------------------------------------------------------
    public static final UDF1<byte[], String> tstzspansetFromBinary  = SpanUDFs::spansetFromBinaryImpl;
    public static final UDF1<byte[], String> intspansetFromBinary   = SpanUDFs::spansetFromBinaryImpl;
    public static final UDF1<byte[], String> floatspansetFromBinary = SpanUDFs::spansetFromBinaryImpl;
    public static final UDF1<byte[], String> bigintspansetFromBinary= SpanUDFs::spansetFromBinaryImpl;
    public static final UDF1<byte[], String> datespansetFromBinary  = SpanUDFs::spansetFromBinaryImpl;

    public static void registerAll(org.apache.spark.sql.SparkSession spark) {
        spark.udf().register("tstzspanFromBinary",     tstzspanFromBinary,     DataTypes.StringType);
        spark.udf().register("intspanFromBinary",      intspanFromBinary,      DataTypes.StringType);
        spark.udf().register("floatspanFromBinary",    floatspanFromBinary,    DataTypes.StringType);
        spark.udf().register("bigintspanFromBinary",   bigintspanFromBinary,   DataTypes.StringType);
        spark.udf().register("datespanFromBinary",     datespanFromBinary,     DataTypes.StringType);
        spark.udf().register("tstzspansetFromBinary",  tstzspansetFromBinary,  DataTypes.StringType);
        spark.udf().register("intspansetFromBinary",   intspansetFromBinary,   DataTypes.StringType);
        spark.udf().register("floatspansetFromBinary", floatspansetFromBinary, DataTypes.StringType);
        spark.udf().register("bigintspansetFromBinary",bigintspansetFromBinary,DataTypes.StringType);
        spark.udf().register("datespansetFromBinary",  datespansetFromBinary,  DataTypes.StringType);
    }
}
