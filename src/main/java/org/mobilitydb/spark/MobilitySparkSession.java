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

import org.apache.spark.sql.SparkSession;
import org.mobilitydb.spark.geo.DistanceUDFs;
import org.mobilitydb.spark.geo.GeoAnalyticsUDFs;
import org.mobilitydb.spark.geo.GeoUDFs;
import org.mobilitydb.spark.geo.STBoxUDFs;
import org.mobilitydb.spark.geo.StaticGeoUDFs;
import org.mobilitydb.spark.geo.AlwaysSpatialRelsUDFs;
import org.mobilitydb.spark.geo.GeoAffineUDFs;
import org.mobilitydb.spark.geo.TempSpatialRelsUDFs;
import org.mobilitydb.spark.geo.TPointSTBoxOpsUDFs;
import org.mobilitydb.spark.temporal.AccessorAliasUDFs;
import org.mobilitydb.spark.temporal.AccessorUDFs;
import org.mobilitydb.spark.temporal.TileUDFs;
import org.mobilitydb.spark.temporal.SeqSetGapsUDFs;
import org.mobilitydb.spark.temporal.AnalyticsUDFs;
import org.mobilitydb.spark.temporal.BoolOpsUDFs;
import org.mobilitydb.spark.temporal.BucketUDFs;
import org.mobilitydb.spark.temporal.ConstructorUDFs;
import org.mobilitydb.spark.temporal.MathUDFs;
import org.mobilitydb.spark.temporal.PosOpsUDFs;
import org.mobilitydb.spark.temporal.PredicateUDFs;
import org.mobilitydb.spark.temporal.SimilarityUDFs;
import org.mobilitydb.spark.temporal.SpanAccessorUDFs;
import org.mobilitydb.spark.temporal.SpanAlgebraUDFs;
import org.mobilitydb.spark.temporal.SpanUDFs;
import org.mobilitydb.spark.temporal.IOAliasUDFs;
import org.mobilitydb.spark.temporal.SpansetOpsUDFs;
import org.mobilitydb.spark.temporal.SetOpsUDFs;
import org.mobilitydb.spark.temporal.SubtypeConstructorUDFs;
import org.mobilitydb.spark.temporal.AggregateUDAFs;
import org.mobilitydb.spark.temporal.MoreAccessorUDFs;
import org.mobilitydb.spark.temporal.RestrictionUDFs;
import org.mobilitydb.spark.temporal.TBoxOpsUDFs;
import org.mobilitydb.spark.temporal.TBoxUDFs;
import org.mobilitydb.spark.temporal.TemporalBoxOpsUDFs;
import org.mobilitydb.spark.temporal.TemporalCompUDFs;
import org.mobilitydb.spark.temporal.TTextUDFs;
import org.mobilitydb.spark.temporal.TemporalUDFs;
import org.mobilitydb.spark.temporal.TransformUDFs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static functions.functions.*;

/**
 * Entry point for MobilitySpark.
 *
 * Initialises MEOS and registers all UDFs with the given SparkSession.
 * Call {@link #create(SparkSession)} before running any temporal SQL,
 * and {@link #close()} after (or use try-with-resources).
 *
 * <pre>{@code
 *   SparkSession spark = SparkSession.builder().master("local[2]").getOrCreate();
 *   try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
 *       spark.sql("SELECT atTime(trip, instant) FROM trips").show();
 *   }
 * }</pre>
 */
public final class MobilitySparkSession implements AutoCloseable {

    private static final AtomicBoolean SRS_CSV_REGISTERED = new AtomicBoolean(false);

    private MobilitySparkSession() {}

    public static MobilitySparkSession create(SparkSession spark) {
        meos_initialize();
        meos_initialize_timezone("UTC");
        meos_initialize_noexit_error_handler();
        registerSpatialRefSys();
        TemporalUDFs.registerAll(spark);
        SpanUDFs.registerAll(spark);
        GeoUDFs.registerAll(spark);
        GeoAnalyticsUDFs.registerAll(spark);
        StaticGeoUDFs.registerAll(spark);
        DistanceUDFs.registerAll(spark);
        ConstructorUDFs.registerAll(spark);
        AccessorUDFs.registerAll(spark);
        SpanAccessorUDFs.registerAll(spark);
        SpanAlgebraUDFs.registerAll(spark);
        SpansetOpsUDFs.registerAll(spark);
        IOAliasUDFs.registerAll(spark);
        SubtypeConstructorUDFs.registerAll(spark);
        AccessorAliasUDFs.registerAll(spark);
        TileUDFs.registerAll(spark);
        SeqSetGapsUDFs.registerAll(spark);
        SetOpsUDFs.registerAll(spark);
        AnalyticsUDFs.registerAll(spark);
        PredicateUDFs.registerAll(spark);
        TBoxUDFs.registerAll(spark);
        TBoxOpsUDFs.registerAll(spark);
        TemporalCompUDFs.registerAll(spark);
        TemporalBoxOpsUDFs.registerAll(spark);
        TTextUDFs.registerAll(spark);
        STBoxUDFs.registerAll(spark);
        PosOpsUDFs.registerAll(spark);
        MathUDFs.registerAll(spark);
        BoolOpsUDFs.registerAll(spark);
        BucketUDFs.registerAll(spark);
        SimilarityUDFs.registerAll(spark);
        TempSpatialRelsUDFs.registerAll(spark);
        AlwaysSpatialRelsUDFs.registerAll(spark);
        GeoAffineUDFs.registerAll(spark);
        TPointSTBoxOpsUDFs.registerAll(spark);
        MoreAccessorUDFs.registerAll(spark);
        RestrictionUDFs.registerAll(spark);
        TransformUDFs.registerAll(spark);
        AggregateUDAFs.registerAll(spark);
        // Optional th3index family. It lives in its own package and is included
        // by default; passing -DH3=OFF drops that package from compilation,
        // mirroring the MEOS/MobilityDB CMake option. An excluded package is
        // absent from the classpath, so registerFamily skips it with zero
        // residue — the Java analogue of the C `#if H3` compile guard.
        registerFamily(spark, "org.mobilitydb.spark.h3.Th3IndexUDFs");
        return new MobilitySparkSession();
    }

    /**
     * Registers an optional family's UDFs by reflectively invoking its static
     * {@code registerAll(SparkSession)} method. When the family was excluded at
     * build time its class is absent from the classpath, so registration is
     * skipped — the Java analogue of the MEOS {@code #if H3} compile guard.
     * Any other reflective failure indicates a wiring error and is rethrown.
     */
    private static void registerFamily(SparkSession spark, String className) {
        final Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException excluded) {
            return; // family disabled at build time (e.g. -DH3=OFF)
        }
        try {
            cls.getMethod("registerAll", SparkSession.class).invoke(null, spark);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                "Failed to register temporal family " + className, e);
        }
    }

    /**
     * Extracts the bundled spatial_ref_sys.csv to a temp file and registers
     * it with MEOS so that geodetic coordinate operations (e.g. length on
     * tgeogpoint) can look up SRID definitions without a PostGIS installation.
     * Only runs once per JVM; subsequent calls are no-ops.
     */
    private static void registerSpatialRefSys() {
        if (!SRS_CSV_REGISTERED.compareAndSet(false, true)) return;
        try (InputStream in = MobilitySparkSession.class
                .getResourceAsStream("/spatial_ref_sys.csv")) {
            if (in == null) return;
            File tmp = File.createTempFile("meos_spatial_ref_sys", ".csv");
            tmp.deleteOnExit();
            try (OutputStream out = new FileOutputStream(tmp)) {
                byte[] buf = new byte[65536];
                int n;
                while ((n = in.read(buf)) > 0) out.write(buf, 0, n);
            }
            meos_set_spatial_ref_sys_csv(tmp.getAbsolutePath());
        } catch (Exception ignored) {}
    }

    @Override
    public void close() {
        meos_finalize();
    }
}
