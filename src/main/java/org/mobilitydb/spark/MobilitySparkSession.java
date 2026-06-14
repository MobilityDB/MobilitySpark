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
import org.mobilitydb.spark.generated.GeneratedSpatioTemporalUDFs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static functions.GeneratedFunctions.meos_set_spatial_ref_sys_csv;

/**
 * Entry point for MobilitySpark.
 *
 * <p>Initialises MEOS and registers the entire catalog-GENERATED UDF surface
 * with the given SparkSession. The UDFs are produced at build time by
 * {@code tools/codegen_spark_udfs.py} from the MEOS-API catalog (North Star:
 * bindings are generated from MEOS, never hand-written), so this session holds
 * no hand-registered operator logic — it wires MEOS init + the generated
 * {@link GeneratedSpatioTemporalUDFs#registerAll(SparkSession)}.
 *
 * <pre>{@code
 *   SparkSession spark = SparkSession.builder().master("local[2]").getOrCreate();
 *   try (MobilitySparkSession ms = MobilitySparkSession.create(spark)) {
 *       spark.sql("SELECT numInstants(trip) FROM trips").show();
 *   }
 * }</pre>
 */
public final class MobilitySparkSession implements AutoCloseable {

    private static final AtomicBoolean SRS_CSV_REGISTERED = new AtomicBoolean(false);

    private MobilitySparkSession() {}

    public static MobilitySparkSession create(SparkSession spark) {
        // MeosThread.ensureReady() performs the guarded, idempotent MEOS init
        // (meos_initialize + UTC timezone + the no-exit error handler) that every
        // generated UDF also calls per worker thread.
        MeosThread.ensureReady();
        registerSpatialRefSys();
        GeneratedSpatioTemporalUDFs.registerAll(spark);
        return new MobilitySparkSession();
    }

    /**
     * Extracts the bundled spatial_ref_sys.csv to a temp file and registers it with
     * MEOS so geodetic coordinate operations can look up SRID definitions without a
     * PostGIS installation. Only runs once per JVM, and is a no-op when the resource
     * is absent (the planar BerlinMOD benchmark does not need it).
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
        // MEOS global state is process-wide; leave it initialised for the JVM
        // lifetime (meos_finalize is not called so a second create() is safe).
    }
}
