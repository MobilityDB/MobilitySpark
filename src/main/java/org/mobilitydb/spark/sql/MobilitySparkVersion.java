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

package org.mobilitydb.spark.sql;

import functions.GeneratedFunctions;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * The version of MobilitySpark, under its own name.
 *
 * Each binding answers its own version under its own name, and the version of the MEOS library
 * it links under MEOS's name: meosVersion() and meosFullVersion(), which {@link MobilitySparkSql}
 * generates from the catalog. These two are MobilitySpark's own, so no catalog entry states them;
 * they register through {@link MeosSqlRuntime#register} as the generated functions do.
 */
public final class MobilitySparkVersion {

    /** The resource the build writes the project version into. */
    private static final String RESOURCE = "/mobilityspark/version.properties";

    private MobilitySparkVersion() { }

    /** Returns "MobilitySpark" and the version of the project the jar is built from. */
    public static String mobilitysparkVersion() {
        Properties properties = new Properties();
        try (InputStream in = MobilitySparkVersion.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("MobilitySpark is built without " + RESOURCE);
            }
            properties.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return "MobilitySpark " + properties.getProperty("version");
    }

    /**
     * Returns the version of MobilitySpark followed by the string of
     * GeneratedFunctions.meos_full_version: the MEOS library it links and MEOS's dependencies.
     */
    public static String mobilitysparkFullVersion() {
        return mobilitysparkVersion() + ", " + GeneratedFunctions.meos_full_version();
    }

    /** Registers mobilitysparkVersion() and mobilitysparkFullVersion() on a session. */
    public static void registerAll(SparkSession spark) {
        MeosSqlRuntime.register(spark, "mobilitysparkVersion", new MeosSqlRuntime.Overload(
                new DataType[] {}, DataTypes.StringType, a -> mobilitysparkVersion()));
        MeosSqlRuntime.register(spark, "mobilitysparkFullVersion", new MeosSqlRuntime.Overload(
                new DataType[] {}, DataTypes.StringType, a -> mobilitysparkFullVersion()));
    }
}
