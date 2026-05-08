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

package org.mobiltydb;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.mobiltydb.UDF.PowerUDF;
import org.mobiltydb.UDT.classes.TimestampWithValue;

import static jmeos.functions.functions.meos_finalize;
import static jmeos.functions.functions.meos_initialize;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        meos_initialize("UTC");
        // Create an array of TGeomPointInst instances
        TimestampWithValue[] pointsArray = new TimestampWithValue[]{
                new TimestampWithValue(java.sql.Timestamp.valueOf("2023-07-20 12:00:00"), 10.5),
                new TimestampWithValue(java.sql.Timestamp.valueOf("2023-07-21 15:30:00"), 15.3),
                new TimestampWithValue(java.sql.Timestamp.valueOf("2023-07-22 18:45:00"), 20.1)
        };

        spark.udf().register("power", new PowerUDF(), DataTypes.DoubleType);


        // Convert the array to a Dataset
        Dataset<Row> pointsDF = spark.createDataFrame(java.util.Arrays.asList(pointsArray), TimestampWithValue.class);

        // Show the DataFrame
        pointsDF.show();

        // Register the DataFrame as a temporary table
        pointsDF.createOrReplaceTempView("pointsTable");

        // Use Spark SQL query to calculate the Euclidean distance and create a new DataFrame
        Dataset<Row> result = spark.sql(
                "SELECT timestamp, power(value) AS distance FROM pointsTable"
        );

        // Show the resulting DataFrame
        result.show();

        // Perform some basic operations on the DataFrame
        Dataset<Row> filteredPoints = pointsDF.filter("value > 15.0");
        filteredPoints.show();

        meos_finalize();

        // Stop the Spark session
        spark.stop();
    }
}
