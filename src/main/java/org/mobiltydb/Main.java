package org.mobiltydb;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.mobiltydb.UDF.General.TGeomPointInstUDF;
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
        spark.udf().register("tgeompointinst_in", new TGeomPointInstUDF(), DataTypes.StringType);

        // Convert the array to a Dataset
        Dataset<Row> pointsDF = spark.createDataFrame(java.util.Arrays.asList(pointsArray), TimestampWithValue.class);

        // Show the DataFrame
        pointsDF.show();

        // Register the DataFrame as a temporary table
        pointsDF.createOrReplaceTempView("pointsTable");

        Dataset<Row> result = spark.sql(
                "SELECT tgeompointinst_in(double(12.272388), double(57.059), '2021-01-08 00:00:00') as value"
        );

//        Dataset<Row> result = spark.sql(
//                "SELECT timestamp, power(value) AS distance FROM pointsTable"
//        );
//

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
