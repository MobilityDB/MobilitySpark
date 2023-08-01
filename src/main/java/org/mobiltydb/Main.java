package org.mobiltydb;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.mobiltydb.UDF.PowerUDF;
import org.mobiltydb.UDT.classes.TimestampWithValue;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

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

        // Stop the Spark session
        spark.stop();
    }
}
