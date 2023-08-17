package org.mobiltydb.Examples;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import org.mobiltydb.UDT.TimestampSetUDT;
import org.mobiltydb.UDT.OffsetDateTimeUDT;

import jmeos.types.time.TimestampSet;
import utils.UDTRegistrator;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import static jmeos.functions.functions.meos_initialize;
import static jmeos.functions.functions.meos_finalize;
import static org.apache.spark.sql.functions.*;

public class TimestampSetExample {

    public static void main(String[] args) throws SQLException {
        meos_initialize("UTC");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL TimestampSet example")
                .config("spark.master", "local")
                .getOrCreate();

        UDTRegistrator.registerUDTs(spark); // Assuming this function registers TimestampSetUDT

        // Create some example OffsetDateTime objects
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime time1 = now.plusHours(1);
        OffsetDateTime time2 = now.plusHours(2);
        OffsetDateTime time3 = now.plusHours(3);

        TimestampSet tsSet1 = new TimestampSet(now, time1);
        TimestampSet tsSet2 = new TimestampSet(time1, time2);
        TimestampSet tsSet3 = new TimestampSet(time2, time3);

        List<Row> data = Arrays.asList(
                RowFactory.create(tsSet1),
                RowFactory.create(tsSet2),
                RowFactory.create(tsSet3)
        );

        StructType schema = new StructType()
                .add("timestampSet", new TimestampSetUDT());

        // Create a DataFrame with columns of OffsetDateTime and TimestampSet
        Dataset<Row> df = spark.createDataFrame(data, schema);

        System.out.println("Example 1: Create a dataframe with 1 column of TimestampSet data type.");

        // Show the result
        df.show(false);
        df.printSchema();

        meos_finalize();
    }
}
