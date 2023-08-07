package org.mobiltydb.Examples;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.mobiltydb.UDT.PeriodUDT;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import jmeos.types.time.Period;
import static jmeos.functions.functions.meos_initialize;
import static jmeos.functions.functions.meos_finalize;

public class PeriodExample {
    public static void main(String[] args) throws SQLException {
        meos_initialize("UTC");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        UDTRegistration.register(Period.class.getCanonicalName(), PeriodUDT.class.getCanonicalName());
        // Create some example Period objects
        OffsetDateTime now = OffsetDateTime.now();
        Period period1 = new Period(now, now.plusHours(1));
        Period period2 = new Period(now.plusHours(1), now.plusHours(2));
        Period period3 = new Period(now.plusHours(2), now.plusHours(3));

        List<Row> data = Arrays.asList(
                RowFactory.create(period1),
                RowFactory.create(period2),
                RowFactory.create(period3)
        );

        StructType schema = new StructType()
                .add("period", new PeriodUDT());

        // Create a DataFrame with a single column of Periods
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Register the DataFrame as a temporary view
        df.createOrReplaceTempView("Periods");

        // Use Spark SQL to query the view
        Dataset<Row> result = spark.sql("SELECT * FROM Periods");

        // Show the result
        result.show(false);

        Row first = result.first();
        System.out.println(first);

        meos_finalize();
    }
}

