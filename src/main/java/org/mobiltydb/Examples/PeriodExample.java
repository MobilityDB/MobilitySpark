package org.mobiltydb.Examples;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.mobiltydb.UDF.Period.PeriodUDFRegistrator;
import org.mobiltydb.UDT.PeriodUDT;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import types.collections.time.Period;
import utils.UDTRegistrator;

import static functions.functions.meos_initialize;
import static functions.functions.meos_finalize;
import static org.apache.spark.sql.functions.*;


/**
 * This example implements simple use cases utilizing the PeriodUDT Spark version of the Period class.
 * This is only an example of usage but is not a proper test.
**/
public class PeriodExample {
    public static void main(String[] args) throws SQLException, AnalysisException {
        meos_initialize("UTC");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        UDTRegistrator.registerUDTs(spark);
        //UDFRegistrator.registerUDFs(spark);
        PeriodUDFRegistrator.registerAllUDFs(spark);
        // Create some example Period objects
        OffsetDateTime now = OffsetDateTime.now();
        Period period1 = new Period("[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02)");
        Period period2 = new Period("[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02)");
        Period period3 = new Period("[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02)");

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


        System.out.println("Example 1: Show all Periods.");
        df.printSchema();
        // Show the result
        result.show(false);

        // This will throw error because the attributes of period are not exposed to the DataFrame schema!!!
//        Dataset<Row> result2 = df
//                .withColumn("startDate", col("period.lower"))
//                .withColumn("endDate", col("period.upper"))
//                .withColumn("lowerInclusive", col("period.lowerInclusive"))
//                .withColumn("upperInclusive", col("period.upperInclusive"));

        System.out.println("Example 2: Parse a string to period and show the table.");
        spark.sql("SELECT stringToPeriod('[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02)') as period")
                .show(false);

        Period hex = Period.from_hexwkb("012100000040021FFE3402000000B15A26350200");
        System.out.println(hex.toString());
        System.out.println(hex);

        Period p2 = new Period("[2019-09-08 00:00:01Z, 2023-08-07 13:10:49Z)");
        System.out.println(p2.toString());
        System.out.println(p2);

        System.out.println("Example 3: Parse a hexwkb string to period and show the table.");
        //spark.sql("SELECT periodFromHexwkb('012100000040021FFE3402000000B15A26350200') as period")
        //        .show(false);

        System.out.println("Example 4: Add a column displaying the width of the period.");
        df.withColumn("width", expr("periodWidth(period)"));
        //Same but using callUDF method
        df.withColumn("width", call_udf("periodWidth", col("period"))).show();

        System.out.println("Example 5: Select the result of expanding two periods: [2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02) and (2019-09-08 02:00:00+02, 2019-09-10 02:00:00+02)");
        spark.sql("SELECT periodExpand(stringToPeriod('[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02)'), " +
                "stringToPeriod('[2019-09-08 02:00:01+02, 2019-09-10 02:00:01+02)')) as period").show(false);

        Period period4 = new Period("[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02)");
        Period period5 = new Period("[2023-08-06 14:10:49+02, 2023-08-08 15:10:49+02)");
        Period period6 = new Period("[2023-08-08 14:10:49+02, 2023-08-09 15:10:49+02)");

        List<Row> data2 = Arrays.asList(
                RowFactory.create(period4),
                RowFactory.create(period5),
                RowFactory.create(period6)
        );

        Dataset<Row> df2 = spark.createDataFrame(data2, schema);

        df2 = df2
                .withColumn(
                        "otherPeriod",
                        expr("stringToPeriod('[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02)')"))
                .withColumn(
                        "overlaps",
                        expr("periodOverlapsPeriod(period, otherPeriod)"));
                //.withColumn("as_period_set", expr("periodToPeriodSet(period)"))

        System.out.println("Example 6: Create a dataframe with three new periods on the first column, " +
                "a second column with the period [2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02) " +
                "and a third column indicating if they overlap.");
        df2.show(false);
        df2.printSchema();



        meos_finalize();
    }
}
