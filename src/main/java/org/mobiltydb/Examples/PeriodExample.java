package org.mobiltydb.Examples;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.mobiltydb.UDF.Period.StringToPeriodUDF;
import org.mobiltydb.UDT.PeriodUDT;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import jmeos.types.time.Period;
import utils.UDFRegistrator;
import utils.UDTRegistrator;

import static jmeos.functions.functions.meos_initialize;
import static jmeos.functions.functions.meos_finalize;
import static org.apache.spark.sql.functions.col;


/**
 * This example implements simple use cases utilizing the PeriodUDT Spark version of the Period class.
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
        UDFRegistrator.registerUDFs(spark);

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

        df.printSchema();

        // This will throw error because the attributes of period are not exposed to the DataFrame schema!!!
//        Dataset<Row> result2 = df
//                .withColumn("startDate", col("period.lower"))
//                .withColumn("endDate", col("period.upper"))
//                .withColumn("lowerInclusive", col("period.lowerInclusive"))
//                .withColumn("upperInclusive", col("period.upperInclusive"));

        spark.sql("SELECT stringToPeriod('[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02)') as period")
                .show(false);

        meos_finalize();
    }
}

