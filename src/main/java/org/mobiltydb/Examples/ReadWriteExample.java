package org.mobiltydb.Examples;

import jmeos.types.time.Period;
import org.apache.spark.sql.*;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;
import org.mobiltydb.UDF.Period.PeriodUDFRegistrator;
import org.mobiltydb.UDT.PeriodUDT;
import utils.MobilityDBJdbcDialect;
import utils.UDTRegistrator;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static jmeos.functions.functions.meos_finalize;
import static jmeos.functions.functions.meos_initialize;

public class MeosDialectExample {
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

        // Register the dialect
        JdbcDialects.registerDialect(new MobilityDBJdbcDialect());
        String jdbcUrl = "jdbc:postgresql://localhost:25432/mobilitydb?useMobilityDB=true\n";


        // Create some example Period objects
        OffsetDateTime now = OffsetDateTime.now();
        Period period1 = new Period(now, now.plusHours(1));
        Period period2 = new Period(now.plusHours(1), now.plusHours(3));
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


        System.out.println("Example 1: Show all Periods.");
        df.printSchema();
        // Show the result
        result.show(false);

        String tableName = "jdbcExample";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "docker");
        connectionProperties.put("password", "docker");

        //result.write().csv("~/test.csv");
        result.write().mode("overwrite").parquet("testp.parquet");

        //Load back parquet
        Dataset<Row> resultLoaded = spark.read().parquet("testp.parquet");
        resultLoaded.printSchema();
        resultLoaded.show();

        //result.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, tableName, connectionProperties);

        meos_finalize();
    }
}
