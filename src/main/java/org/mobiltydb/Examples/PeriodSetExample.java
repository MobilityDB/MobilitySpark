package org.mobiltydb.Examples;

import types.collections.time.Period;
import types.collections.time.PeriodSet;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UDTRegistration;
import org.mobiltydb.UDT.PeriodSetUDT;
import org.mobiltydb.UDT.PeriodUDT;

import utils.PeriodSetUDFRegistrator;
import utils.UDTRegistrator;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static functions.functions.meos_finalize;
import static functions.functions.meos_initialize;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.expr;

public class PeriodSetExample {
    public static void main(String[] args) throws SQLException, AnalysisException {
        meos_initialize("UTC");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        UDTRegistrator.registerUDTs(spark);

        PeriodSetUDFRegistrator.registerUDFs(spark);

        // Create Period and PeriodSet objects
        OffsetDateTime now = OffsetDateTime.now();
        Period period1 = new Period("[2021-04-08 05:04:45+01, 2021-04-08 06:04:45+01]");
        Period period2 = new Period("[2021-04-08 07:04:45+01, 2021-04-08 08:04:45+01]");
        Period period3 = new Period("[2021-04-08 09:04:45+01, 2021-04-08 10:04:45+01]");

        ArrayList<Period> periodSetList = new ArrayList<>();
        periodSetList.add(period1);
        periodSetList.add(period2);
        periodSetList.add(period3);
        PeriodSet periodSet = new PeriodSet(periodSetList);

        List<Row> data = List.of(
                RowFactory.create(periodSet)
        );

        StructType schema = new StructType()
                .add("periodSet", new PeriodSetUDT());

        // Create a DataFrame with a single column of Periods
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Register the DataFrame as a temporary view
        df.createOrReplaceTempView("PeriodSets");

        // Use Spark SQL to query the view
        Dataset<Row> result = spark.sql("SELECT * FROM PeriodSets");

        // Show the result
        result.show(false);

        df.printSchema();

        Dataset<Row> resultUdf = spark.sql("SELECT periodset_in('{[2021-04-08 05:04:45+01, 2021-04-08 06:04:45+01], [2021-04-08 07:04:45+01, 2021-04-08 08:04:45+01], [2021-04-08 09:04:45+01, 2021-04-08 10:04:45+01]}') as value");
        resultUdf.show(false);

        // Convert row into PeriodSet class
        Row periodSetValue = resultUdf.collectAsList().get(0);
        PeriodSet periodSet1 = (PeriodSet) periodSetValue.get(0);

        meos_finalize();
        spark.stop();

    }
}
