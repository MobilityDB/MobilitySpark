package org.mobiltydb.Examples;

import org.apache.spark.sql.*;
import utils.UDFRegistrator;
import utils.UDTRegistrator;


import static jmeos.functions.functions.meos_finalize;
import static jmeos.functions.functions.meos_initialize;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class AISDatasetExample {
    private static String CSV_PATH = "/home/satria/Documents/aisinput.csv";

    public static void main(String[] args) {
        meos_initialize("UTC+2");

        SparkSession spark = SparkSession.builder()
                .appName("CSVReaderApp")
                .master("local[*]")
                .getOrCreate();

        UDTRegistrator.registerUDTs(spark);
        UDFRegistrator.registerUDFs(spark);

        // Read CSV file
        Dataset<Row> ais = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH);
        ais.show();

        // Read AIS Dataset
        ais = ais.withColumn("point", callUDF("tGeogPointIn", col("latitude"), col("longitude"), col("t")))
                        .withColumn("sog", callUDF("tFloatIn", col("sog"), col("t")));
        ais = ais.drop("latitude", "longitude");
        ais.show();

        // Assemble AIS Dataset
        Dataset<Row> trajectories = ais.groupBy("mmsi")
                .agg(callUDF("tGeogPointSeqIn", functions.collect_list(col("point"))).as("trajectory"),
                        callUDF("tFloatSeqIn", functions.collect_list(col("sog"))).as("sog"));
        trajectories.show();

        spark.stop();
        meos_finalize();
    }
}
