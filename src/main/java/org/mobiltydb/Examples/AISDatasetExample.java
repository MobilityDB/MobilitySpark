package org.mobiltydb.Examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.UDFRegistrator;
import utils.UDTRegistrator;


import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class AISDatasetExample {
    private static String CSV_PATH = "/home/satria/Documents/aisinput.csv";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CSVReaderApp")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> ais = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH);
        UDTRegistrator.registerUDTs(spark);
        UDFRegistrator.registerUDFs(spark);

        ais = ais.withColumn("point", callUDF("tGeogPointIn", col("latitude"), col("longitude"), col("t")))
                        .withColumn("sog", callUDF("tFloatIn", col("sog"), col("t")));
        ais = ais.drop("latitude", "longitude");

        ais.show();
        spark.stop();
    }
}
