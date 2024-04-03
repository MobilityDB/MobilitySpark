package org.mobiltydb.Examples.Data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import utils.UDFRegistrator;
import utils.UDTRegistrator;

import static functions.functions.meos_finalize;
import static functions.functions.meos_initialize;
import static org.apache.spark.sql.functions.*;

public class PartitioningExample {

    private static String CSV_PATH = "src/main/java/org/mobiltydb/Examples/Data/states_2022-06-27-00.csv";

    public static void main(String[] args) {
        meos_initialize("UTC");
        SparkSession spark = SparkSession.builder()
                .appName("CSVReaderApp")
                .master("local[*]") //This causes the system to break if paralellism is too high
                //.config("spark.master", "local")
                .config("spark.sql.parquet.binaryAsString", true)
                .config("spark.executor.memory", "4G")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.default.parallelism", 2)
                .getOrCreate();

        UDTRegistrator.registerUDTs(spark);
        UDFRegistrator.registerUDFs(spark);

        // Read CSV file
        Dataset<Row> flights_raw = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH)
                //.limit(2000000)
                .na().drop("any", new String[]{"lat", "lon"})
                //.where("lat != NULL AND lon != NULL")
                .withColumn("time", col("time").cast(DataTypes.TimestampType)); //2277578

        flights_raw.createOrReplaceTempView("flights_raw");

        // flights_raw = flights_raw.repartition(2);

        flights_raw.show();
        flights_raw.printSchema();

        System.out.println(flights_raw.select("icao24").distinct().count()); //11121
        System.out.println(flights_raw.count());

        //Create Point Strings
        Dataset<Row> flights = flights_raw
                .withColumn("pointStr", concat_ws("", lit("Point("), col("lon"), lit(" "), col("lat"), lit(")@"), date_format(col("time"), "yyyy-MM-dd HH:mm:ss")))
                .withColumn("point", callUDF("stringToTGeogPoint", col("pointStr")))
                .select("icao24", "point", "pointStr");

        flights.show();

        System.out.println("Num partitions:" + flights.rdd().getNumPartitions());

        Dataset<Row> trajectories = flights.groupBy("icao24").agg(
                callUDF("tGeogPointSeqIn", collect_list(col("point"))).as("trajectory"),
                count("point").as("originalInstants")
        );

        //flights.unpersist(true);

        trajectories = trajectories.withColumn("meosInstants", callUDF("tGeogPointSeqNumInstant", col("trajectory")));

        trajectories.show();

//        spark.sql("""
//                CREATE TABLE flights (icao24 STRING, point STRING)
//                USING PARQUET
//                CLUSTERED BY (icao24) INTO 4 BUCKETS;
//                """);
//
//        spark.sql("""
//                INSERT INTO flights
//                SELECT
//                    icao24,
//                    CONCAT('Point(', lon, ' ', lat, ')@', DATE_FORMAT(time, 'yyyy-MM-dd HH:mm:ss')) AS point
//                FROM flights_raw
//                WHERE lat IS NOT NULL AND lon IS NOT NULL;
//                """);
//
//        Dataset<Row> result = spark.sql("SELECT icao24, stringToTGeogPoint(point) FROM flights LIMIT 5;");
//        result.show();
//        result.printSchema();
//

        meos_finalize();
    }

}
