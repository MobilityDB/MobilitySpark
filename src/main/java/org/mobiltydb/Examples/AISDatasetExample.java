package org.mobiltydb.Examples;

import org.apache.spark.sql.*;
import org.locationtech.jts.geom.Geometry;
import scala.collection.JavaConversions;
import types.basic.tfloat.TFloatInst;
import types.basic.tfloat.TFloatSeq;
import types.basic.tpoint.tgeog.TGeogPointInst;
import types.basic.tpoint.tgeog.TGeogPointSeq;
import types.basic.tpoint.tgeom.TGeomPointInst;
import types.temporal.TInterpolation;
import utils.TInstComparator;
import utils.UDFRegistrator;
import utils.UDTRegistrator;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static functions.functions.meos_finalize;
import static functions.functions.meos_initialize;
import static org.apache.spark.sql.functions.*;


public class AISDatasetExample {
    private static String CSV_PATH = "src/main/java/org/mobiltydb/Examples/Data/aisinput.csv";

    public static void main(String[] args) {
        meos_initialize("UTC");

        //TGeomPointInst tgeom = new TGeomPointInst("Point(1.5 1.5)@2021-01-08 00:00:00");
        //System.out.println(tgeom.toString());

        TFloatInst tf1 = new TFloatInst("1.5@2019-09-01 00:00:00+00");
        TFloatInst tf2 = new TFloatInst("2.5@2019-09-02 00:00:02+00");
        TFloatInst tf3 = new TFloatInst("1.3@2019-09-02 00:00:03+00");

        List<TFloatInst> floatList = new ArrayList<>();
        floatList.add(tf3);
        floatList.add(tf2);

        Collections.sort(floatList, new TInstComparator());

        String floatListString = Arrays.toString(floatList.toArray());
        System.out.println(floatListString);

        TFloatSeq tfs1 = new TFloatSeq("[1.5@2019-09-01 00:00:00, 2.5@2019-09-02]");
        System.out.println(tfs1.toString());
        TFloatSeq tfs2 = new TFloatSeq(floatListString);
        System.out.println(tfs2.toString());

        System.out.println(floatList.toString());

        TFloatSeq tfs3 = new TFloatSeq(floatList.toString());
        tfs3 = (TFloatSeq) tfs3.update(tf1);

        System.out.println(tfs3);

        TGeogPointInst tgpi1 = new TGeogPointInst("POINT(12.272388 57.059)@2021-01-08 00:00:00+00");
        TGeogPointInst tgpi2 = new TGeogPointInst("POINT(12.272388 58.059)@2021-01-08 00:00:01+00");
        TGeogPointInst tgpi3 = new TGeogPointInst("POINT(12.272388 59.059)@2021-01-08 00:00:02+00");

        List<TGeogPointInst> pointList = new ArrayList<>();
        pointList.add(tgpi1);
        pointList.add(tgpi2);

        //Collections.sort(pointList, new TInstComparator());

        TGeogPointSeq tgps1 = new TGeogPointSeq(pointList.toString());
        System.out.println(tgps1);

        SparkSession spark = SparkSession.builder()
                .appName("CSVReaderApp")
                .master("local[1]") // This causes the system to break if > 1 for some reason
                //.config("spark.master", "local")
                .getOrCreate();

        UDTRegistrator.registerUDTs(spark);
        UDFRegistrator.registerUDFs(spark);

        // Read CSV file
        Dataset<Row> ais = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH); //156837
        ais.show();

        //System.out.println(ais.count());

////        // Read AIS Dataset
//        ais = ais.withColumn("point", callUDF("tGeogPointInstIn", col("latitude"), col("longitude"), col("t")))
//                        .withColumn("sog", callUDF("tFloatIn", col("sog"), col("t")));
//        ais = ais.drop("latitude", "longitude");
//        ais.show();


// Assuming 't' is the timestamp column, 'latitude' and 'longitude' are the lat/long columns
        ais = ais
                .withColumn("pointStr", concat_ws("", lit("Point("), col("longitude"), lit(" "), col("latitude"), lit(")@"), date_format(col("t"), "yyyy-MM-dd HH:mm:ss")))
                .withColumn("point", callUDF("stringToTGeogPoint", col("pointStr")))
                .withColumn("sog", callUDF("tFloatIn", col("sog"), col("t")))
                .drop("latitude", "longitude"); // Drop the intermediate 'pointStr' column along with 'latitude' and 'longitude'


        ais.printSchema();

        ais.show(false);

        // Assemble AIS Dataset
        Dataset<Row> trajectories = ais.groupBy("mmsi")
                .agg(
                        callUDF("tGeogPointSeqIn", collect_list(col("pointStr"))).as("trajectory"), //,
                        callUDF("tFloatSeqIn", collect_list(col("sog"))).as("sog")
                );

        trajectories.printSchema();

        trajectories.limit(5).show();
//
//        // TODO: Inspect why the number of points is not reduced for SparkMeos implementation.
        Dataset<Row> originalCounts = ais.groupBy("mmsi")
                .count()
                .withColumnRenamed("count", "original #points");

        originalCounts.show();
//
        Dataset<Row> instantsCounts = trajectories
                .withColumn("SparkMEOS #points", callUDF("tGeogPointSeqNumInstant", trajectories.col("trajectory")))
                .withColumn("SparkMEOS #points (tfloat)", callUDF("tFloatNumInstants", trajectories.col("sog")));
        instantsCounts.show();

        Dataset<Row> startTimeStamp = trajectories
                .withColumn("Start Timestamp", callUDF("tGeogPointSeqStartTimestamp", trajectories.col("trajectory")));

        startTimeStamp.show();

        originalCounts.join(instantsCounts, "mmsi").join(startTimeStamp, "mmsi").
                select("mmsi", "SparkMEOS #points", "original #points", "Start Timestamp").limit(10).show();

        System.out.println(ais.select("mmsi").distinct().count());

        trajectories.repartition(2).selectExpr("tGeogPointSeqNumInstant(trajectory)").show();

        meos_finalize();

    }
}
