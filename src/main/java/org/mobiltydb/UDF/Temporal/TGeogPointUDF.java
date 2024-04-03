package org.mobiltydb.UDF.Temporal;

import org.apache.spark.sql.api.java.UDF2;
import org.locationtech.jts.geom.*;
import org.mobiltydb.UDT.TGeogPointInstUDT;
import types.basic.tfloat.TFloatInst;
import types.basic.tpoint.tgeog.TGeogPoint;
import types.basic.tpoint.tgeog.TGeogPointInst;
import types.basic.tpoint.tgeog.TGeogPointSeq;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import types.collections.time.Time;
import types.temporal.TInstant;
import types.temporal.Temporal;
import utils.TInstComparator;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TGeogPointUDF {
    public static UDF1<String, TGeogPointInst> stringTGeogPointInst = new UDF1<>() {
        @Override
        public TGeogPointInst call(String s) throws Exception {
            return new TGeogPointInst(s);
        }
    };

    /**
     * Create a tGeogPointIn
     */
    public static UDF3<Double, Double, String, TGeogPoint> tGeogPointInstIn = new UDF3<>() {
        @Override
        public TGeogPoint call(Double latitude, Double longitude, String ts) throws Exception {
            String s = "";
            // Now use these to create a TGeogPoint instance
            return new TGeogPointInst(s);
        }
    };


    /**
     * Convert ArraySeq from Dataframe to TGeogPointSeq.
     */
    public static UDF1<Seq<TGeogPointInst>, TGeogPointSeq> tGeogPointSeqIn = new UDF1<>() {
        @Override
        public TGeogPointSeq call(Seq<TGeogPointInst> points) throws Exception {
            //System.out.println(points);
            ArrayList<TGeogPointInst> pointsList = new ArrayList<>(JavaConverters.seqAsJavaListConverter(points).asJava());
            //List<String> pointsList = JavaConverters.seqAsJavaListConverter(points).asJava();
            System.out.println(pointsList);
            //Set<TGeogPointInst> pointSet = new LinkedHashSet<>(pointsList);
            //pointsList = new ArrayList<>(pointSet);
            Collections.sort(pointsList, new TInstComparator());
            return new TGeogPointSeq(pointsList.toString());
        }
    };

    /**
     * Return the number of instant in TGeogPointSeq.
     */
    public static UDF1<TGeogPointSeq, Integer> tGeogPointNumInstant = new UDF1<>() {
        @Override
        public Integer call(TGeogPointSeq tGeogPointSeq){
            return tGeogPointSeq.num_instants();
        }
    };

    /**
     * Return start timestamp from TGeogPointSeq.
     */
    public static UDF1<TGeogPointSeq, Timestamp> tGeogPointSeqStartTimestamp = new UDF1<>() {
        @Override
        public Timestamp call(TGeogPointSeq tGeogPointSeq){
            return Timestamp.valueOf(tGeogPointSeq.start_timestamp());
        }
    };

}
