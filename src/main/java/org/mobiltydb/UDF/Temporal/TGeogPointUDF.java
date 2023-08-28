package org.mobiltydb.UDF.Temporal;

import jmeos.types.basic.tpoint.tgeog.TGeogPoint;
import jmeos.types.basic.tpoint.tgeog.TGeogPointInst;
import jmeos.types.basic.tpoint.tgeog.TGeogPointSeq;
import net.postgis.jdbc.geometry.Point;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import scala.collection.JavaConverters;
import scala.collection.immutable.ArraySeq;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.util.List;

public class TGeogPointUDF {
    public static UDF1<String, TGeogPoint> stringTGeogPoint = new UDF1<>() {
        @Override
        public TGeogPoint call(String s) throws Exception {
            return new TGeogPoint(s);
        }
    };

    /**
     * Initiate TGeogPoint from (Double, Double, Timestamp).
     */
    public static UDF3<Double, Double, Timestamp, TGeogPointInst> tGeogPointInstIn = new UDF3<>() {
        @Override
        public TGeogPointInst call(Double latitude, Double longitude, Timestamp timestamp) throws Exception {
            Point point = new Point(latitude, longitude);
            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX");

            String value = String.format("Point%s@%s", point.getValue(), outputFormat.format(timestamp));
            return new TGeogPointInst(value);
        }
    };

    /**
     * Convert ArraySeq from Dataframe to TGeogPointSeq.
     */
    public static UDF1<ArraySeq<TGeogPointInst>, TGeogPointSeq> tGeogPointSeqIn = new UDF1<>() {
        @Override
        public TGeogPointSeq call(ArraySeq<TGeogPointInst> points) throws Exception {
            List<TGeogPointInst> pointList = JavaConverters.seqAsJavaListConverter(points).asJava();
            return new TGeogPointSeq(pointList.toArray(new TGeogPointInst[0]));
        }
    };

    /**
     * Return the number of instant in TGeogPointSeq.
     */
    public static UDF1<TGeogPointSeq, Integer> tGeogPointNumInstant = new UDF1<>() {
        @Override
        public Integer call(TGeogPointSeq tGeogPointSeq){
            return tGeogPointSeq.numInstants();
        }
    };

    /**
     * Return start timestamp from TGeogPointSeq.
     */
    public static UDF1<TGeogPointSeq, OffsetDateTime> tGeogPointSeqStartTimestamp = new UDF1<>() {
        @Override
        public OffsetDateTime call(TGeogPointSeq tGeogPointSeq) {
            return tGeogPointSeq.startTimestamp();
        }
    };
}
