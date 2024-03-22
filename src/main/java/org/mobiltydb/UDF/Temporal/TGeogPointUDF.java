package org.mobiltydb.UDF.Temporal;

import types.basic.tpoint.tgeog.TGeogPoint;
import types.basic.tpoint.tgeog.TGeogPointInst;
import types.basic.tpoint.tgeog.TGeogPointSeq;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

public class TGeogPointUDF {
    public static UDF1<String, TGeogPoint> stringTGeogPoint = new UDF1<>() {
        @Override
        public TGeogPoint call(String s) throws Exception {
            return new TGeogPointInst(s);
        }
    };

    /**
     * Convert ArraySeq from Dataframe to TGeogPointSeq.
     */
    public static UDF1<Seq<TGeogPointInst>, TGeogPointSeq> tGeogPointSeqIn = new UDF1<>() {
        @Override
        public TGeogPointSeq call(Seq<TGeogPointInst> points) throws Exception {
            List<TGeogPointInst> pointList = JavaConverters.seqAsJavaListConverter(points).asJava();
            return new TGeogPointSeq(Arrays.toString(pointList.toArray(new TGeogPointInst[0])));
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
    public static UDF1<TGeogPointSeq, LocalDateTime> tGeogPointSeqStartTimestamp = new UDF1<>() {
        @Override
        public LocalDateTime call(TGeogPointSeq tGeogPointSeq) {
            return tGeogPointSeq.start_timestamp();
        }
    };
}
