package org.mobiltydb.UDF.Temporal;

import jmeos.types.basic.tfloat.TFloat;
import jmeos.types.basic.tfloat.TFloatInst;
import jmeos.types.basic.tfloat.TFloatSeq;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.JavaConverters;
import scala.collection.immutable.ArraySeq;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

public class TFloatUDF {
    public static UDF1<String, TFloat> stringToTFloat = new UDF1<>() {
        @Override
        public TFloat call(String s) throws Exception {
            return new TFloat(s);
        }
    };

    /**
     * Initialize TFloat from Double and Timestamp.
     */
    public static UDF2<Double, Timestamp, TFloatInst> tFloatInstIn = new UDF2<>() {
        @Override
        public TFloatInst call(Double val, Timestamp timestamp) throws Exception {
            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String value = String.format("%f@%s+00", val, outputFormat.format(timestamp));

            return new TFloatInst(value);
        }
    };

    /**
     * Initialize TFloatSeq from TFloatInst rows.
     */
    public static UDF1<ArraySeq<TFloatInst>, TFloatSeq> tFloatSeqIn = new UDF1<>() {
        @Override
        public TFloatSeq call(ArraySeq<TFloatInst> floats) throws Exception {
            List<TFloatInst> floatList = JavaConverters.seqAsJavaListConverter(floats).asJava();
            return new TFloatSeq(floatList.toArray(new TFloatInst[0]));
        }
    };
}
