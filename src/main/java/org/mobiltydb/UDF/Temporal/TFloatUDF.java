package org.mobiltydb.UDF.Temporal;

import scala.collection.JavaConversions;
import types.basic.tfloat.TFloat;
import types.basic.tfloat.TFloatInst;
import types.basic.tfloat.TFloatSeq;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import utils.TInstComparator;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class TFloatUDF {
    public static UDF1<String, TFloat> stringToTFloat = new UDF1<>() {
        @Override
        public TFloat call(String s) throws Exception {
            return new TFloatInst(s);
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
    public static UDF1<Seq<TFloatInst>, TFloatSeq> tFloatSeqIn = new UDF1<>() {
        @Override
        public TFloatSeq call(Seq<TFloatInst> floats) throws Exception {
            ArrayList<TFloatInst> floatList = new ArrayList<>(JavaConverters.seqAsJavaListConverter(floats).asJava());
//            // Remove duplicates by converting the List to a LinkedHashSet
//            Set<TFloatInst> floatSet = new LinkedHashSet<>(floatList);
//            floatList = new ArrayList<>(floatSet);
            Collections.sort(floatList, new TInstComparator());
            return new TFloatSeq(floatList.toString());
        }
    };

    public static UDF1<TFloatSeq, Integer> tFloatNumInstants = new UDF1<TFloatSeq, Integer>() {
        @Override
        public Integer call(TFloatSeq tFloatSeq) throws Exception {
            return tFloatSeq.num_instants();
        }
    };
}
