package org.mobiltydb.UDF.Temporal;

import types.boxes.TBox;
import org.apache.spark.sql.api.java.UDF1;

public class TBoxUDF {
    public static UDF1<String, TBox> stringToTBox = new UDF1<String, TBox>() {
        @Override
        public TBox call(String s) throws Exception {
            return new TBox(s);
        }
    };
}
