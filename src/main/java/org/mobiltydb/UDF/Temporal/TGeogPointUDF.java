package org.mobiltydb.UDF.Temporal;

import jmeos.types.basic.tpoint.tgeog.TGeogPoint;
import net.postgis.jdbc.geometry.Point;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

public class TGeogPointUDF {
    public static UDF1<String, TGeogPoint> stringTGeogPoint = new UDF1<>() {
        @Override
        public TGeogPoint call(String s) throws Exception {
            return new TGeogPoint(s);
        }
    };

    public static UDF3<Double, Double, Timestamp, TGeogPoint> tGeogPointIn = new UDF3<>() {
        @Override
        public TGeogPoint call(Double latitude, Double longitude, Timestamp timestamp) throws Exception {
            Point point = new Point(latitude, longitude);
            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            String value = String.format("Point%s@%s+00", point.getValue(), outputFormat.format(timestamp));
            return new TGeogPoint(value);
        }
    };
}
