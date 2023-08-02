package org.mobiltydb.UDF.General;

import org.apache.spark.sql.api.java.UDF3;
import types.basic.tpoint.tgeom.TGeomPointInst;

import java.sql.SQLException;
import java.sql.Timestamp;

import static function.functions.pg_time_out;
import static function.functions.pg_timestamp_in;

public class TGeomPointInstUDF implements UDF3<Double, Double, String, String>  {

    @Override
    public String call(Double latitude, Double longitude, String timestamp) throws SQLException {
        String str_pointbuffer;
        str_pointbuffer = String.format("SRID=4326;Point(%f %f)@%s+00", longitude, latitude, timestamp);
        str_pointbuffer = str_pointbuffer.replaceAll(",", ".");
        System.out.println(str_pointbuffer);
        return new TGeomPointInst(str_pointbuffer).toString();
    }
}