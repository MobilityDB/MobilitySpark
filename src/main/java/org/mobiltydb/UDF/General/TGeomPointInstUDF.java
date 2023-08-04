package org.mobiltydb.UDF.General;

import jmeos.types.basic.tpoint.tgeom.TGeomPointInst;
import org.apache.spark.sql.api.java.UDF3;

import java.sql.SQLException;

public class TGeomPointInstUDF implements UDF3<Double, Double, String, String>  {

    @Override
    public String call(Double latitude, Double longitude, String timestamp) throws SQLException {
        String str_pointbuffer;
        str_pointbuffer = String.format("SRID=4326;Point(%f %f)@%s+00", longitude, latitude, timestamp);
        str_pointbuffer = str_pointbuffer.replaceAll(",", ".");

        return new TGeomPointInst(str_pointbuffer).toString();
    }
}