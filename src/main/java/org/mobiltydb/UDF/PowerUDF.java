package org.mobiltydb.UDF;

import org.apache.spark.sql.api.java.UDF1;

public class PowerUDF implements UDF1<Double, Double> {
    @Override
    public Double call(Double point1) {
        // Calculate the distance between the two TemporalPoint objects using their properties.
        return Math.abs(point1*point1);
    }
}
