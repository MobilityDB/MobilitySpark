package org.mobiltydb.UDF.Period;

import jmeos.types.time.Period;
import org.apache.spark.sql.api.java.UDF1;

public class FromHexwkbUDF implements UDF1<String, Period> {
    @Override
    public Period call(String periodString) throws Exception{
        return Period.from_hexwkb(periodString);
    }
}