package org.mobiltydb.UDF.General;

import jmeos.types.time.PeriodSet;
import org.apache.spark.sql.api.java.UDF1;


import java.sql.SQLException;

public class PeriodSetUDF implements UDF1<String, PeriodSet> {

    @Override
    public PeriodSet call(String value) throws SQLException {
        return new PeriodSet(value);
    }
}
