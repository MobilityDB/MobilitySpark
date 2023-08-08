package org.mobiltydb.UDF.General;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import org.apache.spark.sql.api.java.UDF1;


import java.sql.SQLException;

public class PeriodSetUDF {

    /***
     * Initialize PeriodSet from String.
     */
    public static UDF1<String, PeriodSet> periodset_in = PeriodSet::new;

}
