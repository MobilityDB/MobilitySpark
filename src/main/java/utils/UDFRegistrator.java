package utils;

import org.apache.spark.sql.SparkSession;
import org.mobiltydb.UDF.Period.*;
import org.mobiltydb.UDT.*;

/**
 * Unifies UDF registration into one single class.
 */
public class UDFRegistrator {
    public static void registerUDFs(SparkSession spark){
        spark.udf().register("stringToPeriod", new StringToPeriodUDF(), new PeriodUDT());
    }
}
