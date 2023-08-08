package utils;

import org.apache.spark.sql.SparkSession;
import org.mobiltydb.UDF.General.PeriodSetUDF;
import org.mobiltydb.UDT.PeriodSetUDT;

public class PeriodSetUDFRegistrator {
    public static void registerUDFs(SparkSession spark){
        spark.udf().register("periodset_in", PeriodSetUDF.periodset_in, new PeriodSetUDT());
    }
}
