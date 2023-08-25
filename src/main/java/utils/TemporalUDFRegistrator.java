package utils;

import org.apache.spark.sql.SparkSession;
import org.mobiltydb.UDF.Temporal.TBoxUDF;
import org.mobiltydb.UDF.Temporal.TFloatUDF;
import org.mobiltydb.UDF.Temporal.TGeogPointUDF;
import org.mobiltydb.UDT.TBoxUDT;
import org.mobiltydb.UDT.TFloatUDT;
import org.mobiltydb.UDT.TGeogPointUDT;

public class TemporalUDFRegistrator {
    public static void registerUDFs(SparkSession spark){
        spark.udf().register("stringToTFloat", TFloatUDF.stringToTFloat, new TFloatUDT());
        spark.udf().register("stringToTBox", TBoxUDF.stringToTBox, new TBoxUDT());
        spark.udf().register("stringToTGeogPoint", TGeogPointUDF.stringTGeogPoint, new TGeogPointUDT());
        spark.udf().register("tGeogPointIn", TGeogPointUDF.tGeogPointIn, new TGeogPointUDT());
        spark.udf().register("tFloatIn", TFloatUDF.tFloatIn, new TFloatUDT());
    }
}
