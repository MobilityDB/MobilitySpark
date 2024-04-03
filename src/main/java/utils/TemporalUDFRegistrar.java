package utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.mobiltydb.UDF.Temporal.TBoxUDF;
import org.mobiltydb.UDF.Temporal.TFloatUDF;
import org.mobiltydb.UDF.Temporal.TGeogPointUDF;
import org.mobiltydb.UDT.*;

import java.time.LocalDateTime;

public class TemporalUDFRegistrar {
    public static void registerUDFs(SparkSession spark){
        spark.udf().register("stringToTFloat", TFloatUDF.stringToTFloat, new TFloatUDT());
        spark.udf().register("stringToTBox", TBoxUDF.stringToTBox, new TBoxUDT());
        spark.udf().register("stringToTGeogPoint", TGeogPointUDF.stringTGeogPointInst, new TGeogPointInstUDT());
        spark.udf().register("tFloatIn", TFloatUDF.tFloatInstIn, new TFloatInstUDT());
        spark.udf().register("tGeogPointInstIn", TGeogPointUDF.tGeogPointInstIn, new TGeogPointInstUDT());
        spark.udf().register("tGeogPointSeqIn", TGeogPointUDF.tGeogPointSeqIn, new TGeogPointSeqUDT());
        spark.udf().register("tFloatSeqIn", TFloatUDF.tFloatSeqIn, new TFloatSeqUDT());
        spark.udf().register("tGeogPointSeqNumInstant", TGeogPointUDF.tGeogPointNumInstant, DataTypes.IntegerType);
        spark.udf().register("tGeogPointSeqStartTimestamp", TGeogPointUDF.tGeogPointSeqStartTimestamp, DataTypes.TimestampType);
        spark.udf().register("tFloatNumInstants", TFloatUDF.tFloatNumInstants, DataTypes.IntegerType);
    }
}
