package utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.mobiltydb.UDF.Period.*;
import org.mobiltydb.UDT.*;

/**
 * Unifies UDF registration into one single class.
 */
public class UDFRegistrator {
    public static void registerUDFs(SparkSession spark){
        spark.udf().register("stringToPeriod", PeriodUDFs.stringToPeriod, new PeriodUDT());
        spark.udf().register("periodFromHexwkb", PeriodUDFs.fromHexwkbUDF, new PeriodUDT());
        spark.udf().register("periodWidth", PeriodUDFs.width, DataTypes.FloatType);
        spark.udf().register("periodExpand", PeriodUDFs.expand, new PeriodUDT());
        spark.udf().register("isAdjacentPeriod", PeriodUDFs.isAdjacentPeriod, DataTypes.BooleanType);

        TemporalUDFRegistrar.registerUDFs(spark);
        PeriodSetUDFRegistrator.registerUDFs(spark);
    }
}
