package utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.mobiltydb.UDF.General.PeriodSetUDF;
import org.mobiltydb.UDT.PeriodSetUDT;

public class PeriodSetUDFRegistrator {
    public static void registerUDFs(SparkSession spark){
        spark.udf().register("periodset_in", PeriodSetUDF.periodset_in, new PeriodSetUDT());
        spark.udf().register("union_periodset", PeriodSetUDF.union_periodset, new PeriodSetUDT());
        spark.udf().register("overlaps_periodset", PeriodSetUDF.overlaps_periodset, DataTypes.BooleanType);
        spark.udf().register("isover_or_before_periodset", PeriodSetUDF.isover_or_before_periodset, DataTypes.BooleanType);
    }
}
