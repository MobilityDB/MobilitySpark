package org.mobiltydb.UDF.Period;

import types.collections.time.PeriodSet;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.mobiltydb.UDF.General.PeriodSetUDF;
import org.mobiltydb.UDT.PeriodSetUDT;
import org.mobiltydb.UDT.PeriodUDT;


public class PeriodUDFRegistrator {
    /**
     * Registers the UDFs for the Period class to enable usage within SparkSQL.
     * @param spark: SparkSession
     */
    public static void registerAllUDFs(SparkSession spark) {
        // Register each UDF using spark.udf().register()
        spark.udf().register("stringToPeriod", PeriodUDFs.stringToPeriod, new PeriodUDT());
        spark.udf().register("periodFromHexwkb", PeriodUDFs.fromHexwkbUDF, new PeriodUDT());
        spark.udf().register("periodWidth", PeriodUDFs.width, DataTypes.FloatType);
        spark.udf().register("periodExpand", PeriodUDFs.expand, new PeriodUDT());
        spark.udf().register("periodToPeriodSet", PeriodUDFs.toPeriodSet, new PeriodSetUDT());
        spark.udf().register("periodIsAdjacentPeriod", PeriodUDFs.isAdjacentPeriod, DataTypes.BooleanType);
        spark.udf().register("periodIsContainedInPeriod", PeriodUDFs.isContainedInPeriod, DataTypes.BooleanType);
        spark.udf().register("periodContainsPeriod", PeriodUDFs.containsPeriod, DataTypes.BooleanType);
        spark.udf().register("periodOverlapsPeriod", PeriodUDFs.overlapsPeriod, DataTypes.BooleanType);
        spark.udf().register("periodIsAfterPeriod", PeriodUDFs.isAfterPeriod, DataTypes.BooleanType);
        spark.udf().register("periodIsBeforePeriod", PeriodUDFs.isBeforePeriod, DataTypes.BooleanType);
        spark.udf().register("periodIsOverOrAfterPeriod", PeriodUDFs.isOverOrAfterPeriod, DataTypes.BooleanType);
        spark.udf().register("periodIsOverOrBeforePeriod", PeriodUDFs.isOverOrBeforePeriod, DataTypes.BooleanType);
        spark.udf().register("periodDistancePeriod", PeriodUDFs.distancePeriod, DataTypes.FloatType);
        spark.udf().register("periodIntersectionPeriod", PeriodUDFs.intersectionPeriod, new PeriodUDT());
        spark.udf().register("periodGetValue", PeriodUDFs.getValue, DataTypes.StringType);
        spark.udf().register("periodGetLower", PeriodUDFs.getLower, new PeriodUDT());
        spark.udf().register("periodGetUpper", PeriodUDFs.getUpper, new PeriodUDT());
        spark.udf().register("periodGetUpperInc", PeriodUDFs.getUpperInc, DataTypes.BooleanType);
        spark.udf().register("periodGetLowerInc", PeriodUDFs.getLowerInc, DataTypes.BooleanType);
        spark.udf().register("periodIsLowerInclusive", PeriodUDFs.isLowerInclusive, DataTypes.BooleanType);
        spark.udf().register("periodIsUpperInclusive", PeriodUDFs.isUpperInclusive, DataTypes.BooleanType);
        //spark.udf().register("getInner", PeriodUDFs.getInner, new PeriodUDT());
        spark.udf().register("periodEquals", PeriodUDFs.periodEquals, DataTypes.BooleanType);
        spark.udf().register("periodHashCode", PeriodUDFs.hashCode, DataTypes.IntegerType);
        spark.udf().register("periodDuration", PeriodUDFs.duration, new PeriodUDT());
    }

}
