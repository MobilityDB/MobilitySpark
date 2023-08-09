package utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.UDTRegistration;
import org.mobiltydb.UDT.*;

import java.time.Period;

/**
 * Unifies UDT registration into one single class.
 */
public class UDTRegistrator {
    public static void registerUDTs(SparkSession spark){
        // Note: Use full canonical names to register jmeos classes. Otherwise, it won't detect registration.
        UDTRegistration.register(jmeos.types.time.Period.class.getCanonicalName(), PeriodUDT.class.getCanonicalName());
        UDTRegistration.register(jmeos.types.time.PeriodSet.class.getCanonicalName(), PeriodSetUDT.class.getCanonicalName());
    }
}
