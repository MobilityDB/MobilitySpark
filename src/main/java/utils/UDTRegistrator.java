package utils;

import jmeos.types.time.PeriodSet;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.UDTRegistration;
import org.mobiltydb.UDT.*;


/**
 * Unifies UDT registration into one single class.
 */
public class UDTRegistrator {
    public static void registerUDTs(SparkSession spark){
        for (MeosSparkDatatypes type: MeosSparkDatatypes.values()){
            String externalClassCanonicalName = type.getMeosClass().getCanonicalName();
            String udtClassCanonicalName = type.getSparkUdtClass().getCanonicalName();
            UDTRegistration.register(externalClassCanonicalName, udtClassCanonicalName);
        }
    }
}
