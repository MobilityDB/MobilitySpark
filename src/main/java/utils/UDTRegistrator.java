package utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.UDTRegistration;
import org.mobiltydb.UDT.*;


/**
 * Unifies UDT registration into one single class.
 */
public class UDTRegistrator {
    public static void registerUDTs(SparkSession spark){
        for (MeosDatatypeFactory.MeosTypes type: MeosDatatypeFactory.MeosTypes.values()){
            String externalClassCanonicalName = MeosDatatypeFactory.loadMeosDatatype(type);
            String udtClassCanonicalName = MeosDatatypeFactory.loadMeosSparkDatatype(type);
            UDTRegistration.register(externalClassCanonicalName, udtClassCanonicalName);
        }
    }
}
