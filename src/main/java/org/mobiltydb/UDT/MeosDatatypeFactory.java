package org.mobiltydb.UDT;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import jmeos.types.time.TimestampSet;

/**
 * Factory class for handling Meos data types.
 * Provides methods to get the canonical names of both the UDTs and the associated data types.
 */
public class MeosDatatypeFactory {

    /**
     * Enumeration representing the different Meos data types supported.
     */
    public enum MeosTypes{
        PERIOD,
        PERIODSET,
        TIMESTAMPSET
    }

    /**
     * Returns the canonical name of the Spark UDT associated with the specified Meos type.
     *
     * @param type The Meos type for which the associated UDT's canonical name is required.
     * @return The canonical name of the associated UDT.
     * @throws IllegalArgumentException If the provided type is not recognized.
     */
    public static String loadMeosSparkDatatype(MeosTypes type){
        switch (type){
            case PERIOD: return PeriodUDT.class.getCanonicalName();
            case PERIODSET: return PeriodSetUDT.class.getCanonicalName();
            case TIMESTAMPSET: return TimestampSetUDT.class.getCanonicalName();
            default: throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * Returns the canonical name of the Meos data type specified.
     *
     * @param type The Meos type for which the canonical name is required.
     * @return The canonical name of the specified Meos type.
     * @throws IllegalArgumentException If the provided type is not recognized.
     */
    public static String loadMeosDatatype(MeosTypes type){
        switch (type){
            case PERIOD: return Period.class.getCanonicalName();
            case PERIODSET: return PeriodSet.class.getCanonicalName();
            case TIMESTAMPSET: return TimestampSet.class.getCanonicalName();
            default: throw new IllegalArgumentException("Unknown type: " + type);
        }
    }
}
