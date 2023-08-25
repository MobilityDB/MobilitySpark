package org.mobiltydb.UDT;

import jmeos.types.basic.tfloat.TFloat;
import jmeos.types.basic.tpoint.tgeog.TGeogPoint;
import jmeos.types.boxes.TBox;
import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import jmeos.types.time.TimestampSet;

import java.lang.reflect.InvocationTargetException;

/**
 * Factory class for handling Meos data types.
 * Provides methods to get the canonical names of both the UDTs and the associated data types.
 */
public class MeosDatatypeFactory {

    /**
     * Enumeration representing the different Meos data types supported.
     */
    public enum MeosTypes {
        PERIOD(Period.class, PeriodUDT.class),
        PERIODSET(PeriodSet.class, PeriodSetUDT.class),
        TIMESTAMPSET(TimestampSet.class, TimestampSetUDT.class),
        TFLOAT(TFloat.class, TFloatUDT.class),
        TBOX(TBox.class, TBoxUDT.class),
        TGEOGPOINT(TGeogPoint.class, TGeogPointUDT.class);

        private final Class<?> meosClass;
        private final Class<?> sparkUdtClass;

        MeosTypes(Class<?> meosClass, Class<?> sparkUdtClass) {
            this.meosClass = meosClass;
            this.sparkUdtClass = sparkUdtClass;
        }

        public Class<?> getMeosClass() {
            return meosClass;
        }

        public Class<?> getSparkUdtClass() {
            return sparkUdtClass;
        }
    }

    /**
     * Returns the canonical name of the Spark UDT associated with the specified Meos type.
     *
     * @param type The Meos type for which the associated UDT's canonical name is required.
     * @return The canonical name of the associated UDT.
     * @throws IllegalArgumentException If the provided type is not recognized.
     */
    public static String getSparkMeosDatatypeClassname(MeosTypes type) {
        return type.getSparkUdtClass().getCanonicalName();
    }

    /**
     * Returns the canonical name of the Meos data type specified.
     *
     * @param type The Meos type for which the canonical name is required.
     * @return The canonical name of the specified Meos type.
     * @throws IllegalArgumentException If the provided type is not recognized.
     */
    public static String getMeosDatatypeClassname(MeosTypes type) {
        return type.getMeosClass().getCanonicalName();
    }

    public static MeosDatatype<?> createMeosDatatype(MeosTypes type) {
        try {
            return (MeosDatatype<?>) type.getSparkUdtClass().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException("Unable to instantiate type: " + type, e);
        }
    }

}
