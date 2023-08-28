package org.mobiltydb.UDT;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import jmeos.types.time.TimestampSet;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.postgresql.util.PGobject;
import scala.None$;
import scala.Option;
import scala.None$;
import scala.Some;

import java.lang.reflect.InvocationTargetException;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

/**
 * Factory class for handling Meos data types.
 * Provides methods to get the canonical names of both the UDTs and the associated data types.
 */
public class MeosDatatypeFactory {

    /**
     * Enumeration representing the different Meos data types supported.
     */
    public enum MeosTypes {
        PERIOD(Period.class, PeriodUDT.class, new JdbcType("period", Types.OTHER)),
        PERIODSET(PeriodSet.class, PeriodSetUDT.class, new JdbcType("periodset", Types.OTHER)),
        TIMESTAMPSET(TimestampSet.class, TimestampSetUDT.class, new JdbcType("periodset", Types.OTHER));

        private final Class<?> meosClass;
        private final Class<?> sparkUdtClass;
        private final JdbcType jdbcType;

        MeosTypes(Class<?> meosClass, Class<?> sparkUdtClass, JdbcType jdbcType) {
            this.meosClass = meosClass;
            this.sparkUdtClass = sparkUdtClass;
            this.jdbcType = jdbcType;
        }

        public Class<?> getMeosClass() {
            return meosClass;
        }

        public Class<?> getSparkUdtClass() {
            return sparkUdtClass;
        }

        public JdbcType getJdbcType() { return jdbcType; }

        public static Option<JdbcType> getJDBCDatatypeFor(DataType dt){
            for (MeosTypes type: values()){
                if(type.sparkUdtClass.isInstance(dt)){
                    System.out.println(dt);
                    return Option.apply(type.jdbcType);  // Convert the JDBC type to a Scala Option
                }
            }
            return Option.empty();  // Cast to the expected type
        }

        public PGobject createPGObjectInstance() {
            PGobject obj = new PGobject() {};
            try {
                obj.setType(this.jdbcType.databaseTypeDefinition());
            } catch (Exception e) {
                throw new RuntimeException("Failed to set type for PGobject", e);
            }
            return obj;
        }

        public static MeosTypes fromDataType(DataType dt) {
            for (MeosTypes type : values()) {
                if (type.sparkUdtClass.isInstance(dt)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown DataType: " + dt.getClass().getSimpleName());
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

    public static JdbcType getJDBCDatatype(MeosTypes type){
        return type.getJdbcType();
    }

}
