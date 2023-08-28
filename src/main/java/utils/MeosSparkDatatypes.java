package utils;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import jmeos.types.time.TimestampSet;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.mobiltydb.UDT.*;
import org.postgresql.PGConnection;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.sql.Types;

public enum MeosSparkDatatypes {
    PERIOD(Period.class, PeriodUDT.class),
    PERIODSET(PeriodSet.class, PeriodSetUDT.class),
    TIMESTAMPSET(TimestampSet.class, TimestampSetUDT.class);

    private final Class<? extends PGobject> meosClass;
    private final Class<?> sparkUdtClass;

    MeosSparkDatatypes(Class<? extends PGobject> meosClass, Class<?> sparkUdtClass) {
        this.meosClass = meosClass;
        this.sparkUdtClass = sparkUdtClass;
    }

    public Class<?> getMeosClass() {
        return meosClass;
    }

    public Class<?> getSparkUdtClass() {
        return sparkUdtClass;
    }

    /**
     * Registers the type based on the TypeName annotation of the class
     *
     * @param connection - the PGConnection
     * @throws SQLException if any of the classes does not implement PGobject
     */
    public void registerType(MeosSparkDatatypes udtType, PGConnection connection) throws SQLException {
        Class<?> udtClass = udtType.getSparkUdtClass();
        Class<?> meosClass = udtType.getMeosClass();
        String typeName = udtClass.getAnnotation(TypeName.class).name();
        //System.out.println(typeName+ " "+meosClass);
        connection.addDataType(typeName, (Class<? extends PGobject>) meosClass);
    }

    public static MeosSparkDatatypes fromTypeName(String typeName){
        for (MeosSparkDatatypes type: values()){
            Class<?> udtClass = type.getSparkUdtClass();
            String name = udtClass.getAnnotation(TypeName.class).name();
            if (typeName.equals(name)){
                return type;
            }
        }
        return null;
    }

    public static MeosSparkDatatypes fromDataType(DataType dt) {
        for (MeosSparkDatatypes type : values()) {
            if (type.sparkUdtClass.isInstance(dt)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown DataType: " + dt.getClass().getSimpleName());
    }
}
