package org.mobiltydb.UDT;

import java.sql.SQLException;
import org.apache.spark.sql.types.*;

import types.collections.time.Period;

/**
 * Custom User Defined Type (UDT) for the Period data type.
 * This UDT allows for integration of the Period type within Spark's DataFrames and SQL operations.
 */
@SQLUserDefinedType(udt = PeriodUDT.class)
public class PeriodUDT extends MeosDatatype<Period> {
    /**
     * Provides the Java class associated with this UDT.
     * @return The Period class type.
     */
    @Override
    public Class<Period> userClass() {
        return Period.class;
    }
    @Override
    protected Period fromString(String s) throws SQLException{
        return new Period(s);
    }
}
