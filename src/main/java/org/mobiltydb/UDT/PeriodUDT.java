package org.mobiltydb.UDT;

import java.sql.SQLException;

import jmeos.types.core.DataType;
import org.apache.spark.sql.types.*;

import jmeos.types.time.Period;
import utils.TypeName;

/**
 * Custom User Defined Type (UDT) for the Period data type.
 * This UDT allows for integration of the Period type within Spark's DataFrames and SQL operations.
 */
@SQLUserDefinedType(udt = PeriodUDT.class)
@TypeName(name="period")
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
    public Period fromString(String s) throws SQLException{
        System.out.println(s);
        return new Period(s);
    }

}
