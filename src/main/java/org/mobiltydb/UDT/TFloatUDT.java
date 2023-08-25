package org.mobiltydb.UDT;

import jmeos.types.basic.tfloat.TFloat;

import java.sql.SQLException;

public class TFloatUDT extends MeosDatatype<TFloat> {
    /**
     * Provides the Java class associated with this UDT.
     * @return The TFloat class type.
     */
    @Override
    public Class<TFloat> userClass() {
        return TFloat.class;
    }

    @Override
    protected TFloat fromString(String s) throws SQLException {
        return new TFloat(s);
    }
}
