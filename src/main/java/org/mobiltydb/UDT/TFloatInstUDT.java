package org.mobiltydb.UDT;

import jmeos.types.basic.tfloat.TFloat;
import jmeos.types.basic.tfloat.TFloatInst;

import java.sql.SQLException;

public class TFloatInstUDT extends MeosDatatype<TFloatInst> {
    /**
     * Provides the Java class associated with this UDT.
     * @return The TFloatInst class type.
     */
    @Override
    public Class<TFloatInst> userClass() {
        return TFloatInst.class;
    }

    @Override
    protected TFloatInst fromString(String s) throws SQLException {
        return new TFloatInst(s);
    }
}
