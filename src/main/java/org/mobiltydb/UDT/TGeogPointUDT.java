package org.mobiltydb.UDT;

import types.basic.tpoint.tgeog.TGeogPoint;
import types.basic.tpoint.tgeog.TGeogPointInst;

import java.sql.SQLException;

public class TGeogPointUDT extends MeosDatatype<TGeogPoint> {    /**
     * Provides the Java class associated with this UDT.
     * @return The TFloat class type.
     */
    @Override
    public Class<TGeogPoint> userClass() {
        return TGeogPoint.class;
    }

    @Override
    protected TGeogPoint fromString(String s) throws SQLException {
        return new TGeogPointInst(s);
    }
}
