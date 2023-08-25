package org.mobiltydb.UDT;

import jmeos.types.basic.tpoint.tgeog.TGeogPoint;

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
        return new TGeogPoint(s);
    }
}
