package org.mobiltydb.UDT;

import types.basic.tpoint.tgeog.TGeogPointInst;

import java.sql.SQLException;

public class TGeogPointInstUDT extends MeosDatatype<TGeogPointInst> {
    @Override
    public Class<TGeogPointInst> userClass() {
        return TGeogPointInst.class;
    }

    @Override
    protected TGeogPointInst fromString(String s) throws SQLException {
        return new TGeogPointInst(s);
    }
}
