package org.mobiltydb.UDT;

import types.basic.tpoint.tgeog.TGeogPoint;
import types.basic.tpoint.tgeog.TGeogPointSeq;

import java.sql.SQLException;

public class TGeogPointSeqUDT extends MeosDatatype<TGeogPointSeq> {
    @Override
    public Class<TGeogPointSeq> userClass() {
        return TGeogPointSeq.class;
    }

    @Override
    protected TGeogPointSeq fromString(String s) throws SQLException {
        return new TGeogPointSeq(s);
    }
}
