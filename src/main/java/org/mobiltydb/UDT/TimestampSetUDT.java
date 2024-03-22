package org.mobiltydb.UDT;

import types.collections.time.TimestampSet;

import java.sql.SQLException;

public class TimestampSetUDT extends MeosDatatype<TimestampSet> {

    @Override
    public Class<TimestampSet> userClass() {
        return TimestampSet.class;
    }

    @Override
    protected TimestampSet fromString(String s) throws SQLException{
        return new TimestampSet(s);
    }
}
