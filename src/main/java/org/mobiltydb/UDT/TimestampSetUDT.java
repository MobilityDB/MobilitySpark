package org.mobiltydb.UDT;

import jmeos.types.time.TimestampSet;
import utils.TypeName;

import java.sql.SQLException;

@TypeName(name="timestampset")
public class TimestampSetUDT extends MeosDatatype<TimestampSet> {

    @Override
    public Class<TimestampSet> userClass() {
        return TimestampSet.class;
    }

    @Override
    public TimestampSet fromString(String s) throws SQLException{
        return new TimestampSet(s);
    }
}
