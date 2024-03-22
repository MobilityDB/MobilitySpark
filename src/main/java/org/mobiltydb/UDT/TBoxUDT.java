package org.mobiltydb.UDT;

import types.basic.tfloat.TFloat;
import types.boxes.TBox;

import java.sql.SQLException;

public class TBoxUDT extends MeosDatatype<TBox> {
    /**
     * Provides the Java class associated with this UDT.
     * @return The TBox class type.
     */
    @Override
    public Class<TBox> userClass() {
        return TBox.class;
    }

    @Override
    protected TBox fromString(String s) throws SQLException {
        return new TBox(s);
    }
}
