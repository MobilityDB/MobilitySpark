package org.mobiltydb.UDT;

import jmeos.types.basic.tfloat.TFloatInst;
import jmeos.types.basic.tfloat.TFloatSeq;

import java.sql.SQLException;

public class TFloatSeqUDT extends MeosDatatype<TFloatSeq> {
    /**
     * Provides the Java class associated with this UDT.
     * @return The TFloatSeq class type.
     */
    @Override
    public Class<TFloatSeq> userClass() {
        return TFloatSeq.class;
    }

    @Override
    protected TFloatSeq fromString(String s) throws SQLException {
        return new TFloatSeq(s);
    }
}
