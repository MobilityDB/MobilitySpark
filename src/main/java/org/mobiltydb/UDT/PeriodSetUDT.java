package org.mobiltydb.UDT;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


public class PeriodSetUDT extends MeosDatatype<PeriodSet> {
    @Override
    public Class<PeriodSet> userClass() {
        return PeriodSet.class;
    }
    @Override
    protected PeriodSet fromString(String s) throws SQLException{
        return new PeriodSet(s);
    }
}
