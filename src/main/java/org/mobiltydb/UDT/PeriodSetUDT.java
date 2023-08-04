package org.mobiltydb.UDT;

import jmeos.types.time.PeriodSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.sql.SQLException;


public class PeriodSetUDT extends UserDefinedType<PeriodSet> {

    @Override
    public StructType sqlType() {
        return DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, false)
        });
    }

    @Override
    public PeriodSet deserialize(Object datum) {
        if (datum instanceof InternalRow) {
            InternalRow row = (InternalRow) datum;
            try {
                return new PeriodSet((String) row.get(0, DataTypes.StringType));
            } catch (SQLException exception) {
                return null;
            }

        }
        return null;
    }
    @Override
    public Object serialize(PeriodSet periodSet) {
        if (periodSet == null) {
            return null;
        }

        return new GenericInternalRow(new Object[] {periodSet.getValue()});
    }

    @Override
    public Class<PeriodSet> userClass() {
        return PeriodSet.class;
    }
}
