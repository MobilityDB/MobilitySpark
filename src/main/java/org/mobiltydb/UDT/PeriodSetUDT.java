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


public class PeriodSetUDT extends UserDefinedType<PeriodSet> {

    @Override
    public StructType sqlType() {
        return new StructType().add(
                "value", new ArrayType(new PeriodUDT(),false)
        );
    }

    @Override
    public PeriodSet deserialize(Object datum) {
        if (datum instanceof InternalRow row) {
            Period[] periods = new Period[row.numFields()];
            for (int i = 0; i < periods.length; i++) {
                periods[i] = (Period) row.get(i, new PeriodUDT());
            }

            try {
                return new PeriodSet(periods);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public Object serialize(PeriodSet periodSet) {
        if (periodSet == null) {
            return null;
        }

        List<Period> periods = Arrays.stream(periodSet.periods()).toList();

        Object[] values = new Object[periods.size()];
        for(int i = 0; i < periods.size(); i++) {
            values[i] = periods.get(i);
        }

        return new GenericInternalRow(values);
    }

    @Override
    public Class<PeriodSet> userClass() {
        return PeriodSet.class;
    }
}
