package org.mobiltydb.UDT;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class PeriodSetUDT extends UserDefinedType<PeriodSet> {
    private final PeriodUDT periodUDT = new PeriodUDT();

    @Override
    public DataType sqlType() {
        return new StructType().add(
                "value", new ArrayType(periodUDT.sqlType(),false)
        );
    }

    @Override
    public PeriodSet deserialize(Object datum) {
        if (!(datum instanceof InternalRow row)) {
            throw new IllegalArgumentException("Expected InternalRow, but got: " + datum.getClass().getSimpleName());
        }
        ArrayData arrayData = row.getArray(0);
        List<Period> periods = new ArrayList<>();

        for (int i = 0; i < arrayData.numElements(); i++) {
            InternalRow internalRow = arrayData.getStruct(i, 2);
            Period period = periodUDT.deserialize(internalRow);
            periods.add(period);
        }

        try {
            return new PeriodSet(periods.toArray(new Period[0]));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Object serialize(PeriodSet periodSet) {
        if (periodSet == null) {
            return null;
        }

        List<Period> periods = Arrays.stream(periodSet.periods()).toList();

        Object[] values = new Object[periods.size()];
        for(int i = 0; i < periods.size(); i++) {
            values[i] = periodUDT.serialize(periods.get(i));
        }

        ArrayData arrayData = ArrayData.toArrayData(values);

        // Wrap the array data in a row.
        return new GenericInternalRow(new Object[] { arrayData });
    }

    @Override
    public Class<PeriodSet> userClass() {
        return PeriodSet.class;
    }
}
