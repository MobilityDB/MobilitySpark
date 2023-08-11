package org.mobiltydb.UDT;

import jmeos.types.time.Period;
import jmeos.types.time.TimestampSet;
import org.apache.hadoop.yarn.util.Times;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import scala.collection.immutable.Seq;

import java.sql.SQLException;
import java.sql.Time;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimestampSetUDT extends UserDefinedType<TimestampSet> {

    private final OffsetDateTimeUDT offsetDateTimeUDT = new OffsetDateTimeUDT();

    @Override
    public DataType sqlType() {
        return new StructType().add(
                "timestamps", new ArrayType(offsetDateTimeUDT.sqlType(), false)
        );
    }

    @Override
    public TimestampSet deserialize(Object datum) {
        if (!(datum instanceof InternalRow)) {
            throw new IllegalArgumentException("Expected InternalRow, but got: " + datum.getClass().getSimpleName());
        }
        InternalRow row = (InternalRow) datum;
        ArrayData arrayData = row.getArray(0);
        List<OffsetDateTime> timestamps = new ArrayList<>();

        for (int i = 0; i < arrayData.numElements(); i++) {
            InternalRow internalRow = arrayData.getStruct(i, 2);
            OffsetDateTime offsetDateTime = offsetDateTimeUDT.deserialize(internalRow);
            timestamps.add(offsetDateTime);
        }

        try {
            return new TimestampSet(timestamps.toArray(new OffsetDateTime[0]));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    @Override
    public Object serialize(TimestampSet timestampSet) {
        if (timestampSet == null) {
            return null;
        }

        List<OffsetDateTime> timestamps = Arrays.asList(timestampSet.timestamps());
        Object[] serializedTimestamps = new Object[timestamps.size()];
        for (int i = 0; i < timestamps.size(); i++) {
            serializedTimestamps[i] = offsetDateTimeUDT.serialize(timestamps.get(i));
        }
        ArrayData arrayData = ArrayData.toArrayData(serializedTimestamps);

        // Wrap the array data in a row.
        return new GenericInternalRow(new Object[] { arrayData });
    }


    @Override
    public Class<TimestampSet> userClass() {
        return TimestampSet.class;
    }
}
