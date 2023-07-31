package org.mobiltydb.UDT;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.mobiltydb.UDT.classes.TGeomPointInst;

import java.sql.Timestamp;


public class TGeomPointInstUDT extends UserDefinedType<TGeomPointInst> {

    @Override
    public StructType sqlType() {
        // Define the schema of your TemporalPoint class here
        return DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("timestamp", DataTypes.TimestampType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });
    }

    @Override
    public TGeomPointInst deserialize(Object datum) {
        if (datum instanceof InternalRow) {
            InternalRow row = (InternalRow) datum;
            Double value = row.getDouble(1);
            java.sql.Timestamp timestamp = (java.sql.Timestamp) row.get(0, DataTypes.TimestampType);
            return new TGeomPointInst(timestamp, value);
        }
        return null;
    }
    @Override
    public Object serialize(TGeomPointInst point) {
        if (point == null) {
            return null;
        }
        // Convert your TemporalPoint instance to an InternalRow
        return new GenericInternalRow(new Object[] {point.getTimestamp(), point.getValue()});
    }

    @Override
    public Class<TGeomPointInst> userClass() {
        return TGeomPointInst.class;
    }
}