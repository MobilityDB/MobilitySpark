package org.mobiltydb.UDT;

import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import com.google.protobuf.Internal;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;

import jmeos.types.time.Period;

@SQLUserDefinedType(udt = PeriodUDT.class)
public class PeriodUDT extends UserDefinedType<Period>{
    private static final DataType SQL_TYPE = new StructType()
            .add("lower", DataTypes.TimestampType)
            .add("upper", DataTypes.TimestampType)
            .add("lowerInclusive", DataTypes.BooleanType)
            .add("upperInclusive", DataTypes.BooleanType);

    @Override
    public DataType sqlType(){
        return SQL_TYPE;
    }

//    @Override
//    public Row serialize(Period period){
//        return RowFactory.create(
//                period.getLower().toInstant().toEpochMilli(),
//                period.getUpper().toInstant().toEpochMilli(),
//                period.isLowerInclusive(),
//                period.isUpperInclusive());
//    }

    @Override
    public Object serialize(Period period){
        Object[] values = new Object[4];
        values[0] = period.getLower().toInstant().toEpochMilli();
        values[1] = period.getUpper().toInstant().toEpochMilli();
        values[2] = period.isLowerInclusive();
        values[3] = period.isUpperInclusive();
        return new GenericInternalRow(values);
    }

    @Override
    public Period deserialize(Object datum){
        if (!(datum instanceof InternalRow)) {
            throw new IllegalArgumentException("Expected InternalRow, but got: " + datum.getClass().getSimpleName());
        }

        InternalRow row = (InternalRow) datum;
        long lowerMillis = row.getLong(0);
        long upperMillis = row.getLong(1);
        boolean lowerInclusive = row.getBoolean(2);
        boolean upperInclusive = row.getBoolean(3);

        OffsetDateTime lower = OffsetDateTime.ofInstant(Instant.ofEpochMilli(lowerMillis), ZoneId.systemDefault());
        OffsetDateTime upper = OffsetDateTime.ofInstant(Instant.ofEpochMilli(upperMillis), ZoneId.systemDefault());

        try {
            return new Period(lower, upper, lowerInclusive, upperInclusive);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Class<Period> userClass(){
        return Period.class;
    }
}