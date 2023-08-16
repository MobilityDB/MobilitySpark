package org.mobiltydb.UDT;

import java.io.*;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import jnr.ffi.Pointer;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;

import jmeos.types.time.Period;
import org.apache.spark.unsafe.types.ByteArray;
import org.objenesis.strategy.StdInstantiatorStrategy;
import utils.PointerSerializer;

import javax.xml.crypto.Data;

/**
 * Custom User Defined Type (UDT) for the Period data type.
 * This UDT allows for integration of the Period type within Spark's DataFrames and SQL operations.
 */
@SQLUserDefinedType(udt = PeriodUDT.class)
public class PeriodUDT extends UserDefinedType<Period> {

    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.register(Period.class);
        kryo.register(Pointer.class, new PointerSerializer(), 10);
        kryo.register(String.class);
        return kryo;
    });


    // Definition of the SQL data structure for the Period type.
//    private static final DataType SQL_TYPE = new StructType()
//            .add("lower", DataTypes.TimestampType)
//            .add("upper", DataTypes.TimestampType)
//            .add("lowerInclusive", DataTypes.BooleanType)
//            .add("upperInclusive", DataTypes.BooleanType);
    private static final DataType SQL_TYPE = DataTypes.BinaryType;

    /**
     * Provides the SQL data type's schema corresponding to this UDT.
     * @return The SQL data type.
     */
    @Override
    public DataType sqlType() {
        return SQL_TYPE;
    }

    /**
     * Serializes a Period object into a Spark InternalRow representation.
     * @param period The Period object.
     * @return The serialized object as an InternalRow.
     */
//    @Override
//    public Object serialize(Period period) {
//        Object[] values = new Object[4];
//        values[0] = period.getLower().toInstant().toEpochMilli();
//        values[1] = period.getUpper().toInstant().toEpochMilli();
//        values[2] = period.isLowerInclusive();
//        values[3] = period.isUpperInclusive();
//        return new GenericInternalRow(values);
//    }
    @Override
    public Object serialize(Period period){
        Kryo kryo = kryoThreadLocal.get();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        String periodString = period.toString();
        kryo.writeObject(output, periodString);
        output.close();
        byte[] bytes = baos.toByteArray();
        Object[] values = new Object[1];
        values[0] = bytes;
        System.out.println("Serialize " + period.toString() + " bits: " + Arrays.toString(bytes));
        GenericInternalRow row = new GenericInternalRow(values);
        return bytes;
    }

    /**
     * Deserializes an object from a Spark InternalRow representation back into a Period object.
     * @param datum The object, expected to be of type InternalRow.
     * @return The deserialized Period object.
     */
//    @Override
//    public Period deserialize(Object datum) {
//        // Ensure we're dealing with an InternalRow.
//        if (!(datum instanceof InternalRow)) {
//            throw new IllegalArgumentException("Expected InternalRow, but got: " + datum.getClass().getSimpleName());
//        }
//
//        InternalRow row = (InternalRow) datum;
//        long lowerMillis = row.getLong(0);
//        long upperMillis = row.getLong(1);
//        boolean lowerInclusive = row.getBoolean(2);
//        boolean upperInclusive = row.getBoolean(3);
//
//        // Convert epoch millis into OffsetDateTime.
//        OffsetDateTime lower = OffsetDateTime.ofInstant(Instant.ofEpochMilli(lowerMillis), ZoneId.systemDefault());
//        OffsetDateTime upper = OffsetDateTime.ofInstant(Instant.ofEpochMilli(upperMillis), ZoneId.systemDefault());
//
//        try {
//            return new Period(lower, upper, lowerInclusive, upperInclusive);
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    @Override
    public Period deserialize(Object datum){
        if (!(datum instanceof byte[] bytes)) {
            throw new IllegalArgumentException("Expected byte[], but got: " + datum.getClass().getSimpleName());
        }
        Kryo kryo = kryoThreadLocal.get();
        Input input = new Input(new ByteArrayInputStream(bytes));
        String periodString = kryo.readObject(input, String.class);
        input.close();
        System.out.println("Deserialize " + periodString + " bits: " + Arrays.toString(bytes));
        try {
            return new Period(periodString);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Provides the Java class associated with this UDT.
     * @return The Period class type.
     */
    @Override
    public Class<Period> userClass() {
        return Period.class;
    }
}
