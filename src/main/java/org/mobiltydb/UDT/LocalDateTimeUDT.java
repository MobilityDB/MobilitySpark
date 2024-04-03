package org.mobiltydb.UDT;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Custom User Defined Type (UDT) for the OffsetDateTime data type.
 * This UDT allows for integration of the OffsetDateTime type within Spark's DataFrames and SQL operations.
 */
@SQLUserDefinedType(udt = LocalDateTimeUDT.class)
public class LocalDateTimeUDT extends UserDefinedType<LocalDateTime> {

    // Definition of the SQL data structure for the OffsetDateTime type.
    private static final DataType SQL_TYPE = new StructType()
            .add("dateTime", DataTypes.TimestampType, false)
            .add("offset", DataTypes.StringType, false);

    /**
     * Provides the SQL data type's schema corresponding to this UDT.
     * @return The SQL data type.
     */
    @Override
    public DataType sqlType() {
        return SQL_TYPE;
    }

    /**
     * Serializes an OffsetDateTime object into a Spark InternalRow representation.
     * @param localDateTime The OffsetDateTime object.
     * @return The serialized object as an InternalRow.
     */
    @Override
    public Object serialize(LocalDateTime localDateTime) {
        Object[] values = new Object[2];
        //values[0] = localDateTime.toInstant().toEpochMilli();
        //values[1] = UTF8String.fromString(localDateTime.getOffset().toString());
        return new GenericInternalRow(values);
    }

    /**
     * Deserializes an object from a Spark InternalRow representation back into an OffsetDateTime object.
     * @param datum The object, expected to be of type InternalRow.
     * @return The deserialized OffsetDateTime object.
     */
    @Override
    public LocalDateTime deserialize(Object datum) {
        // Ensure we're dealing with an InternalRow.
        if (!(datum instanceof InternalRow)) {
            throw new IllegalArgumentException("Expected InternalRow, but got: " + datum.getClass().getSimpleName());
        }

        InternalRow row = (InternalRow) datum;
        long dt = row.getLong(0);
        String offset = row.getString(1);

        //return Instant.ofEpochMilli(dt).atOffset(ZoneOffset.of(offset));
        return null;
    }

    /**
     * Provides the Java class associated with this UDT.
     * @return The OffsetDateTime class type.
     */
    @Override
    public Class<LocalDateTime> userClass() {
        return LocalDateTime.class;
    }
}
