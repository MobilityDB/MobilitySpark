package org.mobiltydb.UDT;

import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;

import jmeos.types.time.Period;

/**
 * Custom User Defined Type (UDT) for the Period data type.
 * This UDT allows for integration of the Period type within Spark's DataFrames and SQL operations.
 */
@SQLUserDefinedType(udt = PeriodUDT.class)
public class PeriodUDT extends UserDefinedType<Period> {

    // Definition of the SQL data structure for the Period type.
    private static final DataType SQL_TYPE = new StructType()
            .add("lower", DataTypes.TimestampType)
            .add("upper", DataTypes.TimestampType)
            .add("lowerInclusive", DataTypes.BooleanType)
            .add("upperInclusive", DataTypes.BooleanType);

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
    @Override
    public Object serialize(Period period) {
        Object[] values = new Object[4];
        values[0] = period.getLower().toInstant().toEpochMilli();
        values[1] = period.getUpper().toInstant().toEpochMilli();
        values[2] = period.isLowerInclusive();
        values[3] = period.isUpperInclusive();
        return new GenericInternalRow(values);
    }

    /**
     * Deserializes an object from a Spark InternalRow representation back into a Period object.
     * @param datum The object, expected to be of type InternalRow.
     * @return The deserialized Period object.
     */
    @Override
    public Period deserialize(Object datum) {
        // Ensure we're dealing with an InternalRow.
        if (!(datum instanceof InternalRow)) {
            throw new IllegalArgumentException("Expected InternalRow, but got: " + datum.getClass().getSimpleName());
        }

        InternalRow row = (InternalRow) datum;
        long lowerMillis = row.getLong(0);
        long upperMillis = row.getLong(1);
        boolean lowerInclusive = row.getBoolean(2);
        boolean upperInclusive = row.getBoolean(3);

        // Convert epoch millis into OffsetDateTime.
        OffsetDateTime lower = OffsetDateTime.ofInstant(Instant.ofEpochMilli(lowerMillis), ZoneId.systemDefault());
        OffsetDateTime upper = OffsetDateTime.ofInstant(Instant.ofEpochMilli(upperMillis), ZoneId.systemDefault());

        try {
            return new Period(lower, upper, lowerInclusive, upperInclusive);
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
