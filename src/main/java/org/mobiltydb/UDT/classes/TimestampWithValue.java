package org.mobiltydb.UDT.classes;


import java.io.Serializable;
import java.sql.Timestamp;

public class TimestampWithValue implements Serializable {
    private Timestamp timestamp;
    private double value;

    public TimestampWithValue(Timestamp timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }
}
