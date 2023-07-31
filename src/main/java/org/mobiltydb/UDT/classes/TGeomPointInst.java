package org.mobiltydb.UDT.classes;


import java.io.Serializable;
import java.sql.Timestamp;

public class TGeomPointInst implements Serializable {
    private Timestamp timestamp;
    private double value;

    public TGeomPointInst(Timestamp timestamp, double value) {
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
