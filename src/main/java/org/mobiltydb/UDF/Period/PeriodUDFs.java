package org.mobiltydb.UDF.Period;

import types.TemporalObject;
import types.collections.time.Period;
import types.collections.time.PeriodSet;
import types.collections.time.Time;
import types.collections.time.TimestampSet;
import jnr.ffi.Pointer;
import org.apache.arrow.flatbuf.Bool;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.temporal.Temporal;

public class PeriodUDFs {
    /**
     * UDF to convert a Period to a String.
     */
    public static UDF1<String, Period> stringToPeriod = new UDF1<String, Period>() {
        @Override
        public Period call(String s) throws Exception {
            return new Period(s);
        }
    };

    /**
     * Converts a String representation of Hexwkb into Period.
     */
    public static UDF1<String, Period> fromHexwkbUDF = new UDF1<String, Period>() {
        @Override
        public Period call(String s) throws Exception {
            return Period.from_hexwkb(s);
        }
    };

    /**
     * Calculates the width of a Period.
     */
    public static UDF1<Period, Float> width = new UDF1<Period, Float>() {
        @Override
        public Float call(Period period) throws Exception {
            return period.width();
        }
    };

    /**
     * Given two periods, expands the first into the other.
     */
    public static UDF2<Period, Period, Period> expand = new UDF2<Period, Period, Period>() {
        @Override
        public Period call(Period period, Period other) throws Exception {
            return period.expand(other);
        }
    };

    /**
     * Converts a Period into a PeriodSet object.
     */
    public static UDF1<Period, PeriodSet> toPeriodSet = new UDF1<Period, PeriodSet>() {
        @Override
        public PeriodSet call(Period period) throws Exception {
            return period.to_periodset();
        }
    };

    /**
     * Determines if two periods are adjacent.
     */
    public static UDF2<Period, TemporalObject, Boolean> isAdjacentPeriod = new UDF2<Period, TemporalObject, Boolean>() {
        @Override
        public Boolean call(Period period, TemporalObject other) throws Exception {
            return period.is_adjacent(other);
        }
    };


    /**
     * Determines if a Period is contained inside a second Period object.
     */
    public static UDF2<Period, TemporalObject, Boolean> isContainedInPeriod = new UDF2<Period, TemporalObject, Boolean>() {
        @Override
        public Boolean call(Period period, TemporalObject other) throws Exception {
            return period.is_contained_in(other);
        }
    };


    /**
     * Determines if a Period contains a second Period object.
     */
    public static UDF2<Period, TemporalObject, Boolean> containsPeriod = new UDF2<Period, TemporalObject, Boolean>() {
        @Override
        public Boolean call(Period period, TemporalObject other) throws Exception {
            return period.contains(other);
        }
    };

    /**
     * Determines if a first Period overlaps a second Period.
     */
    public static UDF2<Period, TemporalObject, Boolean> overlapsPeriod = new UDF2<Period, TemporalObject, Boolean>() {
        @Override
        public Boolean call(Period period, TemporalObject other) throws Exception {
            return period.overlaps(other);
        }
    };


    /**
     * Determines if a first Period is after a second Period.
     */
    public static UDF2<Period, TemporalObject, Boolean> isAfterPeriod = new UDF2<Period, TemporalObject, Boolean>() {
        @Override
        public Boolean call(Period period, TemporalObject other) throws Exception {
            return period.is_after(other);
        }
    };


    /**
     * Determines if a Period is before a second Period.
     */
    public static UDF2<Period, TemporalObject, Boolean> isBeforePeriod = new UDF2<Period, TemporalObject, Boolean>() {
        @Override
        public Boolean call(Period period, TemporalObject other) throws Exception {
            return period.is_before(other);
        }
    };

    /**
     * Determines if a Period is over or after a second Period.
     */
    public static UDF2<Period, TemporalObject, Boolean> isOverOrAfterPeriod = new UDF2<Period, TemporalObject, Boolean>() {
        @Override
        public Boolean call(Period period, TemporalObject other) throws Exception {
            return period.is_over_or_after(other);
        }
    };

    /**
     * Determines if a Period is over or before a second Period.
     */
    public static UDF2<Period, TemporalObject, Boolean> isOverOrBeforePeriod = new UDF2<Period, TemporalObject, Boolean>() {
        @Override
        public Boolean call(Period period, TemporalObject other) throws Exception {
            return period.is_over_or_before(other);
        }
    };

    /**
     * Determines the distance between two Periods.
     */
    public static UDF2<Period, TemporalObject, Double> distancePeriod = new UDF2<Period, TemporalObject, Double>() {
        @Override
        public Double call(Period period, TemporalObject other) throws Exception {
            return period.distance(other);
        }
    };

    /**
     * Determines the intersection Period between Period A and Period B.
     */
    public static UDF2<Period, TemporalObject, Time> intersectionPeriod = new UDF2<Period, TemporalObject, Time>() {
        @Override
        public Time call(Period period, TemporalObject other) throws Exception {
            return period.intersection(other);
        }
    };

    /**
     * Returns the String representation of the Period.
     */
    public static UDF1<Period, String> getValue = new UDF1<Period, String>() {
        @Override
        public String call(Period period) throws Exception {
            return period.toString();
        }
    };


    /**
     * Returns the lower bound of the Period.
     */
    public static UDF1<Period, LocalDateTime> getLower = new UDF1<Period, LocalDateTime>() {
        @Override
        public LocalDateTime call(Period period) throws Exception {
            return period.lower();
        }
    };

    /**
     * Returns the upper bound of the Period.
     */
    public static UDF1<Period, LocalDateTime> getUpper = new UDF1<Period, LocalDateTime>() {
        @Override
        public LocalDateTime call(Period period) throws Exception {
            return period.upper();
        }
    };

    /**
     * Returns True if upper bound is inclusive.
     */
    public static UDF1<Period, Boolean> getUpperInc = new UDF1<Period, Boolean>() {
        @Override
        public Boolean call(Period period) throws Exception {
            return period.upper_inc();
        }
    };

    /**
     * Returns True if lower bound is inclusive.
     */
    public static UDF1<Period, Boolean> getLowerInc = new UDF1<Period, Boolean>() {
        @Override
        public Boolean call(Period period) throws Exception {
            return period.lower_inc();
        }
    };

    /**
     * Returns True if Period is lower inclusive.
     */
    public static UDF1<Period, Boolean> isLowerInclusive = new UDF1<Period, Boolean>() {
        @Override
        public Boolean call(Period period) throws Exception {
            return period.lower_inc();
        }
    };

    /**
     * Returns True if Period is upper inclusive.
     */
    public static UDF1<Period, Boolean> isUpperInclusive = new UDF1<Period, Boolean>() {
        @Override
        public Boolean call(Period period) throws Exception {
            return period.upper_inc();
        }
    };

    /**
     * Returns the _inner representation of the Period (a jts Pointer).
     */
    public UDF1<Period, Pointer> getInner = new UDF1<Period, Pointer>() {
        @Override
        public Pointer call(Period period) throws Exception {
            return period.get_inner();
        }
    };

    /**
     * Returns True if two Periods are equal.
     */
    public static UDF2<Period, Object, Boolean> periodEquals = new UDF2<Period, Object, Boolean>() {
        @Override
        public Boolean call(Period period, Object other) throws Exception {
            return period.equals(other);
        }
    };

    /**
     * Returns the hashCode (an Integer) of the Period.
     */
    public static UDF1<Period, Integer> hashCode = new UDF1<Period, Integer>() {
        @Override
        public Integer call(Period period) throws Exception {
            return period.hashCode();
        }
    };

    /**
     * Calculates the duration of the Period as a java Duration object.
     */
    public static UDF1<Period, Duration> duration = new UDF1<Period, Duration>() {
        @Override
        public Duration call(Period period) throws Exception {
            return period.duration();
        }
    };
}

