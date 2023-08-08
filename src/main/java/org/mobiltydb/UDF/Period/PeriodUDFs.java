package org.mobiltydb.UDF.Period;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import jmeos.types.time.TimestampSet;
import jnr.ffi.Pointer;
import org.apache.arrow.flatbuf.Bool;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

import java.time.Duration;
import java.time.OffsetDateTime;

public class PeriodUDFs {
    public static UDF1<String, Period> stringToPeriod = new UDF1<String, Period>() {
        @Override
        public Period call(String s) throws Exception {
            return new Period(s);
        }
    };

    public static UDF1<String, Period> fromHexwkbUDF = new UDF1<String, Period>() {
        @Override
        public Period call(String s) throws Exception {
            return Period.from_hexwkb(s);
        }
    };

    public static UDF1<Period, Float> width = new UDF1<Period, Float>() {
        @Override
        public Float call(Period period) throws Exception {
            return period.width();
        }
    };

    public static UDF2<Period, Period, Period> expand = new UDF2<Period, Period, Period>() {
        @Override
        public Period call(Period period, Period other) throws Exception {
            return period.expand(other);
        }
    };

    public static UDF1<Period, PeriodSet> toPeriodSet = new UDF1<Period, PeriodSet>() {
        @Override
        public PeriodSet call(Period period) throws Exception {
            return period.to_periodset();
        }
    };

    public static UDF2<Period, Period, Boolean> isAdjacentPeriod = new UDF2<Period, Period, Boolean>() {
        @Override
        public Boolean call(Period period, Period other) throws Exception {
            return period.is_adjacent_Period(other);
        }
    };

    public static UDF2<Period, PeriodSet, Boolean> isAdjacentPeriodSet = new UDF2<Period, PeriodSet, Boolean>() {
        @Override
        public Boolean call(Period period, PeriodSet other) throws Exception {
            return period.is_adjacent_Periodset(other);
        }
    };

    public static UDF2<Period, Period, Boolean> isContainedInPeriod = new UDF2<Period, Period, Boolean>() {
        @Override
        public Boolean call(Period period, Period other) throws Exception {
            return period.is_contained_in_Period(other);
        }
    };

    public static UDF2<Period, PeriodSet, Boolean> isContainedInPeriodSet = new UDF2<Period, PeriodSet, Boolean>() {
        @Override
        public Boolean call(Period period, PeriodSet other) throws Exception {
            return period.is_contained_in_Periodset(other);
        }
    };

    public static UDF2<Period, Period, Boolean> containsPeriod = new UDF2<Period, Period, Boolean>() {
        @Override
        public Boolean call(Period period, Period other) throws Exception {
            return period.contains_Period(other);
        }
    };

    public static UDF2<Period, PeriodSet, Boolean> containsPeriodSet = new UDF2<Period, PeriodSet, Boolean>() {
        @Override
        public Boolean call(Period period, PeriodSet other) throws Exception {
            return period.contains_Periodset(other);
        }
    };

    public static UDF2<Period, Period, Boolean> overlapsPeriod = new UDF2<Period, Period, Boolean>() {
        @Override
        public Boolean call(Period period, Period other) throws Exception {
            return period.overlaps_Period(other);
        }
    };

    public static UDF2<Period, PeriodSet, Boolean> overlapsPeriodSet = new UDF2<Period, PeriodSet, Boolean>() {
        @Override
        public Boolean call(Period period, PeriodSet other) throws Exception {
            return period.overlaps_Periodset(other);
        }
    };

    public static UDF2<Period, TimestampSet, Boolean> overlapsTimestampSet = new UDF2<Period, TimestampSet, Boolean>() {
        @Override
        public Boolean call(Period period, TimestampSet other) throws Exception {
            return period.overlaps_timestampset(other);
        }
    };

    public static UDF2<Period, Period, Boolean> isAfterPeriod = new UDF2<Period, Period, Boolean>() {
        @Override
        public Boolean call(Period period, Period other) throws Exception {
            return period.isafter_Period(other);
        }
    };

    public static UDF2<Period, PeriodSet, Boolean> isAfterPeriodSet = new UDF2<Period, PeriodSet, Boolean>() {
        @Override
        public Boolean call(Period period, PeriodSet other) throws Exception {
            return period.isafter_Periodset(other);
        }
    };

    public static UDF2<Period, TimestampSet, Boolean> isAfterTimestampSet = new UDF2<Period, TimestampSet, Boolean>() {
        @Override
        public Boolean call(Period period, TimestampSet other) throws Exception {
            return period.isafter_timestampset(other);
        }
    };

    public static UDF2<Period, Period, Boolean> isBeforePeriod = new UDF2<Period, Period, Boolean>() {
        @Override
        public Boolean call(Period period, Period other) throws Exception {
            return period.isbefore_Period(other);
        }
    };

    public static UDF2<Period, Period, Boolean> isOverOrAfterPeriod = new UDF2<Period, Period, Boolean>() {
        @Override
        public Boolean call(Period period, Period other) throws Exception {
            return period.isover_or_after_Period(other);
        }
    };

    public static UDF2<Period, Period, Boolean> isOverOrBeforePeriod = new UDF2<Period, Period, Boolean>() {
        @Override
        public Boolean call(Period period, Period other) throws Exception {
            return period.isover_or_before_Period(other);
        }
    };

    public static UDF2<Period, Period, Float> distancePeriod = new UDF2<Period, Period, Float>() {
        @Override
        public Float call(Period period, Period other) throws Exception {
            return period.distance_Period(other);
        }
    };

    public static UDF2<Period, Period, Period> intersectionPeriod = new UDF2<Period, Period, Period>() {
        @Override
        public Period call(Period period, Period other) throws Exception {
            return period.intersection_Period(other);
        }
    };

    public static UDF1<Period, String> getValue = new UDF1<Period, String>() {
        @Override
        public String call(Period period) throws Exception {
            return period.getValue();
        }
    };

    public static UDF1<Period, OffsetDateTime> getLower = new UDF1<Period, OffsetDateTime>() {
        @Override
        public OffsetDateTime call(Period period) throws Exception {
            return period.getLower();
        }
    };

    public static UDF1<Period, OffsetDateTime> getUpper = new UDF1<Period, OffsetDateTime>() {
        @Override
        public OffsetDateTime call(Period period) throws Exception {
            return period.getUpper();
        }
    };

    public static UDF1<Period, Boolean> getUpperInc = new UDF1<Period, Boolean>() {
        @Override
        public Boolean call(Period period) throws Exception {
            return period.getUpper_inc();
        }
    };

    public static UDF1<Period, Boolean> getLowerInc = new UDF1<Period, Boolean>() {
        @Override
        public Boolean call(Period period) throws Exception {
            return period.getLower_inc();
        }
    };

    public static UDF1<Period, Boolean> isLowerInclusive = new UDF1<Period, Boolean>() {
        @Override
        public Boolean call(Period period) throws Exception {
            return period.isLowerInclusive();
        }
    };

    public static UDF1<Period, Boolean> isUpperInclusive = new UDF1<Period, Boolean>() {
        @Override
        public Boolean call(Period period) throws Exception {
            return period.isUpperInclusive();
        }
    };

    public UDF1<Period, Pointer> getInner = new UDF1<Period, Pointer>() {
        @Override
        public Pointer call(Period period) throws Exception {
            return period.get_inner();
        }
    };

    // Might change to generic function?
    public static UDF2<Period, Object, Boolean> periodEquals = new UDF2<Period, Object, Boolean>() {
        @Override
        public Boolean call(Period period, Object other) throws Exception {
            return period.equals(other);
        }
    };

    public static UDF1<Period, Integer> hashCode = new UDF1<Period, Integer>() {
        @Override
        public Integer call(Period period) throws Exception {
            return period.hashCode();
        }
    };

    public static UDF1<Period, Duration> duration = new UDF1<Period, Duration>() {
        @Override
        public Duration call(Period period) throws Exception {
            return period.duration();
        }
    };

    public static UDF2<Period, Duration, Period> shift = new UDF2<Period, Duration, Period>() {
        @Override
        public Period call(Period period, Duration duration) throws Exception {
            return period.shift(duration);
        }
    };

    public static UDF2<Period, OffsetDateTime, Boolean> contains = new UDF2<Period, OffsetDateTime, Boolean>() {
        @Override
        public Boolean call(Period period, OffsetDateTime offsetDateTime) throws Exception {
            return period.contains(offsetDateTime);
        }
    };

    public static UDF2<Period, Period, Boolean> overlap = new UDF2<Period, Period, Boolean>() {
        @Override
        public Boolean call(Period period, Period other) throws Exception {
            return period.overlap(other);
        }
    };
}
