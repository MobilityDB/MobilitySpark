package org.mobiltydb.UDF.General;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;


import java.sql.SQLException;
import java.time.Duration;

public class PeriodSetUDF {

    /***
     * Initialize PeriodSet from String.
     */
    public static UDF1<String, PeriodSet> periodset_in = PeriodSet::new;

    public static UDF2<PeriodSet, PeriodSet, PeriodSet> union_periodset = new UDF2<PeriodSet, PeriodSet, PeriodSet>() {

        @Override
        public PeriodSet call(PeriodSet periodSet, PeriodSet other) {
            return periodSet.union_PeriodSet(other);
        }

    };

    public static UDF2<PeriodSet, PeriodSet, PeriodSet> intersection_periodset = new UDF2<PeriodSet, PeriodSet, PeriodSet>() {
        @Override
        public PeriodSet call(PeriodSet periodSet, PeriodSet other) {
            return periodSet.intersection_Periodset(other);
        }
    };


    public static UDF2<PeriodSet, PeriodSet, PeriodSet> minus_periodset = new UDF2<PeriodSet, PeriodSet, PeriodSet>() {
        @Override
        public PeriodSet call(PeriodSet periodSet, PeriodSet other) {
            return periodSet.minus_PeriodSet(other);
        }
    };

    public static UDF2<PeriodSet, PeriodSet, Float> distance_periodset = new UDF2<PeriodSet, PeriodSet, Float>() {
        @Override
        public Float call(PeriodSet periodSet, PeriodSet other)  {
            return periodSet.distance_Periodset(other);
        }
    };

    public static UDF2<PeriodSet, PeriodSet, Boolean> isover_or_before_periodset = new UDF2<PeriodSet, PeriodSet, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, PeriodSet other) {
            return periodSet.isover_or_before_Periodset(other);
        }
    };

    public static UDF2<PeriodSet, PeriodSet, Boolean> isbefore_periodset = new UDF2<PeriodSet, PeriodSet, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, PeriodSet other) {
            return periodSet.isbefore_Periodset(other);
        }
    };


    public static UDF2<PeriodSet, PeriodSet, Boolean> isafter_periodset = new UDF2<PeriodSet, PeriodSet, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, PeriodSet other)  {
            return periodSet.isafter_Periodset(other);
        }
    };

    public static UDF2<PeriodSet, PeriodSet, Boolean> overlaps_periodset = new UDF2<PeriodSet, PeriodSet, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, PeriodSet other) {
            return periodSet.overlaps_Periodset(other);
        }
    };

    public static UDF2<PeriodSet, PeriodSet, Boolean> contains_periodset = new UDF2<PeriodSet, PeriodSet, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, PeriodSet other) {
            return periodSet.contains_Periodset(other);
        }
    };

    public static UDF2<PeriodSet, PeriodSet, Boolean> is_contained_periodset = new UDF2<PeriodSet, PeriodSet, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, PeriodSet other) {
            return periodSet.is_contained_Periodset(other);
        }
    };

    public static UDF2<PeriodSet, PeriodSet, Boolean> is_adjacent_Periodset = new UDF2<PeriodSet, PeriodSet, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, PeriodSet other) {
            return periodSet.is_adjacent_Periodset(other);
        }
    };

    public static UDF2<PeriodSet, Period, PeriodSet> union_period = new UDF2<PeriodSet, Period, PeriodSet>() {
        @Override
        public PeriodSet call(PeriodSet periodSet, Period other) {
            return periodSet.union_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, PeriodSet> minus_period = new UDF2<PeriodSet, Period, PeriodSet>() {
        @Override
        public PeriodSet call(PeriodSet periodSet, Period other) {
            return periodSet.minus_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, PeriodSet> intersection_period = new UDF2<PeriodSet, Period, PeriodSet>() {
        @Override
        public PeriodSet call(PeriodSet periodSet, Period other) throws SQLException {
            return periodSet.intersection_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, Float> distance_period = new UDF2<PeriodSet, Period, Float>() {
        @Override
        public Float call(PeriodSet periodSet, Period other) {
            return periodSet.distance_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> isover_or_before_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) {
            return periodSet.isover_or_before_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> isover_or_after_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) {
            return periodSet.isover_or_after_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> isbefore_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) {
            return periodSet.isbefore_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> isafter_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) {
            return periodSet.isafter_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> overlaps_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) {
            return periodSet.overlaps_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> contains_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) {
            return periodSet.contains_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> is_contained_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) {
            return periodSet.is_contained_Period(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> is_adjacent_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) {
            return periodSet.is_adjacent_Period(other);
        }
    };

    public static UDF2<PeriodSet, Duration, PeriodSet> shift = new UDF2<PeriodSet, Duration, PeriodSet>() {
        @Override
        public PeriodSet call(PeriodSet periodSet, Duration duration) throws Exception {
            return periodSet.shift(duration);
        }
    };
}
