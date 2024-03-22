package org.mobiltydb.UDF.General;

import types.TemporalObject;
import types.collections.time.Period;
import types.collections.time.PeriodSet;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import types.collections.time.Time;


import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.Temporal;

public class PeriodSetUDF {

    /***
     * Initialize PeriodSet from String.
     */
    public static UDF1<String, PeriodSet> periodset_in = PeriodSet::new;

    public static UDF2<PeriodSet, Time, PeriodSet> union_periodset = new UDF2<PeriodSet, Time, PeriodSet>() {

        @Override
        public PeriodSet call(PeriodSet periodSet, Time other) {
            return periodSet.union(other);
        }

    };

    public static UDF2<PeriodSet, Time, Time> intersection_periodset = new UDF2<PeriodSet, Time, Time>() {
        @Override
        public Time call(PeriodSet periodSet, Time other) {
            return periodSet.intersection(other);
        }
    };


    public static UDF2<PeriodSet, Time, PeriodSet> minus_periodset = new UDF2<PeriodSet, Time, PeriodSet>() {
        @Override
        public PeriodSet call(PeriodSet periodSet, Time other) {
            return periodSet.minus(other);
        }
    };

    public static UDF2<PeriodSet, PeriodSet, Float> distance_periodset = new UDF2<PeriodSet, PeriodSet, Float>() {
        @Override
        public Float call(PeriodSet periodSet, PeriodSet other) throws Exception {
            return periodSet.distance(other);
        }
    };

    public static UDF2<PeriodSet, TemporalObject, Boolean> isover_or_before_periodset = new UDF2<PeriodSet, TemporalObject, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, TemporalObject other) throws Exception {
            return periodSet.is_over_or_before(other);
        }
    };

    public static UDF2<PeriodSet, TemporalObject, Boolean> isbefore_periodset = new UDF2<PeriodSet, TemporalObject, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, TemporalObject other) throws Exception {
            return periodSet.is_before(other);
        }
    };


    public static UDF2<PeriodSet, TemporalObject, Boolean> isafter_periodset = new UDF2<PeriodSet, TemporalObject, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, TemporalObject other) throws Exception {
            return periodSet.is_after(other);
        }
    };

    public static UDF2<PeriodSet, TemporalObject, Boolean> overlaps_periodset = new UDF2<PeriodSet, TemporalObject, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, TemporalObject other) throws Exception {
            return periodSet.overlaps(other);
        }
    };

    public static UDF2<PeriodSet, TemporalObject, Boolean> contains_periodset = new UDF2<PeriodSet, TemporalObject, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, TemporalObject other) throws Exception {
            return periodSet.contains(other);
        }
    };

    public static UDF2<PeriodSet, TemporalObject, Boolean> is_contained_periodset = new UDF2<PeriodSet, TemporalObject, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, TemporalObject other) throws Exception {
            return periodSet.is_contained_in(other);
        }
    };

    public static UDF2<PeriodSet, TemporalObject, Boolean> is_adjacent_Periodset = new UDF2<PeriodSet, TemporalObject, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, TemporalObject other) throws Exception {
            return periodSet.is_adjacent(other);
        }
    };

    public static UDF2<PeriodSet, Time, PeriodSet> union_period = new UDF2<PeriodSet, Time, PeriodSet>() {
        @Override
        public PeriodSet call(PeriodSet periodSet, Time other) {
            return periodSet.union(other);
        }
    };

    public static UDF2<PeriodSet, Time, PeriodSet> minus_period = new UDF2<PeriodSet, Time, PeriodSet>() {
        @Override
        public PeriodSet call(PeriodSet periodSet, Time other) {
            return periodSet.minus(other);
        }
    };

    public static UDF2<PeriodSet, Time, Time> intersection_period = new UDF2<PeriodSet, Time, Time>() {
        @Override
        public Time call(PeriodSet periodSet, Time other) throws SQLException {
            return periodSet.intersection(other);
        }
    };

    public static UDF2<PeriodSet, Period, Float> distance_period = new UDF2<PeriodSet, Period, Float>() {
        @Override
        public Float call(PeriodSet periodSet, Period other) throws Exception {
            return periodSet.distance(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> isover_or_before_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) throws Exception {
            return periodSet.is_over_or_before(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> isover_or_after_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) throws Exception {
            return periodSet.is_over_or_after(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> isbefore_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) throws Exception {
            return periodSet.is_before(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> isafter_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) throws Exception {
            return periodSet.is_after(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> overlaps_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) throws Exception {
            return periodSet.overlaps(other);
        }
    };

    public static UDF2<PeriodSet, Period, Boolean> contains_period = new UDF2<PeriodSet, Period, Boolean>() {
        @Override
        public Boolean call(PeriodSet periodSet, Period other) throws Exception {
            return periodSet.contains(other);
        }
    };

}
