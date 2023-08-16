package org.mobiltydb.UDT;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import jmeos.types.time.TimestampSet;
import org.apache.hadoop.yarn.util.Times;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import scala.collection.immutable.Seq;

import java.sql.SQLException;
import java.sql.Time;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimestampSetUDT extends MeosDatatype<TimestampSet> {

    @Override
    public Class<TimestampSet> userClass() {
        return TimestampSet.class;
    }

    @Override
    protected TimestampSet fromString(String s) throws SQLException{
        return new TimestampSet(s);
    }
}
