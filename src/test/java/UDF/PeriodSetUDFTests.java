package UDF;

import jmeos.types.time.Period;
import jmeos.types.time.PeriodSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utils.SparkTestUtils;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.col;

public class PeriodSetUDFTests extends SparkTestUtils {

}
