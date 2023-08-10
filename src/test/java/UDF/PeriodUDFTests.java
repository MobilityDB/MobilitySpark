package UDF;

import jmeos.types.time.Period;
import org.apache.spark.sql.Encoders;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utils.SparkTestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * This script tests the PeriodUDT implemented UDFs.
 * Note: Since the behavior of JMEOS and MEOS functions is tested there, these tests will only cover the
 * proper work of the function call in Spark SQL.
 */
public class PeriodUDFTests extends SparkTestUtils {
    @Test
    public void testStringToPeriod(){
        List<String> data = Arrays.asList("[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02]");
        Dataset<Row> df = spark.createDataset(data, Encoders.STRING()).toDF("stringPeriod");
        // Equivalent to spark.sql('SELECT stringToPeriod(period) FROM stringPeriod')
        df = df.withColumn("period", call_udf("stringToPeriod", col("stringPeriod")));
        Row row = df.collectAsList().get(0);
        Period result = (Period) row.getAs("period");
        Assertions.assertEquals("[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02]", result.toString());
    }

    @Test
    public void testFromHexwkb() throws SQLException {
        Dataset<Row> df = spark.sql("SELECT periodFromHexwkb('012100000040021FFE3402000000B15A26350200') as period");
        Row row = df.collectAsList().get(0);
        Period result = (Period) row.getAs("period");
        Assertions.assertEquals("(2019-09-08 02:00:00+02, 2019-09-10 02:00:00+02)", result.toString());
    }

    @Test
    public void testWidth() throws SQLException {

    }

    @Test
    public void testExpand() throws SQLException {

    }
}
