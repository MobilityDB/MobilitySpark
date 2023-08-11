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
    @Test
    public void testPeriodSetIn() {
        List<String> data = Arrays.asList("{[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02], [2023-08-07 16:10:49+02, 2023-08-07 17:10:49+02], [2023-08-07 18:10:49+02, 2023-08-07 19:10:49+02]}");
        Dataset<Row> df = spark.createDataset(data, Encoders.STRING()).toDF("periodSetString");
        df = df.withColumn("periodSet", call_udf("periodset_in", col("periodSetString")));
        Row row = df.collectAsList().get(0);
        PeriodSet result = (PeriodSet) row.getAs("periodSet");
        Assertions.assertEquals("{[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02], [2023-08-07 16:10:49+02, 2023-08-07 17:10:49+02], [2023-08-07 18:10:49+02, 2023-08-07 19:10:49+02]}", result.getValue());
    }
}
