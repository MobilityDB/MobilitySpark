package UDT;

import org.mobiltydb.UDT.OffsetDateTimeUDT;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utils.SparkTestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class OffsetDatetimeUDTTests extends SparkTestUtils {

    @Test
    public void testStringToOffsetDateTime() {
        List<String> data = Arrays.asList("2023-08-07T14:10:49+02:00");
        Dataset<Row> df = spark.createDataset(data, Encoders.STRING()).toDF("stringOffsetDateTime");

        // Register the UDT and UDF
        spark.udf().register("stringToOffsetDateTime", (String s) -> OffsetDateTime.parse(s), new OffsetDateTimeUDT());

        // Equivalent to spark.sql('SELECT stringToOffsetDateTime(offsetDateTime) FROM stringOffsetDateTime')
        df = df.withColumn("offsetDateTime", call_udf("stringToOffsetDateTime", col("stringOffsetDateTime")));
        Row row = df.collectAsList().get(0);
        OffsetDateTime result = (OffsetDateTime) row.getAs("offsetDateTime");
        Assertions.assertEquals("2023-08-07T14:10:49+02:00", result.toString());
    }
}
