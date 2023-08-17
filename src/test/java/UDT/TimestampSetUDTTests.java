package UDT;

import jmeos.types.time.TimestampSet;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mobiltydb.UDT.OffsetDateTimeUDT;
import org.mobiltydb.UDT.PeriodUDT;
import org.mobiltydb.UDT.TimestampSetUDT;
import utils.SparkTestUtils;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class TimestampSetUDTTests extends SparkTestUtils {

    @Test
    public void testStringToTimestampSet() throws SQLException {

        OffsetDateTime odt1 = OffsetDateTime.parse("2023-08-07T14:10:49+02:00");
        OffsetDateTime odt2 = OffsetDateTime.parse("2023-08-07T15:10:49+02:00");

        OffsetDateTime[] odts = new OffsetDateTime[2];
        odts[0] = odt1;
        odts[1] = odt2;

        TimestampSet tss = new TimestampSet(odts);

        List<Row> data = Arrays.asList(
                RowFactory.create(tss)
        );

        StructType schema = new StructType()
                .add("timestampSet", new TimestampSetUDT());

        Dataset<Row> df = spark.createDataFrame(data, schema);

        System.out.println("Schema:");
        df.printSchema();

        System.out.println("DataFrame contents:");
        df.show(false);

        Row row = df.collectAsList().get(0);
        TimestampSet result = (TimestampSet) row.getAs("timestampSet");

        Assertions.assertEquals("{2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02}", result.toString());
    }
}
