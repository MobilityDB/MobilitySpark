package utils;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.mobiltydb.UDF.Period.PeriodUDFRegistrator;

import static jmeos.functions.functions.meos_finalize;
import static jmeos.functions.functions.meos_initialize;

public class SparkTestUtils {

    public static SparkSession spark;

    @BeforeAll
    public static void setUpSparkSession() {
        meos_initialize("UTC");
        spark = SparkSession
                .builder()
                .appName("SparkMEOS Testing Suite")
                .config("spark.master", "local")
                .getOrCreate();
        UDTRegistrator.registerUDTs(spark);
        //UDFRegistrator.registerUDFs(spark);
        PeriodUDFRegistrator.registerAllUDFs(spark);
        PeriodSetUDFRegistrator.registerUDFs(spark);
    }

    @AfterAll
    public static void tearDownSparkSession() {
        if (spark != null) {
            spark.stop();
            spark = null;
            meos_finalize();
        }
    }
}