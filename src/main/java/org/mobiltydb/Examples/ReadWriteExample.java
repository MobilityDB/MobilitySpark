package org.mobiltydb.Examples;

import jmeos.types.time.Period;
import org.apache.spark.sql.*;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.mobiltydb.UDF.Period.PeriodUDFRegistrator;
import org.mobiltydb.UDT.MeosDatatype;
import org.mobiltydb.UDT.MeosDatatypeFactory;
import org.mobiltydb.UDT.PeriodUDT;
import utils.MeosSparkPostgresDriver;
import utils.MobilityDBJdbcDialect;
import utils.UDFRegistrator;
import utils.UDTRegistrator;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static jmeos.functions.functions.meos_finalize;
import static jmeos.functions.functions.meos_initialize;
import static org.apache.spark.sql.functions.*;

public class ReadWriteExample {
    public static void main(String[] args) throws SQLException, AnalysisException {
        meos_initialize("UTC");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .config("spark.jars", "/Users/luisalfredoleonvillapun/SparkMeos/target/SparkMeos-1.0-SNAPSHOT.jar")
                .getOrCreate();

        //spark.sparkContext().setLogLevel("DEBUG");

        UDTRegistrator.registerUDTs(spark);
        UDFRegistrator.registerUDFs(spark);
        //PeriodUDFRegistrator.registerAllUDFs(spark);


        // Create some example Period objects
        OffsetDateTime now = OffsetDateTime.now();
        Period period1 = new Period(now, now.plusHours(1));
        Period period2 = new Period(now.plusHours(1), now.plusHours(3));
        Period period3 = new Period(now.plusHours(2), now.plusHours(3));

        List<Row> data = Arrays.asList(
                RowFactory.create(period1),
                RowFactory.create(period2),
                RowFactory.create(period3)
        );

        StructType schema = new StructType()
                .add("period", new PeriodUDT());

        // Create a DataFrame with a single column of Periods
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Register the DataFrame as a temporary view
        df.createOrReplaceTempView("Periods");


        // Use Spark SQL to query the view
        Dataset<Row> result = spark.sql("SELECT * FROM Periods");


        System.out.println("Example 0: Load dataframe into spark.");
        df.printSchema();
        // Show the result
        result.show(false);

        System.out.println("Example 1: Save dataframe as parquet and read back.");
        //Save as parquet
        result.write().mode("overwrite").parquet("testp.parquet");
        //Load back parquet
        Dataset<Row> resultLoaded = spark.read().parquet("testp.parquet");
        resultLoaded.printSchema();
        resultLoaded.show();

        System.out.println("Example 2: Save dataframe as csv and read back.");
        //Save as csv
        result.withColumn("period", new Column("period").cast(DataTypes.StringType)).write().mode("overwrite").csv("testp.csv");
        //Load back csv
        resultLoaded = spark.read().schema(schema).csv("testp.csv");
        resultLoaded.printSchema();
        resultLoaded.show();

        MeosSparkPostgresDriver customDriver = new MeosSparkPostgresDriver();
        JdbcDialects.registerDialect(new MobilityDBJdbcDialect());

        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:25432/mobilitydb", "docker", "docker");
        DatabaseMetaData metaData = connection.getMetaData();

        ResultSet rs = metaData.getTypeInfo();
        while (rs.next()) {
            System.out.println(rs.getString("TYPE_NAME") + "\t" + JDBCType.valueOf(rs.getInt("DATA_TYPE")).getName());
        }

        rs.close();
        connection.close();


        System.out.println("Example 3: Save dataframe into MobilityDB instance and read back.");

        Properties jdbcProps = new java.util.Properties();
        jdbcProps.setProperty("driver", "utils.MeosSparkPostgresDriver");
        jdbcProps.setProperty("user", "docker");
        jdbcProps.setProperty("password", "docker");
        //jdbcProps.setProperty("stringtype", "unspecified");

        Dataset<Row> resultRead = spark.read().format("jdbc")
//                .option("url", "jdbc:postgresql://localhost:25432/mobilitydb")
                        .option("truncate","true") // Important
                        .option("driver", "utils.MeosSparkPostgresDriver")
                        .jdbc("jdbc:postgresql://localhost:25432/mobilitydb", "sparkmeos", jdbcProps);
        resultRead = resultRead.withColumn("p", functions.expr("stringToPeriod(p)"));
        resultRead.printSchema();

        resultRead.show();


        result.withColumnRenamed("period", "p")
                //.withColumn("p", new Column("p").cast(DataTypes.StringType))
                .write()
                .format("jdbc")
                .option("truncate","true") // Important
                .mode("append")  // This will overwrite the existing table. You can also use "append" to add to it.
                .jdbc("jdbc:postgresql://localhost:25432/mobilitydb", "sparkmeos", jdbcProps);

        meos_finalize();
    }
}
