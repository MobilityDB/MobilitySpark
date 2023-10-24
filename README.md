# MobilitySpark

A proof-of-concept implementation of MEOS for https://github.com/MobilityDB/MobilityDB on Spark, using the JMEOS library as middleware.

::: note 📝

MobilitySpark explores the advantages of MobilityDB datatypes and functions in the Spark environment. However, it should be noted that the current implementation is not a full implementation and is still far from complete.

:::

## Table of Contents

- [Features](https://www.notion.so/SparkMeos-67cc21ffc21b479b91c832dba75dbae8?pvs=21)
- [Requirements](https://www.notion.so/SparkMeos-67cc21ffc21b479b91c832dba75dbae8?pvs=21)
- [Installation](https://www.notion.so/SparkMeos-67cc21ffc21b479b91c832dba75dbae8?pvs=21)
- [Usage](https://www.notion.so/SparkMeos-67cc21ffc21b479b91c832dba75dbae8?pvs=21)
- [Understanding SparkMeos](https://www.notion.so/SparkMeos-67cc21ffc21b479b91c832dba75dbae8?pvs=21)
    - [The Code Structure](https://www.notion.so/SparkMeos-67cc21ffc21b479b91c832dba75dbae8?pvs=21)
    - [UDTs](https://www.notion.so/SparkMeos-67cc21ffc21b479b91c832dba75dbae8?pvs=21)
    - [UDFs](https://www.notion.so/SparkMeos-67cc21ffc21b479b91c832dba75dbae8?pvs=21)
- [Future Work](https://www.notion.so/SparkMeos-67cc21ffc21b479b91c832dba75dbae8?pvs=21)

## Features

- ✅ Common user-defined-type (UDT) implementation, defined as MeosDatatype.
    - Includes instantiation of some MobilityDB types such as PeriodUDT, PeriodSetUDT, TimestampSetUDT.
- 📝 Some functions defined as UDF’s.
- 🌟 Examples.

## Requirements

- 🚀 MobilityDB installed with MEOS
- 🔧 JMEOS working version
- ⚡ Spark 3.4.0
- 📝 Maven 4
- ☕ Java 17 (recommended)

## Usage

### Build the project

First, make sure to have installed Maven in your machine. In case of MacOS, you can install via [Homebrew](https://formulae.brew.sh/formula/maven). The other step will be to install MobilityDB with MEOS in your machine, as the meos library files are required. To install MobilityDB with MEOS please follow the following instructions in your terminal:

```bash
git clone https://github.com/MobilityDB/MobilityDB
mkdir MobilityDB/build
cd MobilityDB/build
**cmake -D MEOS=ON.. // This flag is important, to indicate the installer to build with the meos tools.**
make
sudo make install
```

Once done, we can proceed to build the project. Spark will be downloaded as a maven dependency automatically, and JMEOS is already packaged within the project.

To build the project using Maven command-line tools, follow these steps:

1. Open a terminal or command prompt.
2. Navigate to the directory where the project's `pom.xml` file is located.
3. Run the command `mvn clean install` to build the project and create the JAR file.
4. Once the build is complete, the JAR file can be found in the `target` directory.

Note that you may need to have Maven installed on your system in order to run the command.

### Set up Intellij IDEA (Optional)

Additionally, if you are using Intellij IDEA you can use similar setup to run your spark project.

```java
<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="Spark" type="Application" factoryName="Application">
    <envs>
      <env name="SPARK_LOCAL_IP" value="10.93.44.4" />
    </envs>
    <option name="MAIN_CLASS_NAME" value="org.mobiltydb.Examples.AISDatasetExample" />
    <module name="SparkMeos" />
    <option name="PROGRAM_PARAMETERS" value="-c spark.driver.bindAddress=127.0.0.1" />
    <option name="VM_PARAMETERS" value="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED " />
    <method v="2">
      <option name="Make" enabled="true" />
    </method>
  </configuration>
</component>
```

### Running the examples

::: note 📝

Please note that the examples provided in MobilitySpark are for simple demonstration purposes only. They do not represent a full implementation of the MEOS functionality and should not be used as such. However, they do provide a good starting point for understanding how Spark interacts with MEOS.

:::

Once you have built the project run the command:

```bash
java --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
     --add-exports java.base/sun.security.action=ALL-UNNAMED \
     --add-opens java.base/java.nio=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     -cp target/classes/ org.mobiltydb.Examples.<EXAMPLE>
```

where <EXAMPLE>, is the name of the example class to execute.

It is recommended though to run the example directly in IntelliJ idea with the following VM configurations:

```bash
--add-exports
java.base/sun.nio.ch=ALL-UNNAMED
--add-exports
java.base/sun.security.action=ALL-UNNAMED
--add-opens
java.base/java.nio=ALL-UNNAMED
--add-opens
java.base/java.lang=ALL-UNNAMED
```

### AIS Dataset

We also implemented AIS Dataset example.

```java
// Read AIS Dataset
        ais = ais.withColumn("point", callUDF("tGeogPointIn", col("latitude"), col("longitude"), col("t")))
                        .withColumn("sog", callUDF("tFloatIn", col("sog"), col("t")));
        ais = ais.drop("latitude", "longitude");
        ais.show();
```

In the example, we did aggregation using spark and custom UDF from SparkMeos to assemble the dataset.

```java
// Assemble AIS Dataset
        Dataset<Row> trajectories = ais.groupBy("mmsi")
                .agg(callUDF("tGeogPointSeqIn", functions.collect_list(col("point"))).as("trajectory"),
                        callUDF("tFloatSeqIn", functions.collect_list(col("sog"))).as("sog"));
        trajectories.show();
```

Furthermore, we did simple analytics.

```java
Dataset<Row> originalCounts = ais.groupBy("mmsi")
                .count()
                .withColumnRenamed("count", "original #points");

        Dataset<Row> instantsCounts = trajectories
                .withColumn("SparkMEOS #points", callUDF("tGeogPointSeqNumInstant", trajectories.col("trajectory")));

        Dataset<Row> startTimeStamp = trajectories
                .withColumn("Start Timestamp", callUDF("tGeogPointSeqStartTimestamp", trajectories.col("trajectory")));

        originalCounts.join(instantsCounts, "mmsi").join(startTimeStamp, "mmsi").
                select("mmsi", "SparkMEOS #points", "original #points", "Start Timestamp").show();
```

## Understanding SparkMeos

### The Code Structure

The SparkMeos project is divided mainly into two folders: `UDTs` and `UDFs`, as well as a `utils` folder. The `UDTs` folder contains the user-defined types that have been implemented in the project, while the `UDFs` folder contains the user-defined functions. The `utils` folder contains utility classes that are used throughout the project.

This structure allows for easy organization and management of the project's components, making it easier to maintain and further develop the project.

### UDTs

The `MeosDatatype<T>` class in SparkMeos is the core class of the project. It has the signature `public abstract class MeosDatatype<T> extends UserDefinedType<T>`. All UDTs in SparkMeos should inherit from this class to implement their behavior. The class assumes that all Meos datatypes utilize a BinaryType datatype for standardization purposes. The serialization and deserialization methods are already implemented in the class, and the only methods that need to be redefined when creating a `MeosDatatype<T>` are `userClass()`, specifying the JMEOS class that this datatype is linked to, and `fromString(String s)`, to create the object from a string.

For example, let’s evaluate the implementation of the PeriodUDT:

```java
@SQLUserDefinedType(udt = PeriodUDT.class)
public class PeriodUDT extends MeosDatatype<Period> {
    /**
     * Provides the Java class associated with this UDT.
     * @return The Period class type.
     */
    @Override
    public Class<Period> userClass() {
        return Period.class;
    }
    @Override
    protected Period fromString(String s) throws SQLException{
        return new Period(s);
    }
}
```

The previous code block is written in Java and defines the implementation of the `PeriodUDT` user-defined type in the SparkMeos project. It includes the class signature, annotations, and inheritance of the `MeosDatatype` class, which is the core class of the project. The `PeriodUDT` class overrides two methods, `userClass()` and `fromString(String s)`, to specify the JMEOS class that this datatype is linked to and to create the object from a string, respectively. The `@SQLUserDefinedType` annotation is used to specify the UDT class that the `PeriodUDT` class is associated with.

Now we can instantiate a Period datatype in Spark!

```java
meos_initialize("UTC");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        UDTRegistrator.registerUDTs(spark);
        //UDFRegistrator.registerUDFs(spark);
        PeriodUDFRegistrator.registerAllUDFs(spark);
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

        System.out.println("Example 1: Show all Periods.");
        df.printSchema();
        // Show the result
        result.show(false);

meos_finalize();
```

The previous code shows how to instantiate a `Period` datatype in Spark, create a `DataFrame` with a single column of `Periods`, register the `DataFrame` as a temporary view, and use Spark SQL to query the view. The resulting `DataFrame` is then printed to the console. The `meos_initialize` and `meos_finalize` functions are used to initialize and finalize the JMEOS middleware, respectively. However let me highlight some important things when using SparkMeos:

- `meos_initialize()` and `meos_finalize()` functions are used to initialize and finalize the JMEOS middleware, respectively. These functions should always be called before and after using the JMEOS middleware.
- Registering the datatypes and UDFs using the respective registrators (`UDTRegistrator.registerUDTs()` and `UDFRegistrator.registerUDFs()`) is necessary for Spark to recognize and use the UDTs and UDFs in the project.

This will print something similar to:

```bash
Example 1: Show all Periods.
root
 |-- period: period (nullable = true)

23/08/28 10:33:33 INFO CodeGenerator: Code generated in 224.520375 ms
23/08/28 10:33:34 INFO CodeGenerator: Code generated in 75.834833 ms
+------------------------------------------------+
|period                                          |
+------------------------------------------------+
|[2023-08-28 10:33:31+02, 2023-08-28 11:33:31+02)|
|[2023-08-28 11:33:31+02, 2023-08-28 13:33:31+02)|
|[2023-08-28 12:33:31+02, 2023-08-28 13:33:31+02)|
+------------------------------------------------+
```

Notice how the Spark schema now recognizes the period datatype.

### UDFs

Each UDT in SparkMeos needs to register its own UDFs in Spark because Spark does not automatically recognize the functions defined in the UDT class. By registering the UDFs, Spark is able to recognize and use them in SQL queries and data manipulation operations. Additionally, registering the UDFs allows for a more organized and modular approach to defining functions in the project, making it easier to maintain and modify as necessary.

Given the extensive number of functions in MobilityDB, this POC only implements a few of these functions.

For example:

```java
spark.sql(
	"SELECT periodExpand(" +
								"stringToPeriod('[2023-08-07 14:10:49+02, 2023-08-07 15:10:49+02)'), " +
                "stringToPeriod('[2019-09-08 02:00:01+02, 2019-09-10 02:00:01+02)')) " +
	"as period"
).show(false);
```

The `spark.sql()` command shown queries Spark to execute a SQL statement. In this particular example, the SQL statement is selecting a UDF called `periodExpand()` that takes two arguments, which are two periods converted from strings using the `stringToPeriod()` function. The `periodExpand()` function returns a third period that has the lower bound of the earliest period and the upper bound of the latest period. The resulting period is then displayed as an output using the `show()` function.

As well as with the UDT’s, each UDF should be registered, this happens when calling `UDFRegistrator.registerUDFs(spark);`. 

## Unit Test

Currently, we have implemented small unittest for our project. However, the unittest only works for Intellij IDEA and will fail if we run them through maven. We are currently disabling the unittest in the build configuration.

## Future Work

Since this is only a POC to demonstrate that MobilityDB and MEOS can be ported into Spark, we want to highlight the next steps and future work to achieve a full implementation:

- **Complete implementation and integration of MEOS data types into SparkMEOS.**
    - This includes the core, basic, boxes, temporal, and time data types from JMEOS.
    - So far, we have implemented the time data types as well as some extra few.
- **Generation of the UDFs with MobilityDB syntax.**
    - The currently implementation wraps some of the functions either into a UDF1 or UDF2 class, but wrapping all the existing functions will be an almost impossible task to do.
    - A proposal is to create a UDF generator script that, just like in JMEOS, creates all the UDFs based on the expected data and return type.
    - Since some of the functions can return many different data types, future implementations should take into account the use of function overloading and polymorphism to fit into Spark SQL this behavior. This is achievable since all UDTs use MeosDatatype<T> under the hood, and also each instance of T follows the hierarchy defined in JMEOS.
- **************Proper UDT implementation**************
    - Right now, we have abstraction called MeosDataType for implementing the UDT, and able to implement MEOS DataType as UDT in Spark. In the future we want to extend the abstraction of Meos DataType by implementing the base type like Temporal, Sequence, etc as an UDT interface to allow polymorphism.
- **Creation of the JDBC Spark / Postgres Dialect with the UDTs.**
    - Initial work in this has been explored (please see the jdbc-controller branch), but a more robust and satisfactory implementation is needed.
    - We need to implement a Dialect that correctly maps each UDT to its MobilityDB equivalent. For example we need to map period to PeriodUDT and perform the necessary transformations.
        - This happens under the JdbcDialect class from Spark. It is necessary to override some of this class’ methods such as getCatalystType for table reading and getJdbcType for table writing.
        - The PostgresDriver should also be extended to handle and register first the UDT datatypes, or map the UDT datatypes to their corresponding PGObject (which is implemented in the JMEOS datatypes).
- **Implementing MobilityDB’s optimizations into Spark Catalyst.**
    - This point will represent very extensive work but critical to exploit the benefits of Spark and distributed computing to it’s maximum.
        - Catalyst is Spark’s optimizer. All queries go through it to generate an execution plan.
        - It will be critical to implement distribution behavior to reduce random shuffling when executing operations in Spark. For instance, we can partition MobilityDB’s datatypes by 2 different approaches: by time or by geographic position. In the case of position, catalyst should implement the partition strategy to process data belonging to the same tile in the same cluster.
