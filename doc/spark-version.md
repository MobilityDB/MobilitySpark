# Spark version target

MobilitySpark targets **Apache Spark 3.5.x** (the current LTS line). One artefact, one supported Spark major.

## Position table

| Axis | Choice | Why |
|---|---|---|
| Spark engine version | **3.5.x** only | LTS line through 2026; the production-line default in every managed Spark distribution (Databricks DBR 15, AWS EMR 7.0-7.4, Google Dataproc image 2.2, Azure Synapse pool default). One MobilitySpark jar runs on the whole 3.x line because the Java/Scala API surface of `spark-core` is stable across 3.0 → 3.5. |
| MEOS C-library version | follows JMEOS | The two existing MEOS lines (v1.3 on MobilityDB master, v1.4 as the in-flight 15-PR bump) flow through JMEOS as separate jar builds. MobilitySpark pins one JMEOS jar; JMEOS pins one MEOS SHA. Version discipline lives in JMEOS, not here. |
| Scala version | 2.12 / 2.13 (as Spark 3.5 supports) | The Maven profile selects against `spark-core_2.13` by default; `_2.12` is available for adopters on the older Scala line. |

## Why not Spark 4.0 today

Spark 4.0 (May 2025 GA) is **early-adoption**, not production-default. Concrete signals:

| Distribution / ecosystem | Default Spark version | Spark 4 status |
|---|---|---|
| Databricks Runtime | DBR 15 = Spark 3.5 | DBR 16 (Spark 4) available, not the UI default |
| AWS EMR | EMR 7.0-7.4 = Spark 3.5 | EMR 7.5+ has Spark 4, not the default image |
| Google Dataproc | Image 2.2 = Spark 3.5 LTS | Image 2.3 (Spark 4) in preview |
| Azure Synapse / HDInsight | Spark 3.5 pool | Spark 4 not yet GA in the managed runtime |
| Cloudera CDP | Spark 3.5 | Spark 4 not yet shipped |
| Delta Lake | Delta 3.x → Spark 3.4 / 3.5 | Delta 4.0 → Spark 4 (separate release line) |
| Apache Iceberg | `iceberg-spark-runtime-3.5` stable | `iceberg-spark-runtime-4.0` in Iceberg 1.7+, recently-stabilised |
| Apache Hudi | Spark 3.x supported across recent releases | Spark 4 support rolling out in the 1.x line |
| Apache Sedona (spatial, closest analogue) | Spark 3.4 / 3.5 first-class | Spark 4 added in Sedona 1.7 (late 2025), some modules still flagged experimental |

Spark 4 also introduces behavioural breakers (ANSI-by-default, type widening, parameterised SQL) that **break existing 3.x SQL**, so enterprise data teams typically wait 12-18 months before migrating because every SQL workload needs revalidation.

## Trigger signals that switch this position

Commit to Spark 4 support when **two or more** of these fire:

| Signal | Why it matters |
|---|---|
| Databricks DBR 16 becomes the default runtime in the Databricks UI | Mainstream tipping point — production workloads start moving |
| AWS EMR 7.5+ becomes the default in the EMR console | Same, for AWS |
| Iceberg makes Spark 4 the **recommended** runtime (not just supported) | Data-lake stack consensus |
| Apache Sedona drops the "experimental" qualifier on Spark 4 modules | Direct spatial-analogue signal |
| A MobilityDB user opens a Spark-4 issue against MobilitySpark | Concrete demand |

None have fired as of this writing. Track them.

## What the version axis looks like if/when Spark 4 is added

The pattern would mirror MobilityDuck's multi-DuckDB-version foundation (`MobilityDuck/doc/multi-duckdb-version.md`):

- Maven profile per Spark version (`mvn -Pspark-3.5` / `mvn -Pspark-4.0`); each profile pins the matching `spark-core` dependency.
- `if (sparkVersionAtLeast("4.0.0")) { … } else { … }` preprocessor-style branches for the small set of API divergences (`udf.register` shape, `DataType.fromName`, deprecated `ScalaUDF` constructors).
- CI matrix dimension `spark_version: [3.5.4, 4.0.0]`; each row builds a separate jar.
- Per-Spark-version jar in the release artefacts (`mobilityspark-1.0.0-spark-3.5.jar` / `mobilityspark-1.0.0-spark-4.0.jar`).

This is **future work**, not current. The estimated effort is small-to-medium because the Spark Java API divergences between 3.5 and 4.0 are narrow compared to the DuckDB v1.4-vs-v1.5 ABI break.

## Relationship to MEOS-version churn

The MEOS-version axis is **orthogonal** to the Spark-version axis. MEOS versions (v1.3, v1.4) flow through JMEOS regenerations; MobilitySpark depends on one JMEOS jar at a time. Bumping MEOS means swapping the JMEOS dependency, not touching anything Spark-version-specific.

## Conclusion

| Question | Answer |
|---|---|
| What Spark version does MobilitySpark target? | 3.5.x (current LTS) |
| Does MobilitySpark support Spark 4 today? | No |
| Is the lack of Spark 4 support a parity gap with the standard stack? | No — Spark 4 is early-adoption; the production stack is 3.5 |
| When will Spark 4 support be added? | When the trigger signals fire (see table above) |
| Is there a MEOS-version-axis problem MobilitySpark needs to solve? | No — JMEOS owns it; MobilitySpark pins one JMEOS jar at a time |
