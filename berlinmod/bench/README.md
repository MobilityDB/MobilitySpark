# BerlinMOD Cross-Platform Benchmark

Measures the 18 BerlinMOD portable SQL queries on three platforms — **MobilityDB**
(PostgreSQL), **MobilityDuck** (DuckDB), and **MobilitySpark** (Apache Spark) —
using identical SQL on a shared CSV dataset.

Queries are defined in [Discussion #861](https://github.com/MobilityDB/MobilityDB/discussions/861).
Share your results on [Discussion #913](https://github.com/MobilityDB/MobilityDB/discussions/913).

---

## Prerequisites

### All platforms
- Python 3.8+ (for `report.py` and the JSON conversion in timing scripts)
- `osm2pgrouting` (for generating BerlinMOD data):
  ```bash
  sudo apt-get install osm2pgrouting
  ```

### MobilityDB
- PostgreSQL with MobilityDB installed
- `psql` on `PATH`

### MobilityDuck
- `duckdb` CLI (community or local MobilityDuck build) on `PATH`

  The script auto-detects a local MobilityDuck build at `$MOBILITYDUCK_CLI`
  or falls back to the system `duckdb`.

### MobilitySpark
Install once:
```bash
# 1. Maven (builds the fat JAR)
sudo apt-get install maven

# 2. Apache Spark 3.5.4  (~300 MB)
bash ../../setup/install_spark.sh
source ~/.bashrc           # or open a new shell

# 3. Build the JAR (once, or after code changes)
cd ../..
mvn package -DskipTests -q
```

---

## Quick start

### 1 — Generate the dataset

```bash
# Generate BerlinMOD CSV data (scale 0.005 → ~100 vehicles, ~10 000 trips, ~15 min)
bash ../../setup/generate_data.sh

# Larger dataset (scale 0.05 → ~1000 vehicles, ~100 000 trips, ~2–3 h)
bash ../../setup/generate_data.sh --scalefactor 0.05
```

CSV files land in `berlinmod/data/` by default.

### 2 — Run the benchmark

```bash
# All three platforms, 3 runs per query, data from berlinmod/data/
bash bench.sh

# Skip platforms you haven't installed
bash bench.sh --skip-mspark
bash bench.sh --skip-mbdb --skip-mduck   # Spark only

# Custom data directory, 5 runs, custom output directory
bash bench.sh --data /path/to/data --runs 5 --output /path/to/results
```

Results are written to `results/mbdb.json`, `results/mduck.json`,
`results/mspark.json`, and `results/report.md`.

### 3 — Read the report

```bash
cat results/report.md
```

Or regenerate it at any time from existing JSON files:

```bash
python3 report.py --results results
```

---

## Running individual platforms

```bash
# MobilityDB only
bash bench_mbdb.sh --data ../data --runs 3 --output results/mbdb.json

# MobilityDuck only
bash bench_mduck.sh --data ../data --runs 3 --output results/mduck.json

# MobilitySpark only
bash bench_mspark.sh --data ../data --runs 3 --output results/mspark.json
```

Use `--no-load` to skip the data-load step if the tables / file-based database
already exist from a previous run.

---

## File layout

```
berlinmod/
  bench/
    bench.sh          — orchestrator (calls all three + report.py)
    bench_mbdb.sh     — MobilityDB timer
    bench_mduck.sh    — MobilityDuck timer
    bench_mspark.sh   — MobilitySpark (Spark + BerlinMODBench.java) timer
    report.py         — reads results/*.json, writes results/report.md
    results/          — per-run JSON + report (not committed)
  data/               — shared CSV files (vehicles.csv, trips.csv, …)
  q01.sql … q17.sql   — portable SQL queries (named-function dialect)
  qrt.sql             — binary round-trip query
setup/
  install_spark.sh    — installs Maven + Spark 3.5.4
  generate_data.sh    — generates BerlinMOD CSV data via PostgreSQL
```

---

## Sharing your results

1. Run `bench.sh` to generate `results/report.md`.
2. Open [Discussion #913](https://github.com/MobilityDB/MobilityDB/discussions/913).
3. Paste the markdown table as a new comment.

---

## Methodology notes

- **Timing**: wall-clock milliseconds (`date +%s%3N`) around each query invocation,
  median of N runs. Data loading is excluded from all timings.
- **MobilityDuck**: queries run against a persistent file-based DuckDB so load time
  is paid once, not per query.
- **MobilitySpark**: all queries run in a single `spark-submit` session
  (class `BerlinMODBench`); JVM startup is excluded from query timings.
- **SQL**: all three platforms execute identical SQL from the `berlinmod/*.sql` files.
  No platform-specific operator symbols — named functions only per the portable
  dialect in [Discussion #861](https://github.com/MobilityDB/MobilityDB/discussions/861).
