# MobilitySpark BerlinMOD benchmark runner

Times the canonical BerlinMOD suite on Apache Spark. The queries come from the
`berlinmod/suite/` submodule (the single canonical source); see
[`../README.md`](../README.md) for context.

---

## Prerequisites

```bash
# Apache Spark 3.5.4 (~300 MB) + Maven
sudo apt-get install maven
bash ../../setup/install_spark.sh
source ~/.bashrc

# Build the fat JAR (once, or after code changes)
cd ../.. && mvn package -DskipTests -q
```

## Run

```bash
# Full corpus, 3 runs/query, data from berlinmod/data/
bash bench_mspark.sh --data ../data --runs 3 --output results/mspark.json

# 1 run/query; select queries
bash bench_mspark.sh --data ../data --quick --queries q05,q10
```

All queries run in a single `spark-submit` session (class `BerlinMODBench`);
JVM startup is excluded from query timings. The runner reads every query from
`berlinmod/suite/` — no Spark-specific variant. The index-less spatial joins
(q10/q11/q12/q14) can exceed a per-query budget on the full corpus; wrap the
call in `timeout` to bound them (see `../README.md`, "Spatial joins on Spark").

## Cross-engine comparison (offline)

Each tool emits its own `*.json`. To build a comparison table/chart, collect
`mspark.json` here together with `mbdb.json` / `mduck.json` from those repos
into one directory, then merge offline:

```bash
python3 report.py --results <dir> --output <dir>/report.md
python3 chart.py  --results <dir> --output <dir>/chart.png --log
```

`chart.py` requires `matplotlib`. `--log` is advisable since Spark's spatial
joins span several orders of magnitude.

## Methodology

- **Timing**: wall-clock milliseconds around each query, median of N runs; data
  loading excluded.
- **SQL**: the identical canonical `<query>.sql` from `berlinmod/suite/` —
  named-function portable dialect, no operator symbols.
