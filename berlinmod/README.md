# BerlinMOD benchmark — MobilitySpark

This directory benchmarks the **canonical portable BerlinMOD suite** on
Apache Spark via MobilitySpark's MEOS-backed UDFs.

The queries, schema, and load script are **not** kept here — they live in the
single canonical source, vendored as the `suite/` git submodule
([`berlinmod-portability`](https://github.com/estebanzimanyi/berlinmod-portability),
shared byte-for-byte with MobilityDB and MobilityDuck). This directory holds
only the Spark-side runner, the data corpus, and the expected results.

```
berlinmod/
  suite/        # submodule — the ONE canonical SQL (q01..q17, qrt, schema.sql,
                #             portable_aliases.sql, data/load.sql)
  data/         # CSV corpus loaded into Spark
  expected/     # expected query results for correctness checks
  bench/        # bench_mspark.sh (timing), report.py, chart.py
  run_mspark.sh # correctness demo runner
```

After cloning, initialise the submodule:

```bash
git submodule update --init berlinmod/suite
```

---

## Running

**Timing benchmark** (`BerlinMODBench` — reads every query from `suite/`):

```bash
berlinmod/bench/bench_mspark.sh --data berlinmod/data --output bench/results/mspark.json
# --quick = 1 run/query, --queries q05,q10 to select, --runs N to repeat
```

The runner sets `-Dberlinmod.sql.dir=berlinmod/suite`, so every query is read
from the canonical submodule — there is no Spark-specific query variant and no
SQL rewriting.

**Correctness demo** (`BerlinMODDemo` — self-contained synthetic dataset):

```bash
berlinmod/run_mspark.sh [spark-submit-binary]
```

---

## Spatial joins on Spark

Spark SQL has **no native spatial index**. The canonical queries pre-filter
spatial joins with the bounding-box `&&` operator, which PostgreSQL (GiST) and
DuckDB (TRTREE) accelerate with a spatial index. Spark cannot — it evaluates
`&&` as an opaque UDF, so the index-less spatial joins **q10 / q11 / q12 / q14**
degrade to a Cartesian product with a per-pair MEOS UDF (each call re-parses the
trajectory from hex). On the full corpus these exceed any reasonable per-query
budget; bound them with the caller's own `timeout`.

The route to timings comparable to the indexed engines is the **th3index
columnar prefilter**: explode each trip's th3index into one row per H3 cell and
turn the overlap into an **equi-join on the cell column**, which Spark
accelerates natively (hash join) — pruning candidate pairs before the exact
`tDwithin`/`eDwithin` runs. th3index is portable (PG/DuckDB/Spark all compute it
from the same MEOS function), so it is the Tier-1 acceleration all three share.

---

## Data

`data/` holds the BerlinMOD corpus (`trips.csv` is git-ignored — it is large and
regenerated). The CSVs are produced by
[MobilityDB-BerlinMOD](https://github.com/MobilityDB/MobilityDB-BerlinMOD) via
`berlinmod_portability_export()`:

```sql
-- In a PostgreSQL database with generated BerlinMOD data:
\i BerlinMOD/berlinmod_export.sql
SELECT berlinmod_portability_export('/path/to/output/');
```

This writes `vehicles.csv`, `trips.csv`, `query_*.csv` in the schema defined by
`suite/schema.sql`. Drop them into `data/` and re-run the benchmark.

---

## Cross-engine comparison

Each tool benchmarks itself and emits a results JSON. The cross-engine
comparison is assembled **offline** by merging the per-tool JSONs:

```bash
# Collect mspark.json (here) + mbdb.json / mduck.json (from those repos) into one dir, then:
python3 bench/report.py --results <dir> --output <dir>/report.md
python3 bench/chart.py  --results <dir> --output <dir>/chart.png --log   # --log: Spark spatial joins span orders of magnitude
```
