# BerlinMOD on MobilityDB — Finalized Findings

*Scale factor 0.005 (141 vehicles / 1,620 trips). MobilityDB 1.4.0 on PostgreSQL 18.1.
MobilityDuck and MobilitySpark are deferred (their builds don't yet expose th3index),
so this covers the **MobilityDB** column only.*

## 1. Query timings (tier 3 = all indexes, median of 3 runs)

| Query | ms | | Query | ms |
|---|--:|---|---|--:|
| q01 | 5 | | q10 | 27,740 |
| q02 | 19,930 | | q11 | 27,140 |
| q03 | 9,150 | | q12 | 25,220 |
| q04 | 15,570 | | q13 | 9,150 |
| q05 | 51,820 | | q14 | 17,680 |
| q06 | 3,460 | | q15 | 8,910 |
| q07 | 132,320 | | q16 | 68,780 |
| q08 | 4,350 | | q17 | 24,120 |
| qrt | 9,870 | | | |

Full machine spec + table: `berlinmod/bench/results/report.md`.

## 2. Correctness

These are **execution-verified and, for the th3-prefilter queries, soundness-verified** —
not validated against `expected/*.csv`, which is a stale toy fixture (2–5 rows/query,
models `{Sedan,Lorry}`) that does not match the sf-0.005 data and cannot serve as an oracle.

- **Soundness check (oracle substitute):** each th3-prefiltered query returns exactly the
  exhaustive predicate's result — q02 = 139 = 139 (with prefilter / with `eIntersects` alone),
  q04 = 43 = 43. The prefilter drops no true positives.
- **q16 = 0 is genuine:** 10 candidate pairs co-occur in a region during a period, but none
  are *always disjoint*.
- Two correctness bugs were found and fixed (see §5).

## 3. th3index acceleration study — th3 does **not** accelerate this MobilityDB workload

The headline result of the index-tier study. Two independent lines of evidence:

**(a) The prefilter operators as written are not GiST-indexable.** `ever_eq` and
`everIntersectsH3IndexSet_Th3Index` are not in the `th3index` GiST operator class (which has
`&&`, `@>`, `<@`, `~=`, `-|-`, …). So the `trip_h3` GiST index is never used — the predicates
run as post-filters. The native MobilityDB spatial indexes (GiST/SP-GiST on `trip`, via
`trip && stbox` / `trip && expandSpace`) drive every query.

Tier comparison (median ms, th3-prefilter queries), where T3 = both indexes, T2 = native only,
T1 = th3 only:

| Query | T3 both | T2 native-only | T1 th3-only |
|---|--:|--:|--:|
| q02 | 23,172 | 20,710 | 50,498 |
| q04 | 17,227 | 16,051 | 51,132 |
| q05 | 62,600 | 55,028 | 57,523 |
| q06 | 3,848 | 3,705 | 209 |
| q10 | 29,696 | 29,190 | 26,296 |

T2 ≈ T3 everywhere → the th3 index's presence is irrelevant (it is never used). The only large
movement is in T1, and it is entirely about **losing the native index**: q06's "18×" is the
native SP-GiST nested-loop being a *pessimization* for a 12-truck self-join (dropping it lets the
planner fall back to a faster hash-join + filter), not a th3 win.

**(b) Even with the indexable `&&` operator, the th3 index does not help.** Rewriting the
prefilter to `t1.trip_h3 && t2.trip_h3` does use the GiST index, but `&&` (bbox-overlap of the
whole cell sequence) is too loose. Measuring the same `&&` query with the th3 index toggled:

| Query (`&&`-prefilter) | no index (plain filter) | th3 GiST index | effect |
|---|--:|--:|---|
| q06 | 795 ms | 18,038 ms | th3 index **23× slower** |
| q10 | 134,359 ms | 137,291 ms | neutral |

## 4. Conclusion and cross-platform framing

On **MobilityDB**, th3index is **redundant**: the mature native spatial indexes already
accelerate the BerlinMOD predicates, and the th3 prefilter is either non-indexable overhead
(`ever_eq`) or an indexable-but-too-loose filter (`&&`) that the index makes slower.

This is consistent with th3's intended value being on platforms that **lack** native moving-object
spatial indexes — **MobilityDuck (DuckDB) and MobilitySpark (Spark)** — where a columnar th3
prefilter may be the only available acceleration. That comparison is exactly what the
cross-platform benchmark exists to measure, and is the next step once those builds expose
th3index. The MobilityDB column is the reference baseline for it.

## 5. Methodology and fixes

- **Instance:** dedicated `bench_pg` (PG18, port 5455, `shared_buffers=2GB`). Each tier reloads
  into fresh relfilenodes, resetting the cache so tiers are comparable; no native-then-th3
  cache-warming confound.
- **Timing:** wall-clock median (3 runs for the report, 2 for the tier study); `EXPLAIN (ANALYZE)`
  for the index studies. Loading excluded.
- **Correctness fixes committed this round:**
  - Bench timers (`bench_mbdb.sh`/`bench_mduck.sh`) masked query errors as phantom ~4 ms timings;
    now they fail loud and exclude failed queries.
  - `QueryRegions` was loaded with the wrong SRID (lon/lat stamped as 3857), silently returning 0
    rows for every region query; now reprojected 4326→3857.

## 6. Caveats / open items (MobilityDB)

- No independent result oracle (the cross-platform agreement test is the real one — pending
  Duck/Spark). The `expected/*.csv` fixtures need regenerating for sf 0.005.
- Tier-study timings are median of 2 (noisy on the slow queries; q06's signal is stable).
- The `&&` rewrite is documented here, not applied to the shared portable queries (it changes
  q05's semantics — no exact re-filter — and belongs in a deliberate prefilter-design decision).
