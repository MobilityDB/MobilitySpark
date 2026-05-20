-- BerlinMOD Q5 — Spark-optimised variant (UNNEST + equi-join on H3 cell).
--
-- Semantic equivalent to q05.sql.  Same input, same output.  This file
-- exists ONLY for Spark because Spark SQL has no spatial index — the
-- portable q05.sql evaluates the `everEqTh3IndexTh3Index` predicate
-- row-by-row inside a non-equi join, which is O(N²) and intractable on
-- realistic dataset sizes.
--
-- Rewrite strategy: explode each trip's th3index into one row per
-- distinct H3 cell, then the spatial prefilter becomes an **equi-join
-- on the cell column** — which Spark accelerates natively via
-- sort-merge / shuffle-hash.  Each candidate pair is then deduplicated
-- and the expensive `minDistance` runs on the much smaller candidate set.
--
-- At default scale (~10K trips × ~50 distinct cells per trip):
--   * portable q05.sql: ~10⁸ candidate pairs to evaluate
--   * this variant   : ~5×10⁵ candidate pairs (200× reduction before
--                                              the aggregation)
--
-- Run on Spark via `bench/bench_mspark.sh` — the runner auto-prefers
-- `q05_spark.sql` over `q05.sql` when present.  Run on PostgreSQL /
-- DuckDB via the portable `q05.sql` (do NOT use this variant there;
-- PG/Duck would lose the GiST/TRTREE acceleration they actually have).

WITH TripCells AS (
  SELECT /*+ BROADCAST(l, v) */
         l.licence,
         l.licenceId,
         t.tripId,
         t.vehId,
         t.trip,
         explode(th3IndexValues(t.trip_h3)) AS cell
  FROM   QueryLicences l
  JOIN   Vehicles      v ON v.licence = l.licence
  JOIN   Trips         t ON t.vehId   = v.vehId
),
Candidates AS (
  SELECT DISTINCT
         tc1.licence  AS licence1,
         tc2.licence  AS licence2,
         tc1.trip     AS trip1,
         tc2.trip     AS trip2
  FROM   TripCells tc1
  JOIN   TripCells tc2
    ON   tc1.cell      = tc2.cell             -- equi-join on H3 cell
   AND   tc1.licenceId < tc2.licenceId
)
SELECT licence1, licence2,
       MIN(minDistance(trip1, trip2)) AS min_dist
FROM   Candidates
GROUP  BY licence1, licence2
ORDER  BY licence1, licence2;
