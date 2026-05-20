-- BerlinMOD Q16 — Spark-optimised variant (UNNEST + equi-join on H3 cell).
--
-- Semantic equivalent to q16.sql.  Same input, same output.  Spark-only.
--
-- Q16's portable form joins two licence-filtered Trips sets with three
-- additional small-dim crosses (QueryPeriods, QueryRegions).  Even
-- restricted to the first 10 of each dim, the t1 × t2 leg is O(L²)
-- where L = ~10 trips per query-licence vehicle, but the bbox-overlap
-- prefilter against `stbox(r.geom, p.period)` must be evaluated for
-- every (t1, t2, r, p) tuple.
--
-- This rewrite folds the h3 cell prefilter in via UNNEST + equi-join,
-- shrinking the candidate-pair count before the bbox / aDisjoint
-- evaluation.  Region/period bbox checks remain unchanged because
-- they're already small-dim joins amenable to broadcast.

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
  WHERE  l.licenceId <= 10
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
SELECT /*+ BROADCAST(p, r) */
       p.periodId, p.period, r.regionId,
       c.licence1, c.licence2
FROM   Candidates    c
JOIN   QueryPeriods  p ON true
JOIN   QueryRegions  r ON true
WHERE  p.periodId <= 10
  AND  r.regionId <= 10
  AND  c.trip1 && stbox(r.geom, p.period)
  AND  c.trip2 && stbox(r.geom, p.period)
  AND  eIntersects(atTime(c.trip1, p.period), r.geom)
  AND  eIntersects(atTime(c.trip2, p.period), r.geom)
  AND  aDisjoint(atTime(c.trip1, p.period), atTime(c.trip2, p.period))
ORDER  BY p.periodId, r.regionId, c.licence1, c.licence2;
