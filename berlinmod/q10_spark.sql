-- BerlinMOD Q10 — Spark-optimised variant (UNNEST + equi-join on H3 cell).
--
-- Semantic equivalent to q10.sql.  Same input, same output.  Spark-only.
--
-- The portable q10.sql does `JOIN Trips t2 ON t1.vehId <> t2.vehId WHERE
-- everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)` — a non-equi join with
-- a row-by-row prefilter.  Spark has no spatial index, so this is O(N²)
-- on the full Trips × Trips space.
--
-- This rewrite explodes each trip's th3index into one row per H3 cell,
-- so the spatial prefilter becomes an **equi-join on the cell column**.
-- Then the expensive `tDwithin` runs on the deduplicated candidate set.

WITH TripCells AS (
  SELECT t.tripId, t.vehId, t.trip,
         explode(th3IndexValues(t.trip_h3)) AS cell
  FROM   Trips t
),
QLTripCells AS (
  -- One side of the join: trips of vehicles whose licence is in QueryLicences.
  SELECT /*+ BROADCAST(l, v) */
         l.licence,
         tc.tripId,
         tc.vehId,
         tc.trip,
         tc.cell
  FROM   QueryLicences l
  JOIN   Vehicles      v  ON v.licence = l.licence
  JOIN   TripCells     tc ON tc.vehId  = v.vehId
),
Candidates AS (
  SELECT DISTINCT
         qtc.licence AS licence1,
         tc.vehId    AS car2Id,
         qtc.tripId  AS tripId1,
         tc.tripId   AS tripId2,
         qtc.trip    AS trip1,
         tc.trip     AS trip2
  FROM   QLTripCells qtc
  JOIN   TripCells   tc
    ON   qtc.cell  = tc.cell                  -- equi-join on H3 cell
   AND   qtc.vehId <> tc.vehId
)
SELECT licence1,
       car2Id,
       whenTrue(tDwithin(trip1, trip2, 3.0)) AS periods
FROM   Candidates
WHERE  whenTrue(tDwithin(trip1, trip2, 3.0)) IS NOT NULL
ORDER  BY licence1, car2Id, tripId1, tripId2;
