-- BerlinMOD Q6 — Spark-optimised variant (UNNEST + equi-join on H3 cell).
--
-- Semantic equivalent to q06.sql.  Same input, same output.  Spark-only;
-- PG / DuckDB should run the portable `q06.sql` (their spatial indexes
-- already accelerate it).
--
-- Strategy: explode each truck's th3index into one row per H3 cell, then
-- the spatial prefilter is an equi-join on the cell column.  Spark
-- accelerates equi-joins natively; the expensive `eDwithin` then runs on
-- the much smaller deduplicated candidate set.

WITH TruckCells AS (
  SELECT /*+ BROADCAST(v) */
         v.licence,
         t.tripId,
         t.vehId,
         t.trip,
         explode(th3IndexValues(t.trip_h3)) AS cell
  FROM   Vehicles v
  JOIN   Trips    t ON t.vehId = v.vehId
  WHERE  v.type  = 'truck'
),
Candidates AS (
  SELECT DISTINCT
         tc1.licence AS licence1,
         tc2.licence AS licence2,
         tc1.trip    AS trip1,
         tc2.trip    AS trip2
  FROM   TruckCells tc1
  JOIN   TruckCells tc2
    ON   tc1.cell  = tc2.cell                  -- equi-join on H3 cell
   AND   tc1.vehId < tc2.vehId
)
SELECT licence1, licence2
FROM   Candidates
WHERE  eDwithin(trip1, trip2, 10.0)
ORDER  BY licence1, licence2;
