-- BerlinMOD Q15: Which vehicles passed a point from QueryPoints during a
-- period from QueryPeriods?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Scale note: the original BerlinMOD uses 10-item subsets for each dimension;
-- applying all 100 QueryPoints × 100 QueryPeriods is ~100× more expensive.
-- This query mirrors the original by using only the first 10 points and 10 periods.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)
--   stbox(geometry, tstzspan) → stbox           (GiST index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT pt.pointId, pt.geom, pt.geomWKT, pr.periodId, pr.period, t.vehId
  FROM   Trips t, QueryPoints pt, QueryPeriods pr
  WHERE  pt.pointId  <= 10 AND pr.periodId <= 10
    AND  t.trip && stbox(pt.geom, pr.period)
    AND  eIntersects(atTime(t.trip, pr.period), pt.geom)
)
SELECT DISTINCT t.pointId, t.geomWKT AS geom, t.periodId, t.period, v.licence
FROM   Temp t, Vehicles v
WHERE  t.vehId = v.vehId
ORDER  BY t.pointId, t.periodId, v.licence;
