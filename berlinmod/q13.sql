-- Copyright(c) MobilityDB Contributors
-- This file is part of MobilityDB documentation.
-- Licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).
--
-- BerlinMOD Q13: Which vehicles travelled within a region from QueryRegions
-- during a period from QueryPeriods?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Scale note: the original BerlinMOD uses 10-item subsets for each dimension;
-- applying all 100 QueryRegions × 100 QueryPeriods is ~100× more expensive.
-- This query mirrors the original by using only the first 10 regions and 10 periods.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)
--   stbox(geometry, tstzspan) → stbox           (GiST index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT r.regionId, p.periodId, p.period, t.vehId
  FROM   Trips t, QueryRegions r, QueryPeriods p
  WHERE  r.regionId <= 10 AND p.periodId <= 10
    AND  overlaps(t.trip, stbox(r.geom, p.period))
    AND  eIntersects(atTime(t.trip, p.period), r.geom)
)
SELECT DISTINCT t.regionId, t.periodId, t.period, v.licence
FROM   Temp t, Vehicles v
WHERE  t.vehId = v.vehId
ORDER  BY t.regionId, t.periodId, v.licence;
