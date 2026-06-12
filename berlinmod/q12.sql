-- Copyright(c) MobilityDB Contributors
-- This file is part of MobilityDB documentation.
-- Licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).
--
-- BerlinMOD Q12: Which pairs of vehicles were at the same point from
-- QueryPoints at the same instant from QueryInstants?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   valueAtTimestamp(tgeompoint, timestamptz) → geometry
--   stbox(geometry, timestamptz) → stbox        (index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT p.pointId, p.geom, p.geomWKT, i.instantId, i.instant, t.vehId
  FROM   Trips t, QueryPoints p, QueryInstants i
  WHERE  overlaps(t.trip, stbox(p.geom, i.instant))
    AND  valueAtTimestamp(t.trip, i.instant) = p.geom
)
SELECT DISTINCT t1.pointId, t1.geomWKT AS geom,
       t1.instantId, t1.instant,
       v1.licence AS licence1, v2.licence AS licence2
FROM   Temp t1
JOIN   Vehicles v1 ON t1.vehId = v1.vehId
JOIN   Temp     t2 ON t1.vehId < t2.vehId
                  AND t1.pointId   = t2.pointId
                  AND t1.instantId = t2.instantId
JOIN   Vehicles v2 ON t2.vehId = v2.vehId
ORDER  BY t1.pointId, t1.instantId, v1.licence, v2.licence;
