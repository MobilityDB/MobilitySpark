-- BerlinMOD Q11: Which vehicles passed a point from QueryPoints at one of
-- the instants from QueryInstants?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   valueAtTimestamp(tgeompoint, timestamptz) → geometry
--   stbox(geometry, timestamptz) → stbox        (index pre-filter constructor)

WITH Temp AS (
  SELECT p.pointId, p.geom, p.geomWKT, i.instantId, i.instant, t.vehId
  FROM   Trips t, QueryPoints p, QueryInstants i
  WHERE  t.trip && stbox(p.geom, i.instant)
    AND  valueAtTimestamp(t.trip, i.instant) = p.geom
)
SELECT t.pointId, t.geomWKT AS geom, t.instantId, t.instant, v.licence
FROM   Temp t
JOIN   Vehicles v ON t.vehId = v.vehId
ORDER  BY t.pointId, t.instantId, v.licence;
