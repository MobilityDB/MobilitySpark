-- BerlinMOD Q14: Which vehicles were inside a region from QueryRegions at
-- one of the instants from QueryInstants?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   valueAtTimestamp(tgeompoint, timestamptz) → geometry
--   stbox(geometry, timestamptz) → stbox        (index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT r.regionId, i.instantId, i.instant, t.vehId
  FROM   Trips t, QueryRegions r, QueryInstants i
  WHERE  t.trip && stbox(r.geom, i.instant)
    AND  ST_Contains(r.geom, valueAtTimestamp(t.trip, i.instant))
)
SELECT DISTINCT t.regionId, t.instantId, t.instant, v.licence
FROM   Temp t
JOIN   Vehicles v ON t.vehId = v.vehId
ORDER  BY t.regionId, t.instantId, v.licence;
