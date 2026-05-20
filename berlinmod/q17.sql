-- BerlinMOD Q17: Which point(s) from QueryPoints have been visited by the
-- maximum number of distinct vehicles?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)

WITH PointCount AS (
  SELECT p.pointId, COUNT(DISTINCT t.vehId) AS hits
  FROM   Trips t, QueryPoints p
  WHERE  eIntersects(t.trip, p.geom)
  GROUP  BY p.pointId
)
SELECT pointId, hits
FROM   PointCount
WHERE  hits = (SELECT MAX(hits) FROM PointCount)
ORDER  BY pointId;
