-- Copyright(c) MobilityDB Contributors
-- This file is part of MobilityDB documentation.
-- Licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).
--
-- BerlinMOD Q14: Which vehicles were inside a region from QueryRegions at
-- one of the instants from QueryInstants?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL. Every predicate is a MobilityDB spatiotemporal
-- relationship backed by MEOS (GEOS), so the result is byte-identical on all
-- three engines with no dependency on a platform spatial library (PostGIS /
-- duckdb_spatial / Sedona).
--
-- Temporal operations used:
--   atTime(tgeompoint, timestamptz) → tgeompoint  (restrict to one instant)
--   eContains(geometry, tgeompoint) → boolean      (boundary-excluding, ST_Contains)
--   stbox(geometry, timestamptz) → stbox           (index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT r.regionId, i.instantId, i.instant, t.vehId
  FROM   Trips t, QueryRegions r, QueryInstants i
  WHERE  overlaps(t.trip, stbox(r.geom, i.instant))
    AND  eContains(r.geom, atTime(t.trip, i.instant))
)
SELECT DISTINCT t.regionId, t.instantId, t.instant, v.licence
FROM   Temp t
JOIN   Vehicles v ON t.vehId = v.vehId
ORDER  BY t.regionId, t.instantId, v.licence;
