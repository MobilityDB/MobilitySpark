-- Copyright(c) MobilityDB Contributors
-- This file is part of MobilityDB documentation.
-- Licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).
--
-- BerlinMOD Q2: Licence plates of vehicles that ever entered a query region.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- eIntersects(trip, geom) is true whenever the moving vehicle was inside
-- or on the boundary of the polygon at any instant.

SELECT DISTINCT v.licence
FROM   Vehicles v
JOIN   Trips t    ON  t.vehId = v.vehId
JOIN   QueryRegions r ON eIntersects(t.trip, r.geom)
ORDER  BY v.licence;
