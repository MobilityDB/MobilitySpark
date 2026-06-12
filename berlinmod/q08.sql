-- Copyright(c) MobilityDB Contributors
-- This file is part of MobilityDB documentation.
-- Licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).
--
-- BerlinMOD Q8: Trajectory of each vehicle as a hex-WKB geometry string.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- trajectory() collapses a tgeompoint sequence into its spatial path
-- (LINESTRING for a sequence, POINT for a single instant).  Both PostgreSQL
-- COPY and DuckDB COPY serialize the GEOMETRY type as hex WKB in CSV output,
-- and MobilitySpark's trajectory() UDF produces the same format via
-- geo_as_hexewkb(), so the output is byte-for-byte identical across platforms.

SELECT tripId AS tripid,
       trajectory(trip) AS traj
FROM   Trips
ORDER  BY tripId;
