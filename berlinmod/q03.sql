-- Copyright(c) MobilityDB Contributors
-- This file is part of MobilityDB documentation.
-- Licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).
--
-- BerlinMOD Q3: Position of query-licence vehicles at each query instant.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Output convention (binary return):
--   pos is the MEOS hex-WKB encoding of the tgeompoint instant, produced by
--   asHexWKB().  All three platforms call the same MEOS C function
--   (temporal_as_hexwkb, variant 0 = little-endian NDR) so the output is
--   byte-for-byte identical across platforms.

SELECT v.vehId     AS vehid,
       v.licence,
       i.instantId AS instantid,
       asHexWKB(atTime(t.trip, i.instant)) AS pos
FROM   QueryLicences l
JOIN   Vehicles v  ON  v.licence = l.licence
JOIN   Trips    t  ON  t.vehId   = v.vehId
JOIN   QueryInstants i ON true
WHERE  atTime(t.trip, i.instant) IS NOT NULL
ORDER  BY v.vehId, i.instantId, pos;
