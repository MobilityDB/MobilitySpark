-- BerlinMOD Q7: Trip portions of query-licence vehicles during each query period.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Output convention (binary return):
--   pos is the MEOS hex-WKB encoding of the restricted tgeompoint sequence,
--   produced by asHexWKB(). All three platforms call the same MEOS C function
--   so the output is byte-for-byte identical across platforms.

SELECT v.vehId     AS vehid,
       v.licence,
       p.periodId  AS periodid,
       asHexEWKB(atTime(t.trip, p.period)) AS pos
FROM   QueryLicences l
JOIN   Vehicles v  ON  v.licence = l.licence
JOIN   Trips    t  ON  t.vehId   = v.vehId
JOIN   QueryPeriods p ON true
WHERE  atTime(t.trip, p.period) IS NOT NULL
ORDER  BY v.vehId, p.periodId, t.tripId;
