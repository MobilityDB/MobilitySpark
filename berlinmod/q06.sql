-- BerlinMOD Q6: Pairs of trucks that ever came within 10 m of each other.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   eDwithin(tgeompoint, tgeompoint, float8) → boolean
--     True if the two trips ever came within the given distance of each other.
--
-- Spatial prefilter (th3index): trips whose paths never share a cell at any
-- common instant cannot be within 10 m of each other (cell edge at resolution
-- 7 is ≈ 1.2 km, well above the 10 m threshold).
--
-- MobilityDB operator equivalent: t1.trip |=| t2.trip <= 10.0

SELECT /*+ BROADCAST(v1, v2) */
       v1.licence AS licence1,
       v2.licence AS licence2
FROM   Vehicles v1
JOIN   Trips t1 ON t1.vehId = v1.vehId
JOIN   Vehicles v2 ON v1.vehId < v2.vehId
JOIN   Trips t2 ON t2.vehId = v2.vehId
WHERE  v1.type  = 'truck'
  AND  v2.type  = 'truck'
  AND  everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)
  AND  eDwithin(t1.trip, t2.trip, 10.0)
ORDER  BY v1.licence, v2.licence;
-- The `/*+ BROADCAST(v1, v2) */` block is a Spark SQL hint pinning the
-- small Vehicles tables to every executor.  PostgreSQL and DuckDB treat
-- it as an ordinary block comment.  See berlinmod/README.md "NxN
-- mitigations on Spark".
