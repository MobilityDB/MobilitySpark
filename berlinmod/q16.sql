-- BerlinMOD Q16: Which pairs of query-licence vehicles were both within a
-- region from QueryRegions during a period from QueryPeriods, but never at
-- the same location at the same time (always disjoint)?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Scale note: the original BerlinMOD uses 10-item subsets for each dimension;
-- applying all 100 QueryLicences × 100 QueryPeriods × 100 QueryRegions is
-- ~10,000× more expensive.  This query mirrors the original by using only the
-- first 10 licences, 10 periods, and 10 regions.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)
--   aDisjoint(tgeompoint, tgeompoint) → bool    (always spatially disjoint)
--   stbox(geometry, tstzspan) → stbox           (GiST index pre-filter)

SELECT /*+ BROADCAST(l1, v1, l2, v2, p, r) */
       p.periodId, p.period, r.regionId,
       l1.licence AS licence1, l2.licence AS licence2
FROM   QueryLicences l1
JOIN   Vehicles      v1 ON v1.licence = l1.licence
JOIN   Trips         t1 ON t1.vehId   = v1.vehId
JOIN   QueryLicences l2 ON l1.licenceId < l2.licenceId
JOIN   Vehicles      v2 ON v2.licence = l2.licence
JOIN   Trips         t2 ON t2.vehId   = v2.vehId
JOIN   QueryPeriods  p  ON true
JOIN   QueryRegions  r  ON true
WHERE  l1.licenceId <= 10 AND l2.licenceId <= 10
  AND  p.periodId   <= 10
  AND  r.regionId   <= 10
  AND  t1.trip && stbox(r.geom, p.period)
  AND  t2.trip && stbox(r.geom, p.period)
  AND  eIntersects(atTime(t1.trip, p.period), r.geom)
  AND  eIntersects(atTime(t2.trip, p.period), r.geom)
  AND  aDisjoint(atTime(t1.trip, p.period), atTime(t2.trip, p.period))
ORDER  BY p.periodId, r.regionId, l1.licence, l2.licence;
