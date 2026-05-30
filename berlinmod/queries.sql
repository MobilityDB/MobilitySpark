-- BerlinMOD/R benchmark queries — single canonical source shared by all
-- three runners (PostgreSQL psql, DuckDB, MobilitySpark). Each query is
-- delimited by a `-- @query <id>` marker; runners split on the marker and
-- execute each section. The SQL is the portable expression of the fixed
-- BerlinMOD intent; per-engine adaptation (e.g. Spark's preprocessForSpark)
-- is a dialect transform, not a query rewrite.


-- @query q01
-- BerlinMOD Q1: Models of vehicles with licences from QueryLicences.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used: none (pure relational join — baseline portability test).

SELECT l.licence, v.model
FROM   QueryLicences l
JOIN   Vehicles v ON v.licence = l.licence
ORDER  BY l.licence;


-- @query q02
-- BerlinMOD Q2: Licence plates of vehicles that ever entered a query region.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- eIntersects(trip, geom) is true whenever the moving vehicle was inside
-- or on the boundary of the polygon at any instant.
--
-- Spatial prefilter (th3index, polygon-side): geoToH3IndexSet covers the
-- query region with H3 cells at resolution 7; everIntersectsH3IndexSet_Th3Index
-- tests whether the trip's th3index path ever lies in any of those cells.
-- Sound for the eIntersects predicate at any resolution — a trip can only
-- intersect the region if it ever passes through a cell that covers part
-- of it.  On MobilityDB the GiST index on Trips(trip_h3) accelerates the
-- prefilter; on DuckDB / Spark the column is the prefilter mechanism.

SELECT DISTINCT v.licence
FROM   Vehicles v
JOIN   Trips t    ON  t.vehId = v.vehId
JOIN   QueryRegions r ON
   everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(r.geom, 7), t.trip_h3)
   AND eIntersects(t.trip, r.geom)
ORDER  BY v.licence;


-- @query q03
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
ORDER  BY v.vehId, i.instantId;


-- @query q04
-- BerlinMOD Q4: Licence plates of vehicles that ever passed a query point.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   eIntersects(tgeompoint, geometry) → boolean, true if the trip ever intersects geom
--
-- Spatial prefilter (th3index): the trip's th3index sequence (a temporal H3
-- cell index, materialised as the trip_h3 column) must contain the query
-- point's H3 cell at the chosen resolution.  This is a sound prefilter for
-- a point-geometry intersection at any H3 resolution — a trip can only
-- intersect a point if it ever passes through the point's cell.
--
--   COALESCE(everEqH3IndexTh3Index(geomToH3Cell(p.geom, 7), t.trip_h3), TRUE)
--
-- The COALESCE guards against non-POINT geometries (geomToH3Cell returns
-- NULL for those) — falls through to the exact eIntersects.
--
-- MobilityDB operator equivalent:  t.trip && p.geom  (ever-intersects shorthand)
--   On PostgreSQL the GiST index on Trips(trip_h3) accelerates the prefilter;
--   on DuckDB / Spark the th3index column itself is the prefilter mechanism.

SELECT DISTINCT v.licence
FROM   Vehicles v
JOIN   Trips t      ON t.vehId  = v.vehId
JOIN   QueryPoints p ON
   COALESCE(everEqH3IndexTh3Index(geomToH3Cell(p.geom, 7), t.trip_h3), TRUE)
   AND eIntersects(t.trip, p.geom)
ORDER  BY v.licence;


-- @query q05
-- BerlinMOD Q5: For each pair of query-licence vehicles, the minimum
-- spatial distance ever reached between their trips, irrespective of time.
-- The BerlinMOD spec asks for the minimum distance between the places each
-- vehicle has been; the answer is the spatial-min over the two trajectory sets.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operation used:
--   minDistance(tgeompoint[], tgeompoint[]) → float8
--     The set-set spatial minimum distance: the minimum reached between any
--     trip in the first set and any trip in the second, ignoring time.  The
--     kernel prunes far trip pairs by their STBox lower bound, so the N×N is
--     resolved inside one aggregate call rather than a SQL Cartesian join.
--     This is the exact minimum -- the prune never drops the witness pair.

WITH LicTrips AS (
  SELECT l.licence,
         l.licenceId,
         array_agg(t.trip) AS trips
  FROM   QueryLicences l
  JOIN   Vehicles      v ON v.licence = l.licence
  JOIN   Trips         t ON t.vehId   = v.vehId
  GROUP  BY l.licence, l.licenceId )
SELECT a.licence AS licence1,
       b.licence AS licence2,
       minDistance(a.trips, b.trips) AS min_dist
FROM   LicTrips a
JOIN   LicTrips b ON a.licenceId < b.licenceId
ORDER  BY a.licence, b.licence;


-- @query q06
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


-- @query q07
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
       asHexWKB(atTime(t.trip, p.period)) AS pos
FROM   QueryLicences l
JOIN   Vehicles v  ON  v.licence = l.licence
JOIN   Trips    t  ON  t.vehId   = v.vehId
JOIN   QueryPeriods p ON true
WHERE  atTime(t.trip, p.period) IS NOT NULL
ORDER  BY v.vehId, p.periodId, t.tripId;


-- @query q08
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


-- @query qrt
-- BerlinMOD QRT: Binary roundtrip verification.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Protocol: text in, binary out, byte-equal on reception.
--   Each trip was loaded from WKT text (CSV input).
--   asHexWKB() serializes it to the canonical MEOS hex-WKB (variant 0,
--   little-endian NDR) — the same C function on all three platforms.
--   The hex-WKB strings must be byte-for-byte identical across platforms.

SELECT tripId AS tripid,
       asHexWKB(trip) AS trip_hexwkb
FROM   Trips
ORDER  BY tripId;


-- @query q09
-- BerlinMOD Q9: What is the longest distance travelled by a vehicle during
-- each of the periods from QueryPeriods?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint   (restrict to period)
--   length(tgeompoint) → float8                  (Euclidean path length)

WITH Distances AS (
  SELECT p.periodId, p.period, t.vehId,
         SUM(length(atTime(t.trip, p.period))) AS dist
  FROM   Trips t, QueryPeriods p
  WHERE  t.trip && p.period
  GROUP  BY p.periodId, p.period, t.vehId
)
SELECT periodId, period, ROUND(MAX(dist)::numeric, 3) AS maxDist
FROM   Distances
GROUP  BY periodId, period
ORDER  BY periodId;


-- @query q10
-- BerlinMOD Q10: When did the vehicles with licences from QueryLicences meet
-- other vehicles (within 3 m) and what are the other vehicle IDs?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   expandSpace(tgeompoint, float) → stbox      (expand bounding box spatially)
--   tDwithin(tgeompoint, tgeompoint, float) → tbool
--   whenTrue(tbool) → tstzspanset               (intervals when predicate holds)
--
-- Spatial prefilter (th3index): in addition to the existing bbox prefilter
-- t2.trip && expandSpace(t1.trip, 3), we also require the th3index sequences
-- to ever-equal at a common instant.  Both prefilters are sound for a 3 m
-- distance threshold (cell edge ≈ 1.2 km, well above 3 m).

WITH Temp AS (
  SELECT /*+ BROADCAST(l, v1, t1) */
         l.licence AS licence1, t2.vehId AS car2Id,
         whenTrue(tDwithin(t1.trip, t2.trip, 3.0)) AS periods,
         t1.tripId AS tripId1, t2.tripId AS tripId2
  FROM   QueryLicences l
  JOIN   Vehicles v1 ON v1.licence = l.licence
  JOIN   Trips    t1 ON t1.vehId   = v1.vehId
  JOIN   Trips    t2 ON t1.vehId  <> t2.vehId
  WHERE  everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)
    AND  t2.trip && expandSpace(t1.trip, 3)
)
SELECT licence1, car2Id, periods
FROM   Temp
WHERE  periods IS NOT NULL
ORDER  BY licence1, car2Id, tripId1, tripId2;
-- The `/*+ BROADCAST(l, v1, t1) */` block is a Spark SQL hint forcing the
-- QueryLicences-filtered t1 side (~10 vehicles' trips) to be broadcast to
-- every executor, so the t1 × t2 join becomes a broadcast-hash join over
-- the much larger t2.  Without this hint Spark would shuffle 10K × 10K
-- candidate pairs on a non-equi join.  PostgreSQL and DuckDB treat the
-- hint as an ordinary block comment.


-- @query q11
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


-- @query q12
-- BerlinMOD Q12: Which pairs of vehicles were at the same point from
-- QueryPoints at the same instant from QueryInstants?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   valueAtTimestamp(tgeompoint, timestamptz) → geometry
--   stbox(geometry, timestamptz) → stbox        (index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT p.pointId, p.geom, p.geomWKT, i.instantId, i.instant, t.vehId
  FROM   Trips t, QueryPoints p, QueryInstants i
  WHERE  t.trip && stbox(p.geom, i.instant)
    AND  valueAtTimestamp(t.trip, i.instant) = p.geom
)
SELECT DISTINCT t1.pointId, t1.geomWKT AS geom,
       t1.instantId, t1.instant,
       v1.licence AS licence1, v2.licence AS licence2
FROM   Temp t1
JOIN   Vehicles v1 ON t1.vehId = v1.vehId
JOIN   Temp     t2 ON t1.vehId < t2.vehId
                  AND t1.pointId   = t2.pointId
                  AND t1.instantId = t2.instantId
JOIN   Vehicles v2 ON t2.vehId = v2.vehId
ORDER  BY t1.pointId, t1.instantId, licence1, licence2;


-- @query q13
-- BerlinMOD Q13: Which vehicles travelled within a region from QueryRegions
-- during a period from QueryPeriods?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Scale note: the original BerlinMOD uses 10-item subsets for each dimension;
-- applying all 100 QueryRegions × 100 QueryPeriods is ~100× more expensive.
-- This query mirrors the original by using only the first 10 regions and 10 periods.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)
--   stbox(geometry, tstzspan) → stbox           (GiST index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT r.regionId, p.periodId, p.period, t.vehId
  FROM   Trips t, QueryRegions r, QueryPeriods p
  WHERE  r.regionId <= 10 AND p.periodId <= 10
    AND  t.trip && stbox(r.geom, p.period)
    AND  eIntersects(atTime(t.trip, p.period), r.geom)
)
SELECT DISTINCT t.regionId, t.periodId, t.period, v.licence
FROM   Temp t, Vehicles v
WHERE  t.vehId = v.vehId
ORDER  BY t.regionId, t.periodId, v.licence;


-- @query q14
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


-- @query q15
-- BerlinMOD Q15: Which vehicles passed a point from QueryPoints during a
-- period from QueryPeriods?
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Scale note: the original BerlinMOD uses 10-item subsets for each dimension;
-- applying all 100 QueryPoints × 100 QueryPeriods is ~100× more expensive.
-- This query mirrors the original by using only the first 10 points and 10 periods.
--
-- Temporal operations used:
--   atTime(tgeompoint, tstzspan) → tgeompoint
--   eIntersects(tgeompoint, geometry) → bool    (avoids trajectory() override)
--   stbox(geometry, tstzspan) → stbox           (GiST index pre-filter constructor)

WITH Temp AS (
  SELECT DISTINCT pt.pointId, pt.geom, pt.geomWKT, pr.periodId, pr.period, t.vehId
  FROM   Trips t, QueryPoints pt, QueryPeriods pr
  WHERE  pt.pointId  <= 10 AND pr.periodId <= 10
    AND  t.trip && stbox(pt.geom, pr.period)
    AND  eIntersects(atTime(t.trip, pr.period), pt.geom)
)
SELECT DISTINCT t.pointId, t.geomWKT AS geom, t.periodId, t.period, v.licence
FROM   Temp t, Vehicles v
WHERE  t.vehId = v.vehId
ORDER  BY t.pointId, t.periodId, v.licence;


-- @query q16
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


-- @query q17
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

