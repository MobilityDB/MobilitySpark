-- BerlinMOD Q5: For each pair of query-licence vehicles, the minimum distance
-- ever reached between their trips.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operations used:
--   nearestApproachDistance(tgeompoint, tgeompoint) → float8
--     Returns the minimum Euclidean distance reached at any common instant.
--     Returns NULL when the trips have no overlapping time extent.
--
-- MobilityDB operator equivalent:  t1.trip |=| t2.trip
-- NOTE: MobilityPySpark called this min_distance() / nearest_approach_distance();
--       the canonical portable name is nearestApproachDistance() (MEOS convention).
--
-- Semantic note: the BerlinMOD spec asks for "minimum distance between places
-- where each vehicle has been" — a purely spatial question answered by
-- ST_Distance(trajectory1, trajectory2).  This portable version asks the
-- synchronous question: minimum distance at any shared instant.  The two
-- answers differ when vehicles visit the same place at different times.
-- The synchronous interpretation is more meaningful for collision analysis
-- and is the only one natively supported across all three backends.
--
-- Spatial prefilter (th3index): trips whose th3index sequences never
-- agree on a cell at any common instant cannot reach a min distance below
-- the cell edge length at the chosen resolution.  At the default resolution
-- 7 (cell edge ≈ 1.2 km), this is sound for distance thresholds well below
-- the cell edge — BerlinMOD's tDwithin/eDwithin distances are 3-10 m, so
-- the prefilter excludes only pairs whose true min distance is far above
-- those thresholds (and would not appear as MIN of any practical query).
--
--   everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)
--
-- On MobilityDB/PostgreSQL the GiST index on Trips(trip_h3) and on
-- Trips(trip) accelerate the cross-join (the planner pushes the cell
-- predicate into an index seek).  On DuckDB / Spark the column itself is
-- the prefilter machinery.

SELECT l1.licence AS licence1,
       l2.licence AS licence2,
       MIN(nearestApproachDistance(t1.trip, t2.trip)) AS min_dist
FROM   QueryLicences l1
JOIN   Vehicles v1  ON  v1.licence = l1.licence
JOIN   Trips    t1  ON  t1.vehId   = v1.vehId
JOIN   QueryLicences l2 ON l1.licenceId < l2.licenceId
JOIN   Vehicles v2  ON  v2.licence = l2.licence
JOIN   Trips    t2  ON  t2.vehId   = v2.vehId
WHERE  everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)
  AND  nearestApproachDistance(t1.trip, t2.trip) IS NOT NULL
GROUP  BY l1.licence, l2.licence
ORDER  BY l1.licence, l2.licence;
