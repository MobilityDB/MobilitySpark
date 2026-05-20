-- BerlinMOD Q5: For each pair of query-licence vehicles, the minimum
-- spatial distance ever reached between their trips, irrespective of
-- time. The BerlinMOD spec asks for the minimum distance between the
-- places each vehicle has been; the answer is the spatial-min over the
-- two trajectories.
--
-- Portable: works unchanged on MobilityDB/PostgreSQL, MobilityDuck/DuckDB,
-- and MobilitySpark/Spark SQL.
--
-- Temporal operation used:
--   minDistance(tgeompoint, tgeompoint) → float8
--     Returns the minimum spatial distance reached at any pair of
--     points on the two trajectories, ignoring time. Distinct from
--     nearestApproachDistance, which is the time-synchronous variant.
--
-- Spatial prefilter (th3index): trips whose th3index sequences never
-- agree on a cell at any common instant cannot reach a min distance
-- below the cell edge length at the chosen resolution. At the default
-- resolution 7 (cell edge approximately 1.2 km) the prefilter is sound
-- for distance thresholds well below that edge length; it excludes
-- only pairs whose true min distance is far above the thresholds that
-- BerlinMOD's tDwithin/eDwithin queries care about.
--
--   everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)

SELECT l1.licence AS licence1,
       l2.licence AS licence2,
       MIN(minDistance(t1.trip, t2.trip)) AS min_dist
FROM   QueryLicences l1
JOIN   Vehicles v1  ON  v1.licence = l1.licence
JOIN   Trips    t1  ON  t1.vehId   = v1.vehId
JOIN   QueryLicences l2 ON l1.licenceId < l2.licenceId
JOIN   Vehicles v2  ON  v2.licence = l2.licence
JOIN   Trips    t2  ON  t2.vehId   = v2.vehId
WHERE  everEqTh3IndexTh3Index(t1.trip_h3, t2.trip_h3)
GROUP  BY l1.licence, l2.licence
ORDER  BY l1.licence, l2.licence;
