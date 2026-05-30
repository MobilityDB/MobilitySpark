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
