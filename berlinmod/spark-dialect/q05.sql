-- Spark-dialect override of q05 (see BerlinMODBench): array_agg -> collect_list
-- (no ordered aggregate needed here); minDistance is the scalar array UDF. Same
-- MEOS computation and result as the canonical q05.
WITH LicenceTrips AS (
  SELECT l.licenceId, l.licence, collect_list(t.trip) AS trips
  FROM   QueryLicences l
  JOIN   Vehicles v ON v.licence = l.licence
  JOIN   Trips    t ON t.vehId   = v.vehId
  GROUP  BY l.licenceId, l.licence
)
SELECT a.licence AS licence1, b.licence AS licence2,
       minDistance(a.trips, b.trips) AS min_dist
FROM   LicenceTrips a
JOIN   LicenceTrips b ON a.licenceId < b.licenceId
ORDER  BY licence1, licence2;
