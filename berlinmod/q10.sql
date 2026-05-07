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

WITH Temp AS (
  SELECT l.licence AS licence1, t2.vehId AS car2Id,
         whenTrue(tDwithin(t1.trip, t2.trip, 3.0)) AS periods,
         t1.tripId AS tripId1, t2.tripId AS tripId2
  FROM   QueryLicences l
  JOIN   Vehicles v1 ON v1.licence = l.licence
  JOIN   Trips    t1 ON t1.vehId   = v1.vehId
  JOIN   Trips    t2 ON t1.vehId  <> t2.vehId
  WHERE  t2.trip && expandSpace(t1.trip, 3)
)
SELECT licence1, car2Id, periods
FROM   Temp
WHERE  periods IS NOT NULL
ORDER  BY licence1, car2Id, tripId1, tripId2;
