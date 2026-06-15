-- Spark-dialect override of q10: same struct-collect/sort/transform per side, then
-- LATERAL VIEW explode the tDwithinPairs array<struct<i,j,periods>>.
WITH SelRaw AS (
  SELECT array_sort(collect_list(named_struct(
           'k', t.tripId, 'trip', t.trip, 'licence', l.licence, 'vehId', v.vehId))) AS rows
  FROM   QueryLicences l JOIN Vehicles v ON v.licence = l.licence
  JOIN   Trips t ON t.vehId = v.vehId
),
Sel AS (
  SELECT transform(rows, x -> x.trip) AS trips, transform(rows, x -> x.licence) AS licences,
         transform(rows, x -> x.vehId) AS vehIds, transform(rows, x -> x.k) AS tripIds FROM SelRaw
),
AllRaw AS (
  SELECT array_sort(collect_list(named_struct('k', t.tripId, 'trip', t.trip, 'vehId', t.vehId))) AS rows
  FROM   Trips t
),
AllTrips AS (
  SELECT transform(rows, x -> x.trip) AS trips, transform(rows, x -> x.vehId) AS vehIds,
         transform(rows, x -> x.k) AS tripIds FROM AllRaw
)
SELECT s.licences[p.i] AS licence1, a.vehIds[p.j] AS car2Id, p.periods AS periods
FROM   Sel s, AllTrips a LATERAL VIEW explode(tDwithinPairs(s.trips, a.trips, 3.0)) _t AS p
WHERE  s.vehIds[p.i] <> a.vehIds[p.j]
ORDER  BY licence1, car2Id, s.tripIds[p.i], a.tripIds[p.j];
