-- Spark-dialect override of q06: collect ONE sorted struct array (array_sort orders
-- by the first field tripId, giving the parallel-array consistency PG gets from
-- array_agg(... ORDER BY tripId)), transform to parallel arrays, then LATERAL VIEW
-- explode the eDwithinPairs array<struct>. 0-based indices into 0-based arrays.
WITH Raw AS (
  SELECT array_sort(collect_list(named_struct(
           'k', t.tripId, 'trip', t.trip, 'licence', v.licence, 'vehId', v.vehId))) AS rows
  FROM   Vehicles v JOIN Trips t ON t.vehId = v.vehId
  WHERE  v.type = 'truck'
),
Trucks AS (
  SELECT rows.trip    AS trips,
         rows.licence AS licences,
         rows.vehId   AS vehIds
  FROM   Raw
)
SELECT k.licences[p.i] AS licence1, k.licences[p.j] AS licence2
FROM   Trucks k LATERAL VIEW explode(eDwithinPairs(k.trips, k.trips, 10.0)) _t AS p
WHERE  k.vehIds[p.i] < k.vehIds[p.j]
ORDER  BY licence1, licence2;
