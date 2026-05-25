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
   everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(ST_Transform(r.geom, 4326), 7), t.trip_h3)
   AND eIntersects(t.trip, r.geom)
ORDER  BY v.licence;
