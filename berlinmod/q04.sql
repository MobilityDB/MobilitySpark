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
   COALESCE(ever_eq(geoToH3Cell(ST_Transform(p.geom, 4326), 7), t.trip_h3), TRUE)
   AND eIntersects(t.trip, p.geom)
ORDER  BY v.licence;
