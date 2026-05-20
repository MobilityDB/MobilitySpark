/******************************************************************************
 * BerlinMOD portable SQL — MobilityDuck/DuckDB data loader
 *
 * Loads the shared berlinmod/data/ CSV files into DuckDB using the
 * MobilityDuck extension.  Run from the repo root:
 *
 *   duckdb :memory: -s ".read berlinmod/load_mduck.sql"
 *
 * Or pass DATADIR via substitution — DuckDB has no native \copy variables,
 * so the paths below assume execution from the repository root (default).
 * Change DATADIR in the first assignment if running from another directory.
 ******************************************************************************/

-- Path to the CSV data files (relative to the working directory)
SET VARIABLE DATADIR = 'berlinmod/data/';

-- Match the Europe/Berlin timezone used when generating the expected output files
LOAD icu;
SET TimeZone = 'Europe/Berlin';

-------------------------------------------------------------------------------
-- Drop and recreate all tables
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS QueryRegions;
DROP TABLE IF EXISTS QueryPeriods;
DROP TABLE IF EXISTS QueryPoints;
DROP TABLE IF EXISTS QueryInstants;
DROP TABLE IF EXISTS QueryLicences;
DROP TABLE IF EXISTS Trips;
DROP TABLE IF EXISTS Vehicles;

CREATE TABLE Vehicles AS
  SELECT * FROM read_csv(getvariable('DATADIR') || 'vehicles.csv', header = true);

CREATE TABLE QueryLicences AS
  SELECT * FROM read_csv(getvariable('DATADIR') || 'query_licences.csv', header = true);

CREATE TABLE QueryInstants AS
  SELECT instantId,
         CAST(instant AS TIMESTAMPTZ) AS instant
  FROM read_csv(getvariable('DATADIR') || 'query_instants.csv', header = true);

-- Trips: load raw text first, then convert to tgeompoint in a second pass.
-- DuckDB 1.4.x parallel read_csv does not invoke the scalar-function wrapper
-- that calls EnsureMeosInitializedOnThread(), so calling tgeompointFromHexWKB
-- inline in the SELECT causes a SIGSEGV on worker threads.  Two-step loading
-- keeps the CSV scan (multi-threaded) separate from the MEOS conversion
-- (also multi-threaded but through the normal expression pipeline, which
-- does invoke the wrapper).
--
-- The trip_h3 column is a temporal H3-cell index of the trip, used by the
-- portable BerlinMOD SQL as a spatial prefilter.  We always recompute it
-- from the loaded tgeompoint at H3 resolution 7, so the column is consistent
-- across all three platforms regardless of whether the source CSV included
-- a precomputed trip_h3 column.  read_csv's explicit columns parameter
-- reads only the first 3 columns (tripId, vehId, trip) and ignores any
-- extra header column.
CREATE TEMP TABLE TripsRaw AS
  SELECT tripId, vehId, trip
  FROM read_csv(getvariable('DATADIR') || 'trips.csv',
                header = true,
                columns = {'tripId': 'INTEGER', 'vehId': 'INTEGER', 'trip': 'VARCHAR'});

CREATE TABLE Trips AS
  SELECT tripId,
         vehId,
         tgeompointFromHexWKB(trip)                            AS trip,
         tgeompoint_to_th3index(tgeompointFromHexWKB(trip), 7) AS trip_h3
  FROM TripsRaw;

-- QueryPoints: geometry for spatial ops + original WKT string for portable display
CREATE TABLE QueryPoints AS
  SELECT pointId,
         ST_GeomFromText(geom) AS geom,
         geom AS geomWKT
  FROM read_csv(getvariable('DATADIR') || 'query_points.csv', header = true);

-- QueryRegions: cast the WKT polygon string to DuckDB geometry
CREATE TABLE QueryRegions AS
  SELECT regionId,
         ST_GeomFromText(geom) AS geom
  FROM read_csv(getvariable('DATADIR') || 'query_regions.csv', header = true);

-- QueryPeriods: cast the period string to tstzspan
CREATE TABLE QueryPeriods AS
  SELECT periodId,
         CAST(period AS tstzspan) AS period
  FROM read_csv(getvariable('DATADIR') || 'query_periods.csv', header = true);

-- portable schema — shadows trajectory() to return hex-WKB text instead of
-- GEOMETRY, so DuckDB COPY serializes it as a string (byte-for-byte identical
-- to MobilityDB and MobilitySpark).  main.trajectory() is schema-qualified to
-- avoid macro recursion.  run_mduck.sh inlines this file per query, so
-- SET search_path persists for each query's DuckDB session.
CREATE SCHEMA IF NOT EXISTS portable;
CREATE OR REPLACE MACRO portable.trajectory(t) AS ST_AsHexWKB(main.trajectory(t));
SET search_path='portable,main';
