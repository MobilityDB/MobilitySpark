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

-- Trips: cast the WKT string column to tgeompoint
CREATE TABLE Trips AS
  SELECT tripId,
         vehId,
         CAST(trip AS tgeompoint) AS trip
  FROM read_csv(getvariable('DATADIR') || 'trips.csv', header = true);

-- QueryPoints: cast the WKT string column to DuckDB geometry
CREATE TABLE QueryPoints AS
  SELECT pointId,
         ST_GeomFromText(geom) AS geom
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
