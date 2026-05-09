/******************************************************************************
 * BerlinMOD portable SQL — MobilityDB/PostgreSQL data loader
 *
 * Loads the shared berlinmod/data/ CSV files into a PostgreSQL database with
 * the MobilityDB extension.  Run via the companion shell script:
 *
 *   ./berlinmod/run_mbdb.sh [dbname]
 *
 * Or manually (psql must be run from the repository root):
 *
 *   psql -d <dbname> \
 *     -v v_csv=berlinmod/data/vehicles.csv \
 *     -v t_csv=berlinmod/data/trips.csv \
 *     -v ql_csv=berlinmod/data/query_licences.csv \
 *     -v qi_csv=berlinmod/data/query_instants.csv \
 *     -v qp_csv=berlinmod/data/query_points.csv \
 *     -f berlinmod/load_mbdb.sql
 ******************************************************************************/

CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;

-------------------------------------------------------------------------------
-- Drop and recreate all tables
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS QueryRegions  CASCADE;
DROP TABLE IF EXISTS QueryPeriods  CASCADE;
DROP TABLE IF EXISTS QueryPoints   CASCADE;
DROP TABLE IF EXISTS QueryInstants CASCADE;
DROP TABLE IF EXISTS QueryLicences CASCADE;
DROP TABLE IF EXISTS Trips         CASCADE;
DROP TABLE IF EXISTS Vehicles      CASCADE;

CREATE TABLE Vehicles (
  vehId    INTEGER PRIMARY KEY,
  licence  TEXT    NOT NULL,
  type     TEXT    NOT NULL,
  model    TEXT    NOT NULL
);

CREATE TABLE Trips (
  tripId   INTEGER    PRIMARY KEY,
  vehId    INTEGER    NOT NULL REFERENCES Vehicles(vehId),
  trip     tgeompoint NOT NULL
);

CREATE TABLE QueryLicences (
  licenceId INTEGER PRIMARY KEY,
  licence   TEXT    NOT NULL
);

CREATE TABLE QueryInstants (
  instantId INTEGER     PRIMARY KEY,
  instant   TIMESTAMPTZ NOT NULL
);

CREATE TABLE QueryPoints (
  pointId INTEGER              PRIMARY KEY,
  geom    geometry(Point,3857) NOT NULL
);

CREATE TABLE QueryRegions (
  regionId INTEGER                PRIMARY KEY,
  geom     geometry(Polygon,3857) NOT NULL
);

CREATE TABLE QueryPeriods (
  periodId INTEGER    PRIMARY KEY,
  period   tstzspan   NOT NULL
);

-------------------------------------------------------------------------------
-- Load non-temporal tables directly from CSV
-- Paths are substituted by run_mbdb.sh (DATADIR placeholder)
-------------------------------------------------------------------------------

\copy Vehicles      FROM 'DATADIR/vehicles.csv'       DELIMITER ',' CSV HEADER
\copy QueryLicences FROM 'DATADIR/query_licences.csv' DELIMITER ',' CSV HEADER
\copy QueryInstants FROM 'DATADIR/query_instants.csv' DELIMITER ',' CSV HEADER

-------------------------------------------------------------------------------
-- Load Trips: read EWKB hex text, convert to tgeompoint
-------------------------------------------------------------------------------

CREATE TEMP TABLE TripsTmp (tripId INTEGER, vehId INTEGER, trip TEXT);
\copy TripsTmp FROM 'DATADIR/trips.csv' DELIMITER ',' CSV HEADER
INSERT INTO Trips SELECT tripId, vehId, tgeompointfromhexewkb(trip) FROM TripsTmp;
DROP TABLE TripsTmp;

-------------------------------------------------------------------------------
-- Load QueryPoints: read WKT text, parse with ST_GeomFromText (SRID 0)
-------------------------------------------------------------------------------

CREATE TEMP TABLE QueryPointsTmp (pointId INTEGER, geom TEXT);
\copy QueryPointsTmp FROM 'DATADIR/query_points.csv' DELIMITER ',' CSV HEADER
INSERT INTO QueryPoints SELECT pointId, ST_GeomFromText(geom, 3857) FROM QueryPointsTmp;
DROP TABLE QueryPointsTmp;

-------------------------------------------------------------------------------
-- Load QueryRegions: read WKT text, parse polygon geometry
-------------------------------------------------------------------------------

CREATE TEMP TABLE QueryRegionsTmp (regionId INTEGER, geom TEXT);
\copy QueryRegionsTmp FROM 'DATADIR/query_regions.csv' DELIMITER ',' CSV HEADER
INSERT INTO QueryRegions SELECT regionId, ST_GeomFromText(geom, 3857) FROM QueryRegionsTmp;
DROP TABLE QueryRegionsTmp;

-------------------------------------------------------------------------------
-- Load QueryPeriods: read period text, cast to tstzspan
-------------------------------------------------------------------------------

CREATE TEMP TABLE QueryPeriodsTmp (periodId INTEGER, period TEXT);
\copy QueryPeriodsTmp FROM 'DATADIR/query_periods.csv' DELIMITER ',' CSV HEADER
INSERT INTO QueryPeriods SELECT periodId, period::tstzspan FROM QueryPeriodsTmp;
DROP TABLE QueryPeriodsTmp;

-------------------------------------------------------------------------------
-- Optional GiST indexes for performance on larger datasets
-------------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS trips_trip_gist_idx    ON Trips       USING GIST(trip);
CREATE INDEX IF NOT EXISTS qp_geom_gist_idx        ON QueryPoints USING GIST(geom);
CREATE INDEX IF NOT EXISTS qr_geom_gist_idx        ON QueryRegions USING GIST(geom);
CREATE INDEX IF NOT EXISTS qper_period_gist_idx    ON QueryPeriods USING GIST(period);

ANALYZE Vehicles, Trips, QueryLicences, QueryInstants, QueryPoints,
        QueryRegions, QueryPeriods;
