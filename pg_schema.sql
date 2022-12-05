-- Schema for the PostgreSQL version of the experiment
-- As date/time is not used, we don't need to convert it to a better format.
BEGIN TRANSACTION;
DROP TABLE IF EXISTS trackpoint_no_index;
DROP TABLE IF EXISTS trackpoint_indexed;
-- Unindexed version
CREATE TABLE trackpoint_no_index (
  tp_id SERIAL PRIMARY KEY,
  tp_user INTEGER NOT NULL,
  tp_point GEOMETRY NOT NULL,
  tp_altitude FLOAT,
  tp_date VARCHAR(32),
  tp_time VARCHAR(32)
);
-- Indexed version
CREATE TABLE trackpoint_indexed (
  tp_id SERIAL PRIMARY KEY,
  tp_user INTEGER NOT NULL,
  tp_point GEOMETRY NOT NULL,
  tp_altitude FLOAT,
  tp_date VARCHAR(32),
  tp_time VARCHAR(32)
);
CREATE INDEX trackpoint_geom_index ON trackpoint_indexed USING GIST (tp_point);
COMMIT TRANSACTION;