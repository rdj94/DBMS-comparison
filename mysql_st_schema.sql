START TRANSACTION;
DROP TABLE IF EXISTS st_trackpoint_no_index;
DROP TABLE IF EXISTS st_trackpoint_indexed;
CREATE TABLE st_trackpoint_no_index (
  tp_id SERIAL PRIMARY KEY,
  tp_user INTEGER NOT NULL,
  tp_point GEOMETRY NOT NULL,
  tp_altitude FLOAT,
  tp_date VARCHAR(32),
  tp_time VARCHAR(32)
);
CREATE TABLE st_trackpoint_indexed (
  tp_id SERIAL PRIMARY KEY,
  tp_user INTEGER NOT NULL,
  tp_point GEOMETRY NOT NULL,
  SPATIAL INDEX(tp_point),
  tp_altitude FLOAT,
  tp_date VARCHAR(32),
  tp_time VARCHAR(32)
);
COMMIT;