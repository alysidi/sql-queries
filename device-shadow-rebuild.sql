
-------------
BEGIN; 
CREATE TABLE status.device_shadow_bak AS SELECT * FROM status.device_shadow;
TRUNCATE TABLE status.device_shadow;
INSERT INTO status.device_shadow SELECT * FROM status.device_shadow_bak ON CONFLICT DO NOTHING;
DROP table status.device_shadow_bak;
COMMIT;

-------------


BEGIN;

DROP TABLE status.device_shadow2;

-- create a copy of device shadow structure
CREATE TABLE IF NOT EXISTS status.device_shadow2 ( LIKE status.device_shadow INCLUDING ALL );

-- add options 
ALTER TABLE status.device_shadow2
  SET (fillfactor = 70,
       autovacuum_enabled=true,
       autovacuum_vacuum_scale_factor=0.01,
       toast.autovacuum_enabled=true,
       toast.autovacuum_vacuum_scale_factor=0.01);

-- copy data to new table
INSERT INTO status.device_shadow2
SELECT * FROM status.device_shadow;

-- check data in new table
SELECT * FROM status.device_shadow2 limit 11;


COMMIT;


-- rename shadow to bak
ALTER TABLE status.device_shadow
RENAME TO device_shadow_bak;

-- rename shadow2 to shadow
ALTER TABLE status.device_shadow2
RENAME TO device_shadow;

-- DROP status.device_shadow_bak






