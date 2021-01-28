-- should only insert 1 row, and filter out duplicates
PREPARE nameplate(timestamp, text, text, jsonb) AS
  INSERT INTO status.nameplate( timestamp_utc, device_id, device_type, nameplate)
  SELECT * FROM (SELECT $1, $2, $3, $4) AS tmp
  WHERE NOT EXISTS (
      SELECT 1 FROM status.nameplate WHERE device_id=$2 AND device_type=$3 AND nameplate=$4
  ) LIMIT 1
  ON CONFLICT (device_id, timestamp_utc) DO NOTHING;

EXECUTE nameplate('2021-01-26 17:22:10' ,'000100038375', 'PVLINK', '{"w_rtg": 7600, "va_rtg": 7600, "a_rtg": 99.0}')


-- get latest nameplate for a device_id
SELECT *
FROM status.nameplate
WHERE device_id='000100038375'
    AND created_timestamp_utc=
        (SELECT max(created_timestamp_utc)
         FROM status.nameplate
         WHERE device_id='000100038375')