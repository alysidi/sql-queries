-- should only insert 1 row, and filter out duplicates
PREPARE ess_device_info(timestamp, text, text, text, text, text, text, text) AS
  INSERT INTO status.ess_device_info( timestamp_utc, device_id, host_rcpn, device_type, manufacturer, model, version, serial_number)
  SELECT * FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8) AS tmp
  WHERE NOT EXISTS (
      SELECT 1 FROM status.ess_device_info WHERE device_id=$2 AND host_rcpn=$3 AND device_type=$4 AND manufacturer=$5 AND model=$6 AND version=$7 AND serial_number=$8 
  ) LIMIT 1
ON CONFLICT (device_id, host_rcpn, timestamp_utc) DO NOTHING;

EXECUTE ess_device_info('2021-01-26 14:27:21' ,'000100037E2C', '000100072DAB', 'INVERTER', 'Generac3', 'X7603 Islanding Inverter', 'X1217_12610', '000100037E2C')


PREPARE latest_ess_device_info(text, text) AS
-- get latest device info for a device_id
SELECT ( select min(created_timestamp_utc) FROM status.ess_device_info WHERE device_id=$1 and host_rcpn=$2 ) as first_update, 
timestamp_utc as last_updated, device_id, device_type, manufacturer, model, version, serial_number
FROM status.ess_device_info
WHERE device_id=$1 and host_rcpn=$2
    AND created_timestamp_utc=
        (SELECT max(created_timestamp_utc)
         FROM status.ess_device_info
         WHERE device_id=$1 and host_rcpn=$2);

EXECUTE latest_ess_device_info('000100037E2C', '000100072DAB');
EXECUTE latest_ess_device_info('000100037E2C', '000100072FAD');


  -- get latest device info for a device_id
SELECT ( select min(created_timestamp_utc) FROM status.ess_device_info WHERE device_id='000100037E2C' and host_rcpn='000100072FAD' ) as first_update, 
timestamp_utc as last_updated, device_id, device_type, manufacturer, model, version, serial_number
FROM status.ess_device_info
WHERE device_id='000100037E2C' and host_rcpn='000100072FAD'
    AND created_timestamp_utc=
        (SELECT max(created_timestamp_utc)
         FROM status.ess_device_info
         WHERE device_id='000100037E2C' and host_rcpn='000100072FAD');

  

-- individual inverters
WITH DATA AS
    ( SELECT *
     FROM status.device_shadow ds
     JOIN unnest(ARRAY['000100072DAB','000100070664']) host ON ds.host_rcpn = host)


SELECT
    (SELECT min(created_timestamp_utc)
     FROM status.ess_device_info
     WHERE device_id=d.device_id
         AND host_rcpn=d.host_rcpn ) AS first_update,
       e.timestamp_utc AS last_updated,
       d.device_id,   
       d.host_rcpn,
       d.device_type,
       e.manufacturer,
       e.model,
       e.version,
       e.serial_number,
       n.nameplate
FROM DATA d
LEFT JOIN status.ess_device_info e ON d.host_rcpn=e.host_rcpn
AND d.device_id=e.device_id
LEFT JOIN status.nameplate n ON e.device_id = n.device_id
WHERE e.created_timestamp_utc=
        (SELECT max(created_timestamp_utc)
         FROM status.ess_device_info
         WHERE device_id=d.device_id
             AND host_rcpn=d.host_rcpn)
      OR e.created_timestamp_utc IS NULL


   
