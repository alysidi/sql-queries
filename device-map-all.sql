
\timing
-- individual inverters -> get device map for all devices
WITH data AS
    (SELECT ds.device_id,
            ds.host_rcpn,
            ds.st,
            ds.device_type,
            ds.timestamp_utc
     FROM status.device_shadow ds
     JOIN (select distinct host_rcpn from status.device_shadow where device_type='INVERTER' ) as inverters ON ds.host_rcpn = inverters.host_rcpn
      )
    

SELECT
    (SELECT min(created_timestamp_utc)
     FROM status.ess_device_info
     WHERE device_id=d.device_id
         AND host_rcpn=d.host_rcpn ) AS first_update,
       d.timestamp_utc AS last_heard,
       d.device_id,   
       d.host_rcpn,
       d.st,
       r.state_text,
       d.device_type,
       e.manufacturer,
       e.model,
       e.version,
       e.serial_number,
       n.nameplate
-- 3 joins      
FROM DATA d
-- mask the last 4 bits as it denotes device type
LEFT JOIN status.rcp_state r ON left(to_hex(d.st)::text, -1) || 0 = to_hex(r.state_code)
-- get ess device meta data
LEFT JOIN status.ess_device_info e ON d.host_rcpn=e.host_rcpn
AND d.device_id=e.device_id
-- get any nameplates
LEFT JOIN status.nameplate n ON d.device_id = n.device_id
-- latest row from ess_device_info
WHERE (e.created_timestamp_utc=
           (SELECT max(created_timestamp_utc)
            FROM status.ess_device_info
            WHERE device_id=d.device_id
                AND host_rcpn=d.host_rcpn)
       OR e.created_timestamp_utc IS NULL) 
-- latest row from nameplates
AND (n.created_timestamp_utc=
         (SELECT max(created_timestamp_utc)
          FROM status.nameplate
          WHERE device_id=d.device_id)
     OR n.created_timestamp_utc IS NULL)  



