\timing
-- individual inverters -> get device map for all devices
WITH data AS
    ( 
        SELECT device_id, host_rcpn, device_type
        FROM status.ess_device_info
        JOIN unnest(ARRAY[ '000100073466', '000100071818' ]) 
           host ON host_rcpn = host 
        UNION
        SELECT device_id, host_rcpn, device_type
        FROM status.device_shadow
        JOIN unnest(ARRAY[ '000100073466', '000100071818' ]) 
           host ON host_rcpn = host 
      )

SELECT
    (SELECT min(created_timestamp_utc)
     FROM status.ess_device_info
     WHERE device_id=d.device_id
         AND host_rcpn=d.host_rcpn ) AS first_update,
       s.timestamp_utc AS last_heard,
       d.device_id,   
       d.host_rcpn,
       s.st,
       r.state_text,
       d.device_type,
       e.manufacturer,
       e.model,
       e.version,
       e.serial_number,
       n.nameplate
-- 3 joins      
FROM DATA d

-- get ess device meta data
LEFT JOIN status.ess_device_info e ON d.host_rcpn=e.host_rcpn AND d.device_id=e.device_id
-- get shadow data
LEFT JOIN status.device_shadow s ON d.host_rcpn=s.host_rcpn AND d.device_id=s.device_id
-- mask the last 4 bits as it denotes device type
LEFT JOIN status.rcp_state r ON left(to_hex(s.st)::text, -1) || 0 = to_hex(r.state_code)
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

