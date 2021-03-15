\timing
-- individual inverters -> get device map for all devices
WITH data AS
    ( 
        SELECT device_id, host_rcpn, device_type
        FROM status.ess_device_info
        WHERE host_rcpn IN ('0001000720D0','000100071818', '000100073643') 
          
        UNION
        SELECT device_id, host_rcpn, device_type
        FROM status.device_shadow
        WHERE host_rcpn IN ('0001000720D0','000100071818', '000100073643') 
         
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
WHERE (e.updated_timestamp_utc=
           (SELECT max(updated_timestamp_utc)
            FROM status.ess_device_info
            WHERE device_id=d.device_id
                AND host_rcpn=d.host_rcpn)
       OR e.updated_timestamp_utc IS NULL) 
-- latest row from nameplates
AND (n.updated_timestamp_utc=
         (SELECT max(updated_timestamp_utc)
          FROM status.nameplate
          WHERE device_id=d.device_id)
     OR n.updated_timestamp_utc IS NULL)  


