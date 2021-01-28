
\timing
-- individual inverters
WITH DATA AS
    ( SELECT device_id, host_rcpn, st, device_type, timestamp_utc
     FROM status.device_shadow ds
      JOIN unnest(ARRAY[
       '000100071818', '000100072021', '0001000712E6', '000100072E46', '0001000705DB', '000100070B1E'
        ]) host
     ON host_rcpn = host
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
LEFT JOIN status.rcp_state r ON left(to_hex(d.st)::text, -1) || 0 = to_hex(r.state_code)
LEFT JOIN status.ess_device_info e ON d.host_rcpn=e.host_rcpn
AND d.device_id=e.device_id
LEFT JOIN status.nameplate n ON d.device_id = n.device_id
-- latest row from device info
WHERE (e.created_timestamp_utc=
        (SELECT max(created_timestamp_utc)
         FROM status.ess_device_info
         WHERE device_id=d.device_id
             AND host_rcpn=d.host_rcpn)
      OR e.created_timestamp_utc IS NULL)
-- latest row from nameplate
AND (n.created_timestamp_utc=
        (SELECT max(created_timestamp_utc)
         FROM status.nameplate
         WHERE device_id=d.device_id)
      OR n.created_timestamp_utc IS NULL)




