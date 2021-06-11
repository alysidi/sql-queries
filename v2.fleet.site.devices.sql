
-- takes in a single host_rcpn. For multi-site systems, call the function for each system

SELECT r.*,
b.soc,
d.device_type,
d.device_id,
d.host_rcpn,
ls.p as power,
ls.e as energy,
ls.t as temperature,
ls.v as voltage,
d.timestamp_utc as last_heard_utc
FROM status.device_shadow d        
LEFT JOIN LATERAL (
    SELECT p,e,t,v FROM status.legacy_status WHERE host_rcpn=d.host_rcpn AND device_id=d.device_id
    AND timestamp_utc=d.timestamp_utc
    LIMIT 1
) ls ON TRUE 
LEFT JOIN LATERAL (
    SELECT avg(soc) as soc from status.battery_module 
    WHERE device_id=d.device_id and d.device_type='BATTERY'
    and timestamp_utc > NOW()-INTERVAL '3 hours' 
    GROUP BY timestamp_utc
    ORDER BY timestamp_utc DESC
    LIMIT 1
) b ON TRUE

LEFT JOIN status.rcp_state r ON d.st & x'FFF0'::int = r.state_code  
WHERE d.host_rcpn IN ('0001000734F3','00010007272B') and d.device_type != 'UNKNOWN';




