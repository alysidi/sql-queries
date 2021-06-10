
-- takes in a single host_rcpn. For multi-site systems, call the function for each system

SELECT r.*,
( SELECT soc from pwr.system_energy where host_rcpn = '000100073278' and timestamp_local > NOW() - INTERVAL '3 hours' ORDER BY timestamp_local desc LIMIT 1) as soc,
d.device_type,
d.device_id,
ls.p as power,
ls.e as energy,
ls.t as temperature,
ls.v as voltage,
d.timestamp_utc as last_heard_utc
FROM status.device_shadow d        
LEFT JOIN LATERAL (
    SELECT p,e,t,v FROM status.legacy_status WHERE host_rcpn=d.host_rcpn AND device_id=d.device_id
    AND timestamp_utc = d.timestamp_utc
    LIMIT 1
) ls ON TRUE 

LEFT JOIN status.rcp_state r ON d.st & x'FFF0'::int = r.state_code  
WHERE d.host_rcpn = '000100073278'and d.device_type != 'UNKNOWN'






