
-- takes in a single host_rcpn. For multi-site systems, call the function for each system

WITH data AS
    ( 
        SELECT * FROM (
            SELECT device_id, host_rcpn, device_type, NULL as st, NULL as last_heard
            FROM status.ess_device_info
            WHERE host_rcpn = '000100073278'
            and device_type != 'UNKNOWN'
            and NOT is_soft_deleted
            UNION
            SELECT device_id, host_rcpn, device_type, st, timestamp_utc as last_heard
            FROM status.device_shadow
            WHERE host_rcpn = '000100073278'
            and device_type != 'UNKNOWN'
        ) t
        WHERE st IS NOT NULL and last_heard is NOT NULL
      )
select
r.*,
( SELECT soc from pwr.system_energy where host_rcpn = '000100073278' ORDER BY timestamp_local desc LIMIT 1) as soc,
d.device_type,
d.device_id,
ls.p as power,
ls.e as energy,
ls.t as temperature,
ls.v as voltage
FROM data d

LEFT JOIN LATERAL (
    SELECT p,e,t,v FROM status.legacy_status WHERE host_rcpn=d.host_rcpn AND device_id=d.device_id
    AND timestamp_utc = d.last_heard
    LIMIT 1
) ls ON TRUE 

LEFT JOIN status.rcp_state r ON d.st & x'FFF0'::int = r.state_code  






