

-- offline alerts
select device_id,  
host_rcpn,      
'DEVICE_OFFLINE' as alert_type, 
timestamp_utc as last_heard_timestamp_utc, 
to_hex(st) as latest_state, 
to_hex(st) as state, 
timestamp_utc as transition_timestamp_utc
from status.device_shadow ds
left join status.rcp_state r ON r.state_code = (ds.st & x'FFF0'::int)
where timestamp_utc <= now() - INTERVAL '2 days';


-- online alerts
select device_id,  
host_rcpn,      
'DEVICE_ONLINE' as alert_type, 
timestamp_utc as last_heard_timestamp_utc, 
to_hex(st) as latest_state, 
to_hex(st) as state, 
timestamp_utc as transition_timestamp_utc
from status.device_shadow ds
left join status.rcp_state r ON r.state_code = (ds.st & x'FFF0'::int)
where timestamp_utc >= now() - INTERVAL '10 minutes'
and device_id = '000100000694';