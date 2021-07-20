

select COALESCE( count(*), NULL ) as num_transitions_in_window
from status.legacy_status_state_change 
where device_id='0001000823F0'
 AND  timestamp_utc >= NOW() - INTERVAL '12 hours'
 AND  st  BETWEEN x'7000'::int AND x'7FFF'::int 
