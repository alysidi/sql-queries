SELECT parent.device_id, parent.host_rcpn, parent.device_type, count(t.device_id),
       'DEVICE_IN_ERROR_STATE' as alert_category,
        (NOW() - INTERVAL '12 hours') as transition_timestamp_utc
--FROM (VALUES {} ) AS parent(device_id, host_rcpn) -- pass in tuples from chunks
FROM alert_chunk.pvlinks_test AS parent
LEFT JOIN 
         ( SELECT device_id, count(device_id)
              FROM status.legacy_status_state_change lssc
              WHERE timestamp_utc >= NOW() - INTERVAL '12 hours'
              AND st BETWEEN x'7000'::int AND x'7FFF'::int 
              GROUP BY lssc.device_id ) AS t
    ON t.device_id = parent.device_id
GROUP BY parent.device_id, parent.host_rcpn, parent.device_type
HAVING count(t.device_id) = 0;



