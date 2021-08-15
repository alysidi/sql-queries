
with data as (
select * from status.device_shadow order by timestamp_utc desc limit 1000
)
SELECT parent.device_id, parent.host_rcpn, parent.device_type, json_agg(json_build_object('count', COALESCE(t.count,0))) as num_of_errors_in_window,
       'DEVICE_IN_ERROR_STATE' as alert_category,
        (NOW() - INTERVAL '12 hours') as transition_timestamp_utc,
        extract(epoch from ds.timestamp_utc) as latest_timestamp_utc,
        to_hex(ds.st) as latest_device_state
FROM data AS parent
LEFT JOIN 
         ( SELECT device_id, count(device_id) as count
              FROM status.legacy_status_state_change 
              WHERE st BETWEEN x'7000'::int AND x'7FFF'::int
              AND timestamp_utc >= NOW() - INTERVAL '12 hours'
              GROUP BY device_id
              HAVING count(device_id) = 0 ) AS t
    ON t.device_id = parent.device_id
LEFT JOIN status.device_shadow ds ON ( ds.device_id = parent.device_id AND ds.host_rcpn = parent.host_rcpn )
GROUP BY parent.device_id, parent.host_rcpn, parent.device_type, ds.st, ds.timestamp_utc;


