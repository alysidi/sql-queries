select * from 
(
  select device_id, host_rcpn, count(device_id) as event_count
  from status.legacy_status_state_change
  where timestamp_utc>='2021-07-01' and timestamp_utc<'2021-07-02'
  and device_id in (select device_id from status.device_shadow where device_type='PVLINK' 
  and timestamp_utc>now()-interval '1 hour' order by timestamp_utc desc )
  group by device_id, host_rcpn
  having count(device_id) > 1000
) p
LEFT JOIN status.ess_device_info e ON p.device_id = e.device_id AND p.host_rcpn = e.host_rcpn
-- latest row from ess_device_info
WHERE (e.timestamp_utc=
           (SELECT max(timestamp_utc)
            FROM status.ess_device_info
            WHERE device_id=p.device_id
                AND host_rcpn=p.host_rcpn)
       OR e.timestamp_utc IS NULL)
