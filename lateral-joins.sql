\timing 
select * from (
  SELECT ds.device_id, ds.host_rcpn, ds.st, ds.device_type,
    ds.timestamp_utc as last_update_utc,
    COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='BATTERY' limit 1), 0) as  storage,
    COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='PVLINK' limit 1), 0) as solar,
    ((now() - ds.timestamp_utc) > '3 hours')::int as offline,
    -- if any devices tied to an inverter have an error, then select 1
    COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and st between x'7000'::int and x'7FFF'::int limit 1), 0) as error
    FROM (select distinct device_id from status.device_shadow where device_type='BATTERY' ) as inverter
    JOIN status.device_shadow ds on ( ds.device_id=inverter.device_id )
    ) p

CROSS JOIN LATERAL (
 select device_id, min_soc, avg_soc, max_soc, day,
  lag(day) over (order by day) yesterday 
  from status.legacy_status_daily    
  where day > NOW()-INTERVAL '2 day'
  and device_id = p.device_id
  order by day desc
  limit 1
) t1


CROSS JOIN LATERAL (
 select device_id, min_soc, avg_soc, max_soc, month
  from status.legacy_status_monthly    
  where month > NOW()-INTERVAL '1 month'
  and device_id = p.host_rcpn
  order by month desc
  limit 1
) t2


CROSS JOIN LATERAL (
  select device_id, total_whin, total_whout, avg_w, day 
  from status.battery_status_daily
  where day > NOW()-INTERVAL '1 day'
  and device_id = p.device_id
  order by day desc
  limit 1
) t3 -- on TRUE

--where p.error=1 
--order by t3.total_whin DESC
limit 50 offset 0;


-- check order by

\timing
select t3.*, ds.* from status.device_shadow ds
CROSS JOIN LATERAL (
  select device_id, total_whin, total_whout, avg_w, day 
  from status.battery_status_daily
  where day > NOW()-INTERVAL '1 day'
  and device_id = ds.device_id
  order by day desc
  limit 1
) t3 -- on TRUE
where ds.device_type='BATTERY'
order by t3.total_whin desc
limit 10;



\timing
select distinct on(ds.device_id) ds.device_id, t3.*, ds.* 
from status.device_shadow ds
JOIN 
 status.battery_status_daily t3
  on t3.device_id = ds.device_id
  where day > NOW()-INTERVAL '1 day'
and ds.device_type='BATTERY'
order by ds.device_id, t3.total_whin desc
limit 10;


