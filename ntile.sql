with devices as (
  select device_id, avg(avg_soc) as soc, max(total_e) as e,avg(avg_t) as t
  from status.legacy_status_daily
  where day > NOW()-INTERVAL '6 day'
  and device_type='BATTERY'
  group by device_id
)
  select device_id, soc, e, t,
  ntile(5) OVER ( ORDER BY soc ) as p_soc,
  ntile(5) OVER ( ORDER BY e ) as p_e,
  ntile(5) OVER ( ORDER BY t ) as p_t
from devices
order by p_soc, p_e

\d+ status.device_shadow


-- site list + energy

\timing
with data as (
  SELECT ds.host_rcpn, st, device_type,
  timestamp_utc as last_update_utc,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='BATTERY' limit 1), 0) as  storage,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='PVLINK' limit 1), 0) as solar,
  ((now() - timestamp_utc) > '3 hours')::int as offline,
  -- if any devices tied to an inverter have an error, then select 1
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and st between x'7000'::int and x'7FFF'::int limit 1), 0) as error
  FROM (select distinct device_id from status.device_shadow where device_type='INVERTER' ) as inverter
  JOIN status.device_shadow ds on ( ds.device_id=inverter.device_id  )

),
energy as (
  select l.device_id, avg(avg_soc) as soc, max(total_e) as e,avg(avg_t) as t, error, offline, storage, st, d.device_type
  from status.legacy_status_daily l join data d on l.device_id = d.host_rcpn
  where day > NOW()-INTERVAL '1 day'
  group by device_id,error, offline, storage, st, d.device_type
), 
tile as (
    select device_id, soc, e, t, error, offline, device_type,
  ntile(5) OVER ( PARTITION BY device_type ORDER BY soc ) as p_soc,
  ntile(5) OVER ( PARTITION BY device_type ORDER BY e ) as p_e,
  ntile(5) OVER ( PARTITION BY device_type ORDER BY t ) as p_t
from energy
)


select * from data d
  CROSS JOIN LATERAL (
  select device_id, min_soc, avg_soc, max_soc, day
  from status.legacy_status_daily    
  where day > NOW()-INTERVAL '2 day'
  and device_id = d.host_rcpn
  order by day desc
  limit 1
) t1

--where error = 1
limit 10 offset 11;


select * from energy
where error = 0 
limit 50 offset 400;







select min(e), max(e), avg(e), device_type, p_e, count(p_e),
width_bucket(e, 1000, 100000, 5) as bucket
from tile 
where e>0 and e <214248652
group by device_type, p_e, e


select * from energy

select device_id, count(device_id) from energy group by device_id


\d+ status.legacy_status_daily

-- cross lateral join

\timing
  select distinct ds.host_rcpn, t1.device_id, t1.min_soc, t1.avg_soc, t1.max_soc, t1.day,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='BATTERY' limit 1), 0) as  storage

  from status.device_shadow ds 
  JOIN (VALUES  ('000100072A07'),('0001000730B5'),('0001000731DF') )
  as t(p)
ON t.p = host_rcpn 

  CROSS JOIN LATERAL (
  select device_id, min_soc, avg_soc, max_soc, day
  from status.legacy_status_daily    
  where day > NOW()-INTERVAL '1 day'
  and device_id = ds.host_rcpn
  order by day desc
  limit 1
) t1


\timing
  select distinct ds.host_rcpn, l.device_id, l.min_soc, l.avg_soc, l.max_soc

  from status.device_shadow ds 
  join status.legacy_status_daily l
  on l.device_id = ds.host_rcpn
  where day > NOW()-INTERVAL '2 day'
  and  ds.device_type='INVERTER'


\timing
with boo as (

  select device_id, min_soc, avg_soc, max_soc, day
  from status.legacy_status_daily    
  where day > NOW()-INTERVAL '1 day'
  order by day desc
), 
boo2 as (

  select device_id, min_soc, avg_soc, max_soc, month
  from status.legacy_status_monthly    
  where month > NOW()-INTERVAL '1 month'
  order by month desc
)











select * from status.legacy_status_monthly where device_id in (select DISTINCT host_rcpn from status.device_shadow limit 1) order by month desc limit 10

