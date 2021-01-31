
-- site list bulk call
\timing
with data as (
  SELECT ds.host_rcpn, ds.device_id, st, device_type, timestamp_utc,
  timestamp_utc as last_update_utc,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='BATTERY' limit 1), 0) as  storage,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='PVLINK' limit 1), 0) as solar,
  ((now() - timestamp_utc) > '3 hours')::int as offline,
  -- if any devices tied to an inverter have an error, then select 1
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and st between x'7000'::int and x'7FFF'::int limit 1), 0) as error
  FROM unnest(ARRAY['0001000727D6','000100071F39','000100070AD2']) id
  JOIN status.device_shadow ds on ds.device_id=id and ds.host_rcpn=ds.device_id
)

select * from data where solar in (1,0) and storage  in (1,0) and error in (1,0) and offline in (1,0)

-- multi-site-inverter bulk call

\timing
with data as (
  SELECT t.site,t.host_rcpn,
 COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='BATTERY' limit 1), 0) as  storage,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='PVLINK' limit 1), 0) as solar,
  ((now() - timestamp_utc) > '3 hours')::int as offline,
  -- if any devices tied to an inverter have an error, then select 1
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and st between x'7000'::int and x'7FFF'::int limit 1), 0) as error
  FROM status.device_shadow ds
  JOIN (VALUES  
 ('000100070FB4','site1'),
 ('0001000730AC','site2'),
 ('000100070865','site2'),
 ('000100070AD2','site3'),
 ('000100071F39','site1'),
 ('000100070B2D','site3')) as t(host_rcpn,site)
  on ds.device_id=t.host_rcpn and ds.host_rcpn=ds.device_id
)

select * from data where solar in (1,0) and storage  in (1,0) and error in (1,0) and offline in (1,0)


select site, sum(storage) as storage, sum(solar) as solar, sum(offline) as offline, sum(error) as error 
from data 
group by site




