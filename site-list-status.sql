\timing
with data as (
  SELECT ds.host_rcpn, ds.device_id, st, device_type, timestamp_utc,
  -- max(timestamp_utc) as last_update_utc,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='BATTERY' limit 1), 0) as  storage,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='PVLINK' limit 1), 0) as solar,
  ((now() - timestamp_utc) > '3 hours')::int as offline,
  -- if any devices tied to an inverter have an error, then select 1
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and st between x'7000'::int and x'7FFF'::int limit 1), 0) as error
  FROM unnest(ARRAY['0001000727D6']) id
  JOIN status.device_shadow ds on ds.device_id=id and ds.host_rcpn=ds.device_id
)
select * from data



\timing
with data as (
  SELECT ds.host_rcpn, st, device_type,
  timestamp_utc as last_update_utc,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='BATTERY' limit 1), 0) as  storage,
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and device_type='PVLINK' limit 1), 0) as solar,
  ((now() - timestamp_utc) > '3 hours')::int as offline,
  -- if any devices tied to an inverter have an error, then select 1
  COALESCE((select 1 from status.device_shadow where host_rcpn=ds.host_rcpn and st between x'7000'::int and x'7FFF'::int limit 1), 0) as error
  FROM (select distinct host_rcpn from status.device_shadow where device_type='INVERTER' limit 10000000 ) as inverter
  JOIN status.device_shadow ds on ds.device_id=inverter.host_rcpn and ds.host_rcpn=ds.device_id
  --group by ds.host_rcpn, st, device_type, ds.timestamp_utc
)

select host_rcpn, count(host_rcpn) from data 
GROUP BY host_rcpn
having count(host_rcpn)>1



