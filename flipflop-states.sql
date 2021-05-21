\timing
with inverters as (
  select distinct host_rcpn from status.device_shadow where device_type='INVERTER'
),
flipflop as (
select count(m.device_id) as cnt, m.device_id from inverters
LEFT JOIN LATERAL (
 select device_id, st
  from status.legacy_status_state_change
  where device_id = inverters.host_rcpn
  and timestamp_utc > now() - interval '5 day' 
  order by timestamp_utc desc
) m on TRUE
where st in (2096,2080,2128) 
group by m.device_id

order by count(device_id) desc
)


select device_id, cnt as total from flipflop 
where cnt > 10
group by device_id,cnt
order by cnt desc

SELECT coalesce(role.rolname, 'database wide') as role, 
       coalesce(db.datname, 'cluster wide') as database, 
       setconfig as what_changed
FROM pg_db_role_setting role_setting
LEFT JOIN pg_roles role ON role.oid = role_setting.setrole
LEFT JOIN pg_database db ON db.oid = role_setting.setdatabase;

\du+
