\timing
SELECT 
   parent.site_id as site_id, 
   -- daily site energies and performance 
   SUM(daily_today.yield_today) as yield_today, 
   SUM(daily_today.consumptionToday) as consumption_today

FROM ( 
   SELECT 
       tuples.site_id, tuples.system_id, ds.host_rcpn, tuples.installed_pv, tuples.expected_solar_generation_kWh, tuples.expected_solar_generation_last_month_kWh
   FROM ( 
       SELECT 
           (js::json->>'siteId')::text as site_id, (js::json->>'systemId')::text as system_id, 
           (js::json->>'hostRcpn')::text as host_rcpn, (js::json->>'installedPVkW')::real as installed_pv, 
           (js::json->>'expectedSolarGenerationKWh')::real as expected_solar_generation_kWh, 
           (js::json->>'expectedSolarGenerationLastMonthKWh')::real as expected_solar_generation_last_month_kWh 
       FROM  
           unnest(array[
 
  '{"siteId":"siteId1","systemId":"systemId-0001000734D3","hostRcpn":"0001000734D3", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
        '{"siteId":"siteId21","systemId":"systemId-000100073403","hostRcpn":"000100073403", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
        '{"siteId":"siteIdsdf1","systemId":"systemId-000100070C80","hostRcpn":"000100070C80", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
        '{"siteId":"siteIsdfad1","systemId":"systemId-000100073476","hostRcpn":"000100073476", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
        '{"siteId":"siteIdddd1","systemId":"systemId-000100073CC2","hostRcpn":"000100073CC2", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
        '{"siteId":"sitsfeId1","systemId":"systemId-000100070EFB","hostRcpn":"000100070EFB", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
        '{"siteId":"sitwereId1","systemId":"systemId-0001000730FD","hostRcpn":"0001000730FD", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
        '{"siteId":"sitewaId1","systemId":"systemId-00010007314C","hostRcpn":"00010007314C", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
        '{"siteId":"sitsdweeId1","systemId":"systemId-00010007181A","hostRcpn":"00010007181A", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
        '{"siteId":"sitfaseId1","systemId":"systemId-0001000710E1","hostRcpn":"0001000710E1", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}'

       ] ) as js
   ) as tuples 
   -- filtering by device shadow hostRcpn to verify js.host_rcpn 
   LEFT JOIN status.device_shadow ds on ds.device_id=tuples.host_rcpn and ds.host_rcpn=tuples.host_rcpn 
) as parent -- join on daily table for today 
LEFT JOIN LATERAL ( 
   SELECT 
       "generation_energy_exported_Wh" as yield_today, 
       "consumption_energy_imported_Wh" as consumptionToday
       FROM pwr.system_energy_daily 
       WHERE daily = date_trunc('day',NOW() at time zone 'PST') AND system_id = parent.system_id
       ORDER BY daily desc 
       LIMIT 1 
) as daily_today ON true 


-- group on site_id from tuples passed in 
GROUP BY parent.site_id 
-- sort on 1 to n columns 
ORDER BY site_id
 limit 10;

select * from pwr.system_energy where system_id='systemId-0001000734D3' order by timestamp_local desc limit 11;
select * from pwr.system_energy where system_id='systemId-0001000710E1' order by timestamp_local desc limit 11;

select * from pwr.system_energy where system_id='6dfd168f-892a-4c32-bdab-7388a9e522c4' order by timestamp_local desc limit 11;


\d+ pwr.system_energy

select * from pwr.system_energy_daily order by daily desc limit 11;


SELECT timestamp_utc,
       timestamp_utc - LAG(timestamp_utc) OVER (
                                                ORDER BY timestamp_utc) AS diff
FROM status.legacy_status
WHERE timestamp_utc > now() - INTERVAL '1 day'
  AND device_id='000100036033'
ORDER BY timestamp_utc DESC

select * from pwr.system_energy where system_id='systemId-00010007267E'
and timestamp_local > now() - interval '1 day'
order by timestamp_local desc limit 10;

SELECT timestamp_utc,
       timestamp_utc - LAG(timestamp_utc) OVER (
                                                ORDER BY timestamp_utc) AS diff



select count(device_id), time_bucket(INTERVAL '1 day', timestamp_utc) AS day
 from status.legacy_status_state_change
where device_id ='000100033A95' 
and timestamp_utc > now() - interval '2 day'
group by day
order by day desc






select count(device_id), time_bucket(INTERVAL '1 day', timestamp_utc) AS day
 from status.legacy_status_state_change
where device_id ='00010007047B' 
and timestamp_utc > now() - interval '1 day' 
and st in (2096,2080,2128)
group by day
order by day desc


select device_id, st
from status.legacy_status_state_change
where device_id = '000100074213'
and timestamp_utc > now() - interval '5 day' 
and st in (2080, 2096)
order by timestamp_utc desc







with inverters as (
  select distinct host_rcpn from status.device_shadow where device_type='INVERTER'
),
flipflop as (
select count(m.device_id) as cnt, m.device_id from inverters
LEFT JOIN LATERAL (
 select device_id, st
  from status.legacy_status_state_change
  where device_id = inverters.host_rcpn
  and timestamp_utc > now() - interval '1 day' 
  order by timestamp_utc desc
) m on TRUE
where st in (2096,2080,2128)
group by m.device_id
order by count(device_id) desc
)

select cnt, count(cnt), count(cnt)/sum(cnt), sum(cnt),
1-percent_rank() over(order by cnt) as rank,
1-cume_dist() over(order by cnt) as cume_dist
 from 
flipflop
group by cnt;



  select device_id, st
  from status.legacy_status_state_change
  where device_id = '000100071882' 
  and timestamp_utc > now() - interval '1 hour' 
  and st in (2096, 2128, 2080)



\d+ status.legacy_status_state_change

SELECT pg_cancel_backend(pid)
  FROM pg_stat_activity
  WHERE client_addr = '10.4.1.173';

with islanding as (
  select distinct host_rcpn, st
  from status.legacy_status_state_change
  where device_id in (select distinct host_rcpn from status.device_shadow where device_type='INVERTER')
  and timestamp_utc > now() - interval '5 day' 
  and st in (2096, 2128)
)


  select device_id, count(device_id) as cnt
  from status.legacy_status_state_change
  where device_id in (select host_rcpn from islanding)
  and timestamp_utc > now() - interval '5 day' 
  and st = 2096
  group by device_id
  order by cnt desc


 
 \d+ status.legacy_status_state_change

select timestamp_utc    ,  device_id   , beacon_rcpn  ,  host_rcpn   , device_type ,  st, r.state_text from status.legacy_status_state_change 
 LEFT JOIN status.rcp_state r ON left(to_hex(st)::text, -1) || 0 = to_hex(r.state_code)
where 
device_id = '000100071882'
and timestamp_utc > now() - interval '5 day' 
order by timestamp_utc desc

SELECT pg_cancel_backend(5271);

SELECT pg_terminate_backend(1167784);SELECT pg_cancel_backend(1167784)
  FROM pg_stat_activity
  WHERE client_addr = '10.0.3.52';

\d+ status.legacy_status_view

\d+ status.legacy_status_state_change

select timestamp_utc, device_id, st, r.state_text
 from status.legacy_status_state_change
 LEFT JOIN status.rcp_state r ON left(to_hex(st)::text, -1) || 0 = to_hex(r.state_code)

where device_id ='0001000337C3' 
and timestamp_utc between now() - interval '10 day' and now() - interval '8 day'
order by timestamp_utc desc




select * from 


select * from status.device_shadow where device_id='0001000706FE'

select * from status.device_shadow where device_id='00010007030C'

select * from postino.notification_status where state='SENT' order by timestamp_utc desc;

select * from status.ess_device_info where device_id='000100126445'
select * from status.ess_device_info where device_id='000100033A95'
0001000734D3

select * from status.device_shadow where host_rcpn in ('0001000734D3','0001000710E1')

select date_trunc('day',NOW());
select daily, "consumption_energy_imported_Wh", "generation_energy_exported_Wh" from pwr.system_energy_daily where system_id='systemId-0001000710E1' order by daily desc limit 11;
