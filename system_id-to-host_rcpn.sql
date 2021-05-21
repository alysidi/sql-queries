with parent as 
(select system_id from pwr.system_meta_data where system_id not like 'system%')

select format('''{"siteId":"site-%s","systemId":"%s","hostRcpn":"%s", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}'',', s.system_id, s.system_id, s.host_rcpn)

FROM parent
LEFT JOIN LATERAL (
 select system_id, host_rcpn from pwr.system_energy 
   WHERE system_id = parent.system_id 
and timestamp_utc > now() - interval '1 day' 
and timestamp_utc < now() + interval '1 day' 
               ORDER BY timestamp_utc desc 
               LIMIT 1 ) s ON TRUE
WHERE s.system_id is not null
