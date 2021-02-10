\timing 

SELECT 
parent.site_id, SUM(parent.solar) as solar, SUM(parent.storage) as error, SUM(parent.error) as error, SUM(parent.offline) as offline,
MIN(daily.min_soc) as daily_min_soc, AVG(daily.avg_soc) as daily_avg_soc, MAX(daily.max_soc) as daily_max_soc,
MAX(yesterday_max_soc) as yesterday_max_soc,
MIN(monthly.min_soc) as monthly_min_soc, AVG(monthly.avg_soc) as monthly_avg_soc, MAX(monthly.max_soc) as monthly_max_soc

FROM (
 SELECT tuples.site_id, tuples.system_id, ds.host_rcpn, ds.device_id, ds.device_type, ds.timestamp_utc as last_update_utc,
    COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.device_type='BATTERY' limit 1), 0) as  storage,
    COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.device_type='PVLINK' limit 1), 0) as solar,
    ((now() - ds.timestamp_utc) > '3 hours')::int as offline,
    -- if any devices tied to an inverter have an error, then select 1
    COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.st between x'7000'::int and x'7FFF'::int limit 1), 0) as error
    FROM status.device_shadow ds
    JOIN (VALUES 
 ('site1','system1','0001000719AD'),
 ('site1','system2','000100070827'),
 ('site2','system3','0001000711DF'),
 ('site2','system4','00010007280A'),
 ('site3','system5','00010007046F')
    ) as tuples(site_id, system_id, host_rcpn) on ds.device_id=tuples.host_rcpn and ds.host_rcpn=tuples.host_rcpn
) as parent

-- join to daily table
CROSS JOIN LATERAL (
 SELECT device_id, min_soc, avg_soc, max_soc, day,
  lag(day) over (order by day) yesterday,
  lag(max_soc) over (order by day) yesterday_max_soc
  FROM status.legacy_status_daily    
  WHERE day >= date_trunc('day',NOW())-INTERVAL '1 day'
  AND device_id = parent.device_id
  ORDER BY day desc
  LIMIT 1
) as daily


-- repeat lateral join for monthly, yearly
CROSS JOIN LATERAL (
 select device_id, min_soc, avg_soc, max_soc, month,
 lag(month) over(order by month) as last_month
  from status.legacy_status_monthly_materialized 
  where month >= date_trunc('month',NOW())-INTERVAL '1 month'
  and device_id = parent.device_id
  order by month desc
  limit 1
) monthly

-- filter by solar / storage
WHERE parent.solar > 0 
-- group on site_id from tuples passed in
GROUP BY parent.site_id
-- sort on 1 to n columns
ORDER BY daily_avg_soc DESC, parent.site_id
-- paging
LIMIT 50 OFFSET 0


