
\timing
with data as (
 SELECT DISTINCT ON (device_id) device_id, day, last_st,
    lag(day) over (order by day) yesterday, 
    lag(last_st) over (order by day) lag_last_st,
    --PERCENT_RANK() OVER (ORDER BY avg_I ), 
    ntile(5) OVER (ORDER BY avg_I ) 
    FROM status.legacy_status_daily
    WHERE day > NOW() - INTERVAL '2 day'
    ORDER BY device_id, day desc
)
select device_id from data where ntile = 5
limit 50 offset 3012


select count(device_id), ntile from data group by ntile

select count(device_id), ntile from data group by ntile

with tile as (
  select distinct site_id, ntile(5) OVER (ORDER BY raw_battery_lifetime_exported_Ws )
  from pwr.site_energy
  where timestamp_local > NOW()-INTERVAL '44 day'

)

select t.site_id, e.raw_battery_lifetime_exported_Ws, t.ntile
from pwr.site_energy e join tile t
on e.site_id = t.site_id
where timestamp_local > NOW()-INTERVAL '44 day'
and t.ntile=1
limit 10;

\d+ pwr.system_energy