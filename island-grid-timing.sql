\timing
WITH lag_rows AS
  ( SELECT device_id,
           timestamp_utc,
           lag(timestamp_utc) over (PARTITION BY device_id
                                    ORDER BY timestamp_utc DESC) AS lag,
           lag(st) over (PARTITION BY device_id
                         ORDER BY timestamp_utc DESC) AS stlag,
           st
   FROM status.legacy_status_state_change
   WHERE timestamp_utc>now()-INTERVAL '5 days' 
   and st in (2080,2096) 
   and device_id like '00010007%'
   ORDER BY device_id,
            timestamp_utc DESC ),

   grid_to_island as (
      select *, (lag-timestamp_utc) as s from lag_rows where stlag=2080 and st=2096 and device_id!='000100070000' order by s asc
  ),

  how_long_in_island_mode as (
      select device_id, timestamp_utc, st, 
   lag(timestamp_utc) over (PARTITION BY device_id
                                      ORDER BY timestamp_utc DESC) - timestamp_utc AS lag
                           from status.legacy_status_state_change 
  where device_id in (select device_id from grid_to_island group by device_id) and timestamp_utc > now() - INTERVAL '5 days'
  and st in(2080,2096)
  order by device_id, timestamp_utc desc

)
  
select
  percentile_disc(0.25) within group (order by lag) as percentile_25,
  percentile_disc(0.5) within group (order by lag) as percentile_50,
  percentile_disc(0.75) within group (order by lag) as percentile_75,
  percentile_disc(0.95) within group (order by lag) as percentile_95
from how_long_in_island_mode
where st=2096 and lag > INTERVAL '5 minute';



--select avg(lag) from how_long_in_island_mode where st = 2096 and lag > interval '2 minutes';



  --select device_id, count(device_id) from grid_to_island group by device_id order by count(device_id) desc;

-- check ping ping




-- check states

select * from status.legacy_status_state_change where 
st = 2096 and
device_id='000100070763' and timestamp_utc between '2021-01-01' and '2021-01-07' order by timestamp_utc desc;

select * from status.legacy_status_state_change where device_id='00010007047B' and timestamp_utc between '2021-01-01' and '2021-01-07' order by timestamp_utc desc;
select * from status.legacy_status_state_change where device_id='0001000707DC' and timestamp_utc between '2021-01-01' and '2021-01-07' order by timestamp_utc desc;

select * from status.legacy_status_state_change 
where timestamp_utc between '2021-01-01' and '2021-01-06'
and device_type='INVERTER'
and st=2128 
order by timestamp_utc desc

select * from status.rcp_state where state_code in (2080,2096,2128)

select * from status.legacy_status 
where device_id='000100071C59'
and timestamp_utc>'2020-11-06'
order by timestamp_utc desc

https://tools.pika-energy.com/systems/0d9d8ad2-f7b4-11ea-bebe-42010a8000df/devices/000100072483/updates



