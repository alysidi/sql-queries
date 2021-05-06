\timing
-- change the '10 days' in both places below for a wider range.
WITH lag_rows AS
  ( SELECT device_id,
           timestamp_utc,
           lag(timestamp_utc) over (PARTITION BY device_id
                                    ORDER BY timestamp_utc DESC) AS lag,
           lag(st) over (PARTITION BY device_id
                         ORDER BY timestamp_utc DESC) AS stlag,
           st
   FROM status.legacy_status_state_change
   WHERE timestamp_utc>now()-INTERVAL '10 days' 
   and st in (2080,2096,2128) 
   and device_id IN (select distinct host_rcpn from status.device_shadow where device_type='INVERTER' and host_rcpn!='000100070000' limit 1000)
   ORDER BY device_id,
            timestamp_utc DESC ),

-- this is where we go from grid --> island
   grid_to_island as (
      select *, (lag-timestamp_utc) as s from lag_rows where stlag=2080 and st=2096 and device_id!='000100070000' order by s asc
  ),

  -- filter out duration for only island mode
  how_long_in_island_mode as (
      select device_id, timestamp_utc, st, 
   lag(timestamp_utc) over (PARTITION BY device_id
                                      ORDER BY timestamp_utc DESC) - timestamp_utc AS lag
                           from status.legacy_status_state_change 
  where device_id in (select device_id from grid_to_island group by device_id) and timestamp_utc > now() - INTERVAL '10 days'
  and st in(2080,2096,2128)
  order by device_id, timestamp_utc desc

    )



-- count the number of grid island events per device
select * from grid_to_island order by device_id, s;


-- get percentiles, and filter out the 5 minute noise, and only in island mode
select
  percentile_disc(0.25) within group (order by lag) as percentile_25,
  percentile_disc(0.5) within group (order by lag) as percentile_50,
  percentile_disc(0.75) within group (order by lag) as percentile_75,
  percentile_disc(0.95) within group (order by lag) as percentile_95
from how_long_in_island_mode
where st=2096 and lag > INTERVAL '5 minute';



-- count the number of grid island events per device
select * from grid_to_island order by device_id, s;


-- count the number of grid island events per device
select count(device_id), device_id from grid_to_island group by device_id order by count(device_id) desc;

