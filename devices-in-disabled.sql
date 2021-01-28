\timing
-- change the '10 days' in both places below for a wider range.
WITH 
disabled AS (
select device_id, count(device_id)   
FROM status.legacy_status_state_change
   WHERE timestamp_utc>now()-INTERVAL '5 days' 
   and device_type in ('PVLINK') 
   and st = 16
   GROUP BY device_id
   order by count(device_id) desc
),


lag_rows AS
  ( SELECT device_id,
           timestamp_utc,
           lag(timestamp_utc) over (PARTITION BY device_id
                                    ORDER BY timestamp_utc DESC) AS lag,
           lag(st) over (PARTITION BY device_id
                         ORDER BY timestamp_utc DESC) AS stlag,
           st
   FROM status.legacy_status_state_change
   WHERE timestamp_utc>now()-INTERVAL '7 days' 
   and device_type in ('PVLINK')
   and device_id in ( select device_id from disabled )
   and st not in (32816)
   ORDER BY device_id,
            timestamp_utc DESC ),

  
  -- this is where we go from grid --> island
   delta as (
      select *, (lag-timestamp_utc) as s from lag_rows 
      where st=16 or stlag=16 
      and (lag-timestamp_utc) > INTERVAL '12 hours'
      order by device_id, s asc
  )


select * from delta


select * from disabled

select * from status.device_shadow where device_type='PVLINK' and st=16 order by timestamp_utc desc

select 
 lag(timestamp_utc) over (PARTITION BY device_id
                                    ORDER BY timestamp_utc DESC) - timestamp_utc AS lag, * from status.legacy_status_state_change 
where device_id in ( '0001000381AA' )
and timestamp_utc > NOW() - INTERVAL '7 days'
order by timestamp_utc desc

