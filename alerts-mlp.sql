-- get all devices in error state on a specific day
select * from status.device_shadow
where st between x'7000'::int and x'7FFF'::int
and date_trunc('day',timestamp_utc) = date_trunc('day',now() - INTERVAL '0 day')
order by timestamp_utc desc;

-- group errors by state code
select device_type, st, to_hex(st), r.state_text, count(st) from status.device_shadow d
LEFT JOIN status.rcp_state r ON d.st & x'FFF0'::int = r.state_code
where st between x'7000'::int and x'7FFF'::int
group by device_type, st, r.state_text
order by device_type,count(st) desc;

-- get devices by specific state code
select * from status.device_shadow where st=29440
order by timestamp_utc desc;


-- get state change event count by time period by device
select time_bucket(INTERVAL '1 day', timestamp_utc), count(st)
 st, to_hex(st), state_text from status.legacy_status_state_change l LEFT JOIN status.rcp_state r ON l.st & x'FFF0'::int = r.state_code
where device_id='000100034FEB' 
and timestamp_utc > NOW() - INTERVAL '7 days'
--and st between x'7000'::int and x'7FFF'::int
group by time_bucket(INTERVAL '1 day', timestamp_utc), 
 st, to_hex(st), state_text
order by time_bucket(INTERVAL '1 day', timestamp_utc) desc;

-- get state change events from specific day by device
select timestamp_utc, 
st, to_hex(st), state_text from status.legacy_status_state_change l 
LEFT JOIN status.rcp_state r ON l.st & x'FFF0'::int = r.state_code
where device_id='000100035AE4' 
order by timestamp_utc desc limit 50;

-- get last 500 events from specific device 
select timestamp_utc, timestamp_utc - lag(timestamp_utc) OVER (PARTITION BY device_id ORDER BY timestamp_utc) as duration,
st, state_text from status.legacy_status l LEFT JOIN status.rcp_state r ON l.st & x'FFF0'::int = r.state_code
where device_id='000100035AE4' 
order by timestamp_utc desc limit 500;

select * from status.rcp_state


-- current state of device
select * from status.device_shadow d 
LEFT JOIN status.rcp_state r ON d.st & x'FFF0'::int = r.state_code 
where device_id='000100034FEB';

select now() - '2021-05-31 11:43:48'::timestamp
select to_hex(47183)


-- 000100035AE4 arc fault lockout PVL - persistent
-- 000100034FEB and 0001000318DF arc fault PVL - transient  
-- 0001000833BF battery generic error happened on the 28th - battery not online since then
-- 000100072C31 bad reset on inverter that is persistent every day until now()
-- 000100030B29 generic error, but histroy shows input is low on PVL - happens on a duration pattern
-- 0001000824F4 battery offline, but other devices on site are active, why?
-- 00010008189C battery error transient


-- Offline devices and their last reported state
select device_id, device_type, st, r.state_text, timestamp_utc, (timestamp_utc > now() - INTERVAL '1 day') as is_online 
from status.device_shadow l LEFT JOIN status.rcp_state r ON l.st & x'FFF0'::int = r.state_code
where (timestamp_utc > now() - INTERVAL '1 day') = false
and timestamp_utc > now() - INTERVAL '7 day'
order by timestamp_utc desc;

\d+ status.legacy_status_daily

select device_id, count(device_id) as cnt, time_bucket('1 day', timestamp_utc) as day
from status.legacy_status_state_change 
where timestamp_utc > NOW()-INTERVAL '2 day'
and device_id in (select device_id from status.device_shadow where device_type = 'PVLINK' order by timestamp_utc desc limit 100)
group by device_id, day
order by cnt desc
