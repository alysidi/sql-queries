-- get all devices in error state on a specific day
select * from status.device_shadow
where st between x'7000'::int and x'7FFF'::int
and date_trunc('day',timestamp_utc) = date_trunc('day',now() - INTERVAL '0 day')
order by timestamp_utc desc;

-- group errors by state code
select device_type, st, r.state_text, count(st) from status.device_shadow d
LEFT JOIN status.rcp_state r ON d.st & x'FFF0'::int = r.state_code
where st between x'7000'::int and x'7FFF'::int
group by device_type, st, r.state_text
order by count(st) desc;

-- get devices by specific state code
select * from status.device_shadow where st=29440
order by timestamp_utc desc;


-- get state change events by time period by device
select timestamp_utc, timestamp_utc - lag(timestamp_utc) OVER (PARTITION BY device_id ORDER BY timestamp_utc) as duration,
 st, to_hex(st), state_text from status.legacy_status_state_change l LEFT JOIN status.rcp_state r ON l.st & x'FFF0'::int = r.state_code
where device_id='000100034E29' 
and timestamp_utc > NOW() - INTERVAL '7 days'
and st between x'7000'::int and x'7FFF'::int
order by timestamp_utc desc;

-- get state change events from specific day by device
select timestamp_utc, timestamp_utc - lag(timestamp_utc) OVER (PARTITION BY device_id ORDER BY timestamp_utc) as duration,
st, state_text from status.legacy_status_state_change l LEFT JOIN status.rcp_state r ON l.st & x'FFF0'::int = r.state_code
where device_id='000100034E29' 
and date_trunc('day',timestamp_utc) = '2021-05-29'
order by timestamp_utc desc limit 500;

-- get last 500 events from specific device 
select now();

select timestamp_utc, timestamp_utc - lag(timestamp_utc) OVER (PARTITION BY device_id ORDER BY timestamp_utc) as duration,
st, state_text from status.legacy_status l LEFT JOIN status.rcp_state r ON l.st & x'FFF0'::int = r.state_code
where device_id='000100082F33' 
order by timestamp_utc desc limit 500;

-- current state of device
select * from status.device_shadow d 
LEFT JOIN status.rcp_state r ON d.st & x'FFF0'::int = r.state_code 
where device_id='000100082F33';


-- 000100035AE4 arc fault lockout PVL - persistent
-- 000100034FEB arc fault PVL - transient
-- 0001000833BF battery generic error happened on the 28th - battery not online since then
-- 000100072C31 bad reset on inverter that is persistent every day until now()
-- 000100030B29 generic error, but histroy shows input is low on PVL - happens on a duration pattern



-- Offline devices and their last reported state
select device_id, device_type, st, r.state_text, timestamp_utc, (timestamp_utc > now() - INTERVAL '1 day') as is_online 
from status.device_shadow l LEFT JOIN status.rcp_state r ON l.st & x'FFF0'::int = r.state_code
where (timestamp_utc > now() - INTERVAL '1 day') = false
and timestamp_utc > now() - INTERVAL '7 day'
order by timestamp_utc desc;
