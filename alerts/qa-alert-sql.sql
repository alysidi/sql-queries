-- QA on Alerts Data

-- STEP 1
-- find all the pairs of alerts that have been ACTIVE and RESOLVED
-- we will use this to investigate if they should have really been ACTIVE or RESOLVED
select alert_id, count(alert_id), alert_category, device_id
from alert.ess_alert 
group by alert_id, alert_category, device_id
having count(alert_id) > 1
order by device_id, alert_id;


-- STEP 2
-- take an alert_id from STEP 1 and run this query
select 
alert_id, 
device_id,
device_state,
status,
context->>'latestDeviceState' as latestDeviceState, 
context->>'numOfErrorsInWindow' as numOfErrorsInWindow,
context->>'alertCategory' as alertCategory_in_payload,
alert_category,
alert_type,
created_timestamp_utc,
transition_timestamp_utc
from alert.ess_alert 
where alert_id='0e947232-6810-4b96-86c0-ce9f25d52cf8' 
order by created_timestamp_utc ASC;

-- STEP 3
/* 
analyze the results above
For the above alert_id='0e947232-6810-4b96-86c0-ce9f25d52cf8' you will notice that the alert_category_in_payload does not match, however, the alert_category inserted does match.
This means we are not using the proper alert_category in the ADS to RESOLVE alerts
*/


-- Helpful Queries to See History

-- Get Shadow data for the device. If there are 2 rows, it may be a power core swap or cross talk
select to_hex(st), * from status.device_shadow where device_id='000100030882';

-- Get Last 1000 state transitions for a device_id

select timestamp_utc, to_hex(st), device_id, host_rcpn 
from status.legacy_status_state_change
where device_id='000100030882' 
order by timestamp_utc desc limit 100;

