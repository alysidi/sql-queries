
-- when was the last time we saw this error
select * from pwr.alerts order by updated_timestamp_utc desc;

-- when was the last time the device reported
select * from pwr.alerts order by event_timestamp_utc desc;

-- swapped?
select * from status.device_shadow where device_id='00010003674F'

-- double trouble
select * from pwr.alerts where device_id='00010003674F'
