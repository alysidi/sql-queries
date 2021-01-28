
-------------------------------------------------------------
 
-- Failed Sites

-- temp tables will disappear once you close your session
CREATE TEMPORARY TABLE failed_sites select * from audit where event_name like 'reg_site%' and event_log like '%failed site%';



-- count the types of errors
SElECT 
 CASE
    WHEN event_log like '%SAP responded with a failed registration this serial number%' THEN "SAP Error"
    WHEN event_log like '%The serial number you entered is already in use%' THEN "Serial In Use"
    WHEN event_log like '%Inverter RCP not found in equipment records%' THEN "RCP Not Found"
    WHEN event_log like '%Pika system registration failed for this serial number%' THEN "Pika Registration Failed"
    ELSE "Some Other Error"
END as error_type,
count(event_name)
from failed_sites
group by error_type


-- use this query below to find new types of errors
select * from failed_sites where event_log not like '%SAP responded with a failed registration this serial number%'
and event_log not like  '%The serial number you entered is already in use%' 
and event_log not like '%Inverter RCP not found in equipment records%'
and event_log not like '%Pika system registration failed for this serial number%'

-------------------------------------------------------------
-- EXCEPTIONS 

CREATE TEMPORARY TABLE site_exceptions select * from audit where event_name like 'reg_site%' and event_log like 'Failed to save site entities for a site%';

-- exceptions when 
select * from audit where trace_id in (select trace_id from site_exceptions)
order by event_name, created_at;

 