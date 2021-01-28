-- Sunova

SELECT *
FROM address
WHERE address_id IN
    (SELECT homeowner_address_id
     FROM site
     WHERE fleet_id='547b5ad5-8788-4157-b4a0-ce539802cbe2');

SELECT homeowner_address_id
FROM site
WHERE fleet_id='547b5ad5-8788-4157-b4a0-ce539802cbe2';



-- PowerHome and Sunnova

SELECT from_unixtime(installation_date/1000) AS installation_date,
       CASE
           WHEN fleet_id = '547b5ad5-8788-4157-b4a0-ce539802cbe2' THEN 'Sunnova'
           WHEN fleet_id = 'ffb337d2-4679-4618-84e0-2cab37b83823' THEN 'PowerHome'
       END who
FROM site
WHERE fleet_id IN ('547b5ad5-8788-4157-b4a0-ce539802cbe2',
                   'ffb337d2-4679-4618-84e0-2cab37b83823')
AND installation_date > 1601510400000;



create table status.new like status.legacy_status
-- W2s as of Aug 15th

select count(*) from M_SENSORLOCATION
where SENSOR_ID LIKE "0x000004714B%"
and ENDTIME IS NULL
and LOCATION_ID IS NOT NULL
and STARTTIME >= 1597449600000
