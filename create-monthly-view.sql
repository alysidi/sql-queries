

CREATE OR REPLACE VIEW status.legacy_status_monthly 
   AS
      SELECT  
         date_trunc('month', day) AS month,
         --time_bucket(INTERVAL '1 month', timestamp_utc) AS month,
         device_id, 
         device_type, 
         host_rcpn,
         avg(avg_I) as avg_I,
         avg(avg_V) as avg_V,
         avg(avg_P) as avg_P, 
         avg(avg_T) as avg_T, 
         min(min_I) as min_I,
         min(min_V) as min_V,
         min(min_P) as min_P, 
         min(min_T) as min_T, 
         max(max_I) as max_I,
         max(max_V) as max_V,
         max(max_P) as max_P, 
         max(max_T) as max_T, 
         last(total_E, day) as total_E,
         last(last_St, day) as last_St,
         last(last_Rb, day) as last_Rb,
         avg(avg_SoC) as avg_SoC,
         min(min_SoC) as min_SoC,
         max(max_SoC) as max_SoC
         FROM status.legacy_status_daily
         WHERE host_rcpn <> ''
      GROUP BY month, device_id, device_type, host_rcpn
      ORDER BY month desc;
 



-- DROP MATERIALIZED VIEW status.legacy_status_monthly_materialized

CREATE MATERIALIZED VIEW status.legacy_status_monthly_materialized 
   AS
   SELECT *
   FROM status.legacy_status_monthly
   WHERE MONTH >= date_trunc('month', now()) - INTERVAL '1 months' 
   WITH NO DATA;

CREATE UNIQUE INDEX on status.legacy_status_monthly_materialized (month DESC, device_id, device_type, host_rcpn);

CREATE INDEX on status.legacy_status_monthly_materialized (device_id, month DESC);

REFRESH MATERIALIZED VIEW status.legacy_status_monthly_materialized;

REFRESH MATERIALIZED VIEW CONCURRENTLY status.legacy_status_monthly_materialized;

SELECT cron.schedule('nightly-legacy-status-monthly-materialized', '0 11 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY status.legacy_status_monthly_materialized');


-- Queries Materialized View
select min(month), max(month), count(*) from status.legacy_status_monthly_materialized;

select device_id, month, lag(month) over (order by month) as last_month
 from status.legacy_status_monthly_materialized
where device_type='BATTERY' and month >= date_trunc('month',now())-INTERVAL '1 months'
and device_id in (select distinct device_id from status.device_shadow where device_type in ('BATTERY'))
order by month desc

select device_id, month, lag(month) over (order by month) as last_month
from status.legacy_status_monthly_materialized 
where device_id='000100037004'
and month >= date_trunc('month',NOW())-INTERVAL '2 months'
order by month desc

select date_trunc('month',now())-INTERVAL '1 months'

select device_id, count(device_id) 
 from status.legacy_status_monthly_materialized 
group by device_id 


