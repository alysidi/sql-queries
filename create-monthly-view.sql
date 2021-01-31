CREATE MATERIALIZED VIEW status.legacy_status_monthly 
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
      ORDER BY month desc
      WITH NO DATA;

 CREATE UNIQUE INDEX on status.legacy_status_monthly (host_rcpn, device_id, month DESC, device_type);

 CREATE INDEX on status.legacy_status_monthly (device_id, month DESC);

\d+ status.battery_status_daily

\d+ _timescaledb_internal._materialized_hypertable_184

select * from status.legacy_status_monthly order by month desc limit 1

REFRESH MATERIALIZED VIEW CONCURRENTLY status.legacy_status_monthly;

