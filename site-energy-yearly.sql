


CREATE MATERIALIZED VIEW pwr.site_energy_yearly
   AS
      SELECT
         date_trunc('year', day) as year,
         site_id,
         round(avg(average_soc),4) as average_soc, -- how does battery capacity get taken into account?
         -- show last lifetime value?
         last(raw_battery_lifetime_imported_Ws, day) as raw_battery_lifetime_imported_Ws,
         
         sum(time_diff_in_seconds) as time_diff_in_seconds,
         -- 
         avg(average_efficiency) as average_efficiency,
         -- battery energies
         sum(battery_energy_Wh) as battery_energy_Wh,
         round((sum(battery_energy_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_battery_power_W,
         sum(battery_energy_critical_peak_Wh) as battery_energy_critical_peak_Wh,
         sum(battery_energy_not_tou_Wh) as battery_energy_not_tou_Wh,
         sum(battery_energy_off_peak_Wh) as battery_energy_off_peak_Wh,
         sum(battery_energy_on_peak_Wh) as battery_energy_on_peak_Wh,
   
         
         -- consumption energies
         sum(consumption_energy_Wh) as consumption_energy_Wh,
         round((sum(consumption_energy_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_consumption_power_W,
         sum(consumption_energy_critical_peak_Wh) as consumption_energy_critical_peak_Wh,
         sum(consumption_energy_not_tou_Wh) as consumption_energy_not_tou_Wh,
         sum(consumption_energy_off_peak_Wh) as consumption_energy_off_peak_Wh,
         sum(consumption_energy_on_peak_Wh) as consumption_energy_on_peak_Wh,
         sum(consumption_energy_partial_peak_Wh) as consumption_energy_partial_peak_Wh,
         

         -- attribution energies --
         sum(generation_energy_exported_to_battery_Wh) as generation_energy_exported_to_battery_Wh,
         sum(generation_energy_exported_to_grid_Wh) as generation_energy_exported_to_grid_Wh,
         sum(generation_energy_exported_to_home_Wh) as generation_energy_exported_to_home_Wh,
         sum(consumption_energy_from_battery_Wh) as consumption_energy_from_battery_Wh, 
         sum(consumption_energy_from_grid_Wh) as consumption_energy_from_grid_Wh, 
         sum(consumption_energy_from_solar_Wh) as consumption_energy_from_solar_Wh, 

         -- attribution power --
         round((sum(generation_energy_exported_to_battery_Wh) / sum(time_diff_in_seconds))::numeric,4) as solar_power_exportedToBattery_W,
         round((sum(generation_energy_exported_to_grid_Wh) / sum(time_diff_in_seconds))::numeric,4) as solar_power_exportedToGrid_W,
         round((sum(generation_energy_exported_to_home_Wh) / sum(time_diff_in_seconds))::numeric,4) as solar_power_exportedToHome_W,
         round((sum(consumption_energy_from_battery_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_consumption_power_from_battery_W,
         round((sum(consumption_energy_from_grid_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_consumption_power_from_grid_W,
         round((sum(consumption_energy_from_solar_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_consumption_power_from_solar_W

         FROM pwr.site_energy_daily
        -- WHERE site_id='2937580f-0b92-424e-a4a3-339f875aabfa'
      GROUP BY year, site_id
      WITH NO DATA;





-- daily

CREATE VIEW pwr.site_energy_daily
   WITH (timescaledb.continuous)
   AS
      SELECT
         time_bucket(INTERVAL '1 day', timestamp_local) as day,
         site_id,
         round(avg(soc),4) as average_soc, -- how does battery capacity get taken into account?
         -- show last lifetime value?
         last(raw_battery_lifetime_imported_Ws, timestamp_local) as raw_battery_lifetime_imported_Ws,
         
         sum(time_diff_in_seconds) as time_diff_in_seconds,
         -- 
         avg(efficiency) as average_efficiency,
         -- battery energies
         sum(battery_energy_Wh) as battery_energy_Wh,
         round((sum(battery_energy_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_battery_power_W,
         sum(CASE WHEN tou = 'CRITICAL_PEAK' THEN battery_energy_Wh ELSE 0 END) as battery_energy_critical_peak_Wh,
         sum(CASE WHEN tou = 'NOT_TOU' THEN battery_energy_Wh ELSE 0 END) as battery_energy_not_tou_Wh,
         sum(CASE WHEN tou = 'OFF_PEAK' THEN battery_energy_Wh ELSE 0 END) as battery_energy_off_peak_Wh,
         sum(CASE WHEN tou = 'ON_PEAK' THEN battery_energy_Wh ELSE 0 END) as battery_energy_on_peak_Wh,
         sum(CASE WHEN tou = 'PARTIAL_PEAK' THEN battery_energy_Wh ELSE 0 END) as battery_energy_partial_peak_Wh,
         
         -- consumption energies
         sum(consumption_energy_Wh) as consumption_energy_Wh,
         round((sum(consumption_energy_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_consumption_power_W,
         sum(CASE WHEN tou = 'CRITICAL_PEAK' THEN consumption_energy_Wh ELSE 0 END) as consumption_energy_critical_peak_Wh,
         sum(CASE WHEN tou = 'NOT_TOU' THEN consumption_energy_Wh ELSE 0 END) as consumption_energy_not_tou_Wh,
         sum(CASE WHEN tou = 'OFF_PEAK' THEN consumption_energy_Wh ELSE 0 END) as consumption_energy_off_peak_Wh,
         sum(CASE WHEN tou = 'ON_PEAK' THEN consumption_energy_Wh ELSE 0 END) as consumption_energy_on_peak_Wh,
         sum(CASE WHEN tou = 'PARTIAL_PEAK' THEN consumption_energy_Wh ELSE 0 END) as consumption_energy_partial_peak_Wh,
         
         -- generation energies
         sum(generation_energy_Wh) as generation_energy_Wh, 
         round((sum(generation_energy_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_generation_power_W,
         sum(CASE WHEN tou = 'CRITICAL_PEAK' THEN generation_energy_Wh ELSE 0 END) as generation_energy_critical_peak_Wh,
         sum(CASE WHEN tou = 'NOT_TOU' THEN generation_energy_Wh ELSE 0 END) as generation_energy_not_tou_Wh,
         sum(CASE WHEN tou = 'OFF_PEAK' THEN generation_energy_Wh ELSE 0 END) as generation_energy_off_peak_Wh,
         sum(CASE WHEN tou = 'ON_PEAK' THEN generation_energy_Wh ELSE 0 END) as generation_energy_on_peak_Wh,
         sum(CASE WHEN tou = 'PARTIAL_PEAK' THEN generation_energy_Wh ELSE 0 END) as generation_energy_partial_peak_Wh,
         -- net energies
         sum(net_energy_Wh) as net_energy_Wh,
         round((sum(net_energy_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_net_power_W,
         sum(CASE WHEN tou = 'CRITICAL_PEAK' THEN net_energy_Wh ELSE 0 END) as net_energy_critical_peak_Wh,
         sum(CASE WHEN tou = 'NOT_TOU' THEN net_energy_Wh ELSE 0 END) as net_energy_not_tou_Wh,
         sum(CASE WHEN tou = 'OFF_PEAK' THEN net_energy_Wh ELSE 0 END) as net_energy_off_peak_Wh,
         sum(CASE WHEN tou = 'ON_PEAK' THEN net_energy_Wh ELSE 0 END) as net_energy_on_peak_Wh,
         sum(CASE WHEN tou = 'PARTIAL_PEAK' THEN net_energy_Wh ELSE 0 END) as net_energy_partial_peak_Wh,

         -- grid costs
         sum(net_cost) as total_net_cost,
         sum(CASE WHEN tou = 'CRITICAL_PEAK' THEN net_cost ELSE 0 END) as net_cost_critical_peak,
         sum(CASE WHEN tou = 'NOT_TOU' THEN net_cost ELSE 0 END) as net_cost_not_tou,
         sum(CASE WHEN tou = 'OFF_PEAK' THEN net_cost ELSE 0 END) as net_cost_off_peak,
         sum(CASE WHEN tou = 'ON_PEAK' THEN net_cost ELSE 0 END) as net_cost_on_peak,
         sum(CASE WHEN tou = 'PARTIAL_PEAK' THEN net_cost ELSE 0 END) as net_cost_partial_peak,

         -- consumption cost
         sum(consumption_cost) as total_consumption_cost, 
         sum(CASE WHEN tou = 'CRITICAL_PEAK' THEN consumption_cost ELSE 0 END) as consumption_cost_critical_peak, 
         sum(CASE WHEN tou = 'NOT_TOU' THEN consumption_cost ELSE 0 END) as consumption_cost_not_tou,
         sum(CASE WHEN tou = 'OFF_PEAK' THEN consumption_cost ELSE 0 END) as consumption_cost_off_peak,
         sum(CASE WHEN tou = 'ON_PEAK' THEN consumption_cost ELSE 0 END) as consumption_cost_on_peak,
         sum(CASE WHEN tou = 'PARTIAL_PEAK' THEN consumption_cost ELSE 0 END) as consumption_cost_partial_peak,
              
         -- generation savings
         sum(generation_savings) as total_generation_savings,
         sum(CASE WHEN tou = 'CRITICAL_PEAK' THEN generation_savings ELSE 0 END) as generation_savings_critical_peak,
         sum(CASE WHEN tou = 'NOT_TOU' THEN generation_savings ELSE 0 END) as generation_savings_not_tou,
         sum(CASE WHEN tou = 'OFF_PEAK' THEN generation_savings ELSE 0 END) as generation_savings_off_peak,
         sum(CASE WHEN tou = 'ON_PEAK' THEN generation_savings ELSE 0 END) as generation_savings_on_peak,
         sum(CASE WHEN tou = 'PARTIAL_PEAK' THEN generation_savings ELSE 0 END) as generation_savings_partial_peak,
         
         -- battery savings
         sum(battery_savings) as total_battery_savings,
         sum(CASE WHEN tou = 'CRITICAL_PEAK' THEN battery_savings ELSE 0 END) as battery_savings_critical_peak,
         sum(CASE WHEN tou = 'NOT_TOU' THEN battery_savings ELSE 0 END) as battery_savings_not_tou,
         sum(CASE WHEN tou = 'OFF_PEAK' THEN battery_savings ELSE 0 END) as battery_savings_off_peak,
         sum(CASE WHEN tou = 'ON_PEAK' THEN battery_savings ELSE 0 END) as battery_savings_on_peak,
         sum(CASE WHEN tou = 'PARTIAL_PEAK' THEN battery_savings ELSE 0 END) as battery_savings_partial_peak,
         
         --sum(savings) as savings, -- do we still need this?
         --sum(CASE WHEN tou = 'CRITICAL_PEAK' THEN savings ELSE 0 END) as savings_CRITICAL_PEAK,
         --sum(CASE WHEN tou = 'NOT_TOU' THEN savings ELSE 0 END) as savings_NOT_TOU,
         --sum(CASE WHEN tou = 'OFF_PEAK' THEN savings ELSE 0 END) as savings_OFF_PEAK,
         --sum(CASE WHEN tou = 'ON_PEAK' THEN savings ELSE 0 END) as savings_ON_PEAK,
         --sum(CASE WHEN tou = 'PARTIAL_PEAK' THEN savings ELSE 0 END) as savings_PARTIAL_PEAK,

         -- attribution energies --
         sum(generation_energy_exported_to_battery_Wh) as generation_energy_exported_to_battery_Wh,
         sum(generation_energy_exported_to_grid_Wh) as generation_energy_exported_to_grid_Wh,
         sum(generation_energy_exported_to_home_Wh) as generation_energy_exported_to_home_Wh,
         sum(consumption_energy_from_battery_Wh) as consumption_energy_from_battery_Wh, 
         sum(consumption_energy_from_grid_Wh) as consumption_energy_from_grid_Wh, 
         sum(consumption_energy_from_solar_Wh) as consumption_energy_from_solar_Wh, 

         -- attribution power --
         round((sum(generation_energy_exported_to_battery_Wh) / sum(time_diff_in_seconds))::numeric,4) as solar_power_exportedToBattery_W,
         round((sum(generation_energy_exported_to_grid_Wh) / sum(time_diff_in_seconds))::numeric,4) as solar_power_exportedToGrid_W,
         round((sum(generation_energy_exported_to_home_Wh) / sum(time_diff_in_seconds))::numeric,4) as solar_power_exportedToHome_W,
         round((sum(consumption_energy_from_battery_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_consumption_power_from_battery_W,
         round((sum(consumption_energy_from_grid_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_consumption_power_from_grid_W,
         round((sum(consumption_energy_from_solar_Wh) / sum(time_diff_in_seconds))::numeric,4) as average_consumption_power_from_solar_W

         FROM pwr.site_energy
         WHERE anomaly_code IS NULL 
         -- and site_id='2937580f-0b92-424e-a4a3-339f875aabfa'
      GROUP BY day, site_id;

