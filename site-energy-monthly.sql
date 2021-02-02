
CREATE MATERIALIZED VIEW pwr.site_energy_monthly
   AS
      SELECT
         date_trunc('month', day) as month,
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
      GROUP BY month, site_id
      WITH NO DATA;



