SELECT 
           parent.site_id as site_id, 
           COALESCE(MIN(md.timezone), 'UTC') as timezone, 
           -- capabilities 
           SUM(parent.battery) as active_batteries, SUM(parent.pvl) as active_pvls, 
           SUM(parent.inverter) as active_inverters, 
           SUM(parent.icm) as active_icm, 
           SUM(parent.error) as error, 
           MAX(parent.installed_pv) as installed_pv, 
           ROUND(SUM(bat_cap.installed_storage)/1000.0, 1) AS installed_storage, 
           ROUND((SUM(sys_energy.soc * bat_cap.installed_storage) / NULLIF(SUM(bat_cap.installed_storage), 0))::NUMERIC, 1) as soc,  
           ROUND(AVG(bat_cap.soh)::NUMERIC, 1) as soh, 
           -- daily site energies and performance 
           SUM(daily_today.yield_today) as yield_today, 
           SUM(daily_today.consumptionToday) as consumption_today, 
           SUM(daily_today.self_consumption_today) as self_consumption_today, 
           SUM(daily_today.performance_today) as performance_today, 
           SUM(daily_yesterday.yield_yesterday) as yield_yesterday, 
           SUM(daily_yesterday.consumption_yesterday) as consumption_yesterday, 
           SUM(daily_yesterday.self_consumption_yesterday) as self_consumption_yesterday, 
           SUM(daily_yesterday.performance_yesterday) as performance_yesterday, 
           -- monthly site energies and performance
           SUM(monthly_this_month.yield_this_month) as yield_this_month, 
           SUM(monthly_this_month.consumption_this_month) as consumption_this_month, 
           SUM(monthly_this_month.self_consumption_month) as self_consumption_month, 
           SUM(monthly_this_month.avg_performance_this_month) as avg_performance_this_month, 
           SUM(monthly_this_month.performance_ratio_this_month) as performance_ratio_this_month, 
           SUM(monthly_last_month.yield_last_month) as yield_last_month, 
           SUM(monthly_last_month.consumption_last_month) as consumption_last_month, 
           SUM(monthly_last_month.avg_performance_last_month) as avg_performance_last_month, 
           SUM(monthly_last_month.performance_ratio_last_month) as performance_ratio_last_month, 
           -- yearly site energies and performance
           SUM(yearly.yield_this_year) as yield_this_year, 
           SUM(yearly.consumption_this_year) as consumption_this_year, 
           SUM(yearly.self_consumption_year) as self_consumption_year, 
           SUM(yearly.avg_performance_this_year) as avg_performance_this_year, 
           -- determining site status based on priorities 
           MIN( 
               CASE 
                   WHEN parent.host_rcpn is NULL THEN 0 -- N/C
                   WHEN beacon_online = 0 THEN 1 -- OFFLINE
                   ELSE 2 -- ONLINE
               END 
           ) AS status 
        FROM ( 
           SELECT 
               tuples.site_id, tuples.system_id, ds.host_rcpn, tuples.installed_pv, tuples.expected_solar_generation_kWh, tuples.expected_solar_generation_last_month_kWh, 
               -- checking devices for the system status 
               COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.device_type='BEACON' AND ds.timestamp_utc > (now() - '3 hours'::INTERVAL)  limit 1), 0) as beacon_online, 
               -- checking active devices for site capabilities 
               COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.device_type='BATTERY' AND  ds.timestamp_utc > (now() - '90 days'::INTERVAL) limit 1), 0) as  battery, 
               COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.device_type='PVLINK' AND ds.timestamp_utc > (now() - '90 days'::INTERVAL) limit 1), 0) as pvl, 
               COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.device_type='INVERTER' AND ds.timestamp_utc > (now() - '90 days'::INTERVAL) limit 1), 0) as inverter, 
               COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.device_type='ICM' AND ds.timestamp_utc > (now() - '90 days'::INTERVAL) limit 1), 0) as icm, 
               -- if any devices tied to an inverter have an error, then select 1
               COALESCE((select 1 from status.device_shadow d where d.host_rcpn=ds.host_rcpn and d.st between x'7000'::int and x'7FFF'::int limit 1), 0) as error 
           FROM ( 
               SELECT 
                   (js::json->>'siteId')::text as site_id, (js::json->>'systemId')::text as system_id, 
                   (js::json->>'hostRcpn')::text as host_rcpn, (js::json->>'installedPVkW')::real as installed_pv, 
                   (js::json->>'expectedSolarGenerationKWh')::real as expected_solar_generation_kWh, 
                   (js::json->>'expectedSolarGenerationLastMonthKWh')::real as expected_solar_generation_last_month_kWh 
               FROM  
                   unnest(array[ 
           '{"siteId":"site-197f52fd-6687-4a1a-9402-779ce7b1fee3","systemId":"197f52fd-6687-4a1a-9402-779ce7b1fee3","hostRcpn":"000100072AC7", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-fea8c116-46f3-453a-98a5-eff89b7ee43f","systemId":"fea8c116-46f3-453a-98a5-eff89b7ee43f","hostRcpn":"000100072737", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-2390c7ce-e337-4325-93e5-88fa117f300a","systemId":"2390c7ce-e337-4325-93e5-88fa117f300a","hostRcpn":"0001000732E6", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-001ed31d-9260-4570-98d7-b6c149a2f508","systemId":"001ed31d-9260-4570-98d7-b6c149a2f508","hostRcpn":"000100073466", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-ee6e81cc-23b7-4eb1-bd89-cbb0dc4c1554","systemId":"ee6e81cc-23b7-4eb1-bd89-cbb0dc4c1554","hostRcpn":"000100071207", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-e8525ad0-aef3-4c4b-b349-ce02dc088909","systemId":"e8525ad0-aef3-4c4b-b349-ce02dc088909","hostRcpn":"000100072E14", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-4073e359-08da-4b38-8da7-51abb837dc5e","systemId":"4073e359-08da-4b38-8da7-51abb837dc5e","hostRcpn":"000100072204", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-11ddf6d4-425f-453c-b6a2-2cf7f1e49c29","systemId":"11ddf6d4-425f-453c-b6a2-2cf7f1e49c29","hostRcpn":"000100072673", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-c726cc58-a902-40c4-beea-0003423ca953","systemId":"c726cc58-a902-40c4-beea-0003423ca953","hostRcpn":"000100072E85", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-2b307b34-c1eb-4c86-a311-7e204908e310","systemId":"2b307b34-c1eb-4c86-a311-7e204908e310","hostRcpn":"000100072AFD", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-c68c0b71-f470-4ca9-8c61-9fff69caac96","systemId":"c68c0b71-f470-4ca9-8c61-9fff69caac96","hostRcpn":"000100070F2F", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-f057001a-97f9-4465-bb0e-a2aa16514b6b","systemId":"f057001a-97f9-4465-bb0e-a2aa16514b6b","hostRcpn":"000100073350", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-d8270f62-7848-4605-9877-de3db8f2e277","systemId":"d8270f62-7848-4605-9877-de3db8f2e277","hostRcpn":"000100071104", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-3366b764-919d-4a84-8f86-7f477489d6ca","systemId":"3366b764-919d-4a84-8f86-7f477489d6ca","hostRcpn":"0001000732C8", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-6dfd168f-892a-4c32-bdab-7388a9e522c4","systemId":"6dfd168f-892a-4c32-bdab-7388a9e522c4","hostRcpn":"0001000734F3", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-955adfe3-dd2c-4431-8347-ec87bb7c75c6","systemId":"955adfe3-dd2c-4431-8347-ec87bb7c75c6","hostRcpn":"0001000732F9", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-1e03dd8b-f920-432f-af62-e97de86418e0","systemId":"1e03dd8b-f920-432f-af62-e97de86418e0","hostRcpn":"000100072CDD", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-5069d42c-84e3-466d-b811-993c1206aaa4","systemId":"5069d42c-84e3-466d-b811-993c1206aaa4","hostRcpn":"0001000711C6", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-e1d6c08f-8f12-42ee-b4cd-1dae2e1c76ad","systemId":"e1d6c08f-8f12-42ee-b4cd-1dae2e1c76ad","hostRcpn":"000100072D67", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-2f35c3e5-f658-48f6-94e2-696d263aac00","systemId":"2f35c3e5-f658-48f6-94e2-696d263aac00","hostRcpn":"00010007207C", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-92403b49-07a0-477e-8845-5301e89a9ec1","systemId":"92403b49-07a0-477e-8845-5301e89a9ec1","hostRcpn":"000100073403", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-a8b562ef-82c1-4560-9769-e288e91f4d32","systemId":"a8b562ef-82c1-4560-9769-e288e91f4d32","hostRcpn":"000100073180", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-e5bf01ce-c18f-4418-85f4-89ae1d80306d","systemId":"e5bf01ce-c18f-4418-85f4-89ae1d80306d","hostRcpn":"000100073153", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-147ecd80-0b49-4af2-9536-0cd69e0afcd0","systemId":"147ecd80-0b49-4af2-9536-0cd69e0afcd0","hostRcpn":"000100073108", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-212b1bb4-c76e-4405-ac98-3d7d45703ca6","systemId":"212b1bb4-c76e-4405-ac98-3d7d45703ca6","hostRcpn":"0001000734D3", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-8d8aa183-809e-44fd-a893-5bb4ea2312bb","systemId":"8d8aa183-809e-44fd-a893-5bb4ea2312bb","hostRcpn":"0001000732DC", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-3d26b5e1-8764-4a87-b8be-ada39ebd8259","systemId":"3d26b5e1-8764-4a87-b8be-ada39ebd8259","hostRcpn":"0001000732C4", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-acf7c803-d752-432c-8c81-c793d54bbb93","systemId":"acf7c803-d752-432c-8c81-c793d54bbb93","hostRcpn":"0001000734EE", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-ca0cd1ce-dfe7-4181-a269-348877a8f9ca","systemId":"ca0cd1ce-dfe7-4181-a269-348877a8f9ca","hostRcpn":"00010007256C", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-638fa0ce-8319-44cc-9053-a4fc73ad204f","systemId":"638fa0ce-8319-44cc-9053-a4fc73ad204f","hostRcpn":"000100072CDF", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-dc892807-cc74-4723-a031-77254f25cc21","systemId":"dc892807-cc74-4723-a031-77254f25cc21","hostRcpn":"000100073ABD", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-21ae0176-879e-4382-9372-07468dda5cb5","systemId":"21ae0176-879e-4382-9372-07468dda5cb5","hostRcpn":"000100073428", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-84662801-2669-4372-ace1-5cf6832eb8cd","systemId":"84662801-2669-4372-ace1-5cf6832eb8cd","hostRcpn":"000100072D2D", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-3b6a1f11-fcd6-4330-a718-d582736e6a83","systemId":"3b6a1f11-fcd6-4330-a718-d582736e6a83","hostRcpn":"000100072D68", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-b9c4db75-81b1-4f49-a021-7680056132bf","systemId":"b9c4db75-81b1-4f49-a021-7680056132bf","hostRcpn":"000100070ED1", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-114ca6ac-04f3-46cf-88fe-11c0c005e64b","systemId":"114ca6ac-04f3-46cf-88fe-11c0c005e64b","hostRcpn":"000100072357", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-62d4c058-fa57-482e-bfed-2d6b06b01183","systemId":"62d4c058-fa57-482e-bfed-2d6b06b01183","hostRcpn":"000100072121", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-ad298bed-6b46-4be7-8e98-9f9a772eb93c","systemId":"ad298bed-6b46-4be7-8e98-9f9a772eb93c","hostRcpn":"000100073163", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-f8fee386-273b-43b7-8247-fd5c835deb02","systemId":"f8fee386-273b-43b7-8247-fd5c835deb02","hostRcpn":"00010007332B", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-d7e1860a-3339-4a06-8677-3dda434587f8","systemId":"d7e1860a-3339-4a06-8677-3dda434587f8","hostRcpn":"00010007332C", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-533d1bb0-f3a6-4a77-a1ac-a72a6055550d","systemId":"533d1bb0-f3a6-4a77-a1ac-a72a6055550d","hostRcpn":"000100073381", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-aa57a4c9-e88c-4356-afed-6f35b4e7a74d","systemId":"aa57a4c9-e88c-4356-afed-6f35b4e7a74d","hostRcpn":"0001000725EE", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-51a27b9c-7214-4bf8-ac10-c9f5b83b43e3","systemId":"51a27b9c-7214-4bf8-ac10-c9f5b83b43e3","hostRcpn":"0001000733FA", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-38ff2323-0336-499a-b412-6d07a1df1de9","systemId":"38ff2323-0336-499a-b412-6d07a1df1de9","hostRcpn":"000100072E10", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-2264efbd-0f3a-4fea-93de-9c06ed2057d9","systemId":"2264efbd-0f3a-4fea-93de-9c06ed2057d9","hostRcpn":"000100072D3C", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-2acabe9b-04e4-4edf-aabc-c09012eed0f1","systemId":"2acabe9b-04e4-4edf-aabc-c09012eed0f1","hostRcpn":"000100072933", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-1fa63e9a-4437-4563-955f-817198338b15","systemId":"1fa63e9a-4437-4563-955f-817198338b15","hostRcpn":"000100073015", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-c2996a04-8bc5-42f2-afc1-78097498ee2f","systemId":"c2996a04-8bc5-42f2-afc1-78097498ee2f","hostRcpn":"00010007300C", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-a0c8dc14-7afe-44b2-80f8-7a6bb100c6ae","systemId":"a0c8dc14-7afe-44b2-80f8-7a6bb100c6ae","hostRcpn":"0001000711B0", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-987c5ffe-c72d-45b5-b938-9502239bcfc2","systemId":"987c5ffe-c72d-45b5-b938-9502239bcfc2","hostRcpn":"0001000732DF", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-e1189f5a-2a9a-4b65-9fce-fde75ca40807","systemId":"e1189f5a-2a9a-4b65-9fce-fde75ca40807","hostRcpn":"000100073D06", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-6e2dce0c-a60b-4c6a-a337-a908fdfacfc4","systemId":"6e2dce0c-a60b-4c6a-a337-a908fdfacfc4","hostRcpn":"000100072026", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-b8dbdccf-b586-4a05-b0f8-3007dd1f7aa0","systemId":"b8dbdccf-b586-4a05-b0f8-3007dd1f7aa0","hostRcpn":"0001000733E1", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-4df33f88-99fd-4618-b9ab-b8c1b0eb40df","systemId":"4df33f88-99fd-4618-b9ab-b8c1b0eb40df","hostRcpn":"00010007123A", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-7dbacf0a-e470-40f4-8097-58c5b2df9bac","systemId":"7dbacf0a-e470-40f4-8097-58c5b2df9bac","hostRcpn":"000100073349", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-9f0d5324-916b-4bae-887b-89c6e03dd038","systemId":"9f0d5324-916b-4bae-887b-89c6e03dd038","hostRcpn":"0001000721E3", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-bc61990d-4a86-4a82-8201-60718e46d278","systemId":"bc61990d-4a86-4a82-8201-60718e46d278","hostRcpn":"00010007231A", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-f624361d-d799-4701-af95-4cdff3a5e755","systemId":"f624361d-d799-4701-af95-4cdff3a5e755","hostRcpn":"00010007254B", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-1b2ca6b9-eec0-4f12-86d2-465d55afee05","systemId":"1b2ca6b9-eec0-4f12-86d2-465d55afee05","hostRcpn":"0001000726D6", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-9647b6b8-b396-4305-9e02-377d4b3aa08a","systemId":"9647b6b8-b396-4305-9e02-377d4b3aa08a","hostRcpn":"000100072EF9", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-929572c8-e071-4a9a-aaa0-8f5e2ab1eea6","systemId":"929572c8-e071-4a9a-aaa0-8f5e2ab1eea6","hostRcpn":"0001000725FA", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-7929733c-c8ae-403a-9190-9ff9ddeaac5b","systemId":"7929733c-c8ae-403a-9190-9ff9ddeaac5b","hostRcpn":"000100072B10", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-cf2ebf94-efd0-450b-9373-772f10892a74","systemId":"cf2ebf94-efd0-450b-9373-772f10892a74","hostRcpn":"0001000717C7", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-a3b7368a-f4c2-4fd3-a98b-7dbcd7d63401","systemId":"a3b7368a-f4c2-4fd3-a98b-7dbcd7d63401","hostRcpn":"0001000726EC", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-b63084fd-6a64-40f8-833e-585361a7bd8d","systemId":"b63084fd-6a64-40f8-833e-585361a7bd8d","hostRcpn":"0001000710EE", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-13de607c-defd-4f84-93cd-7d1c795e9ce2","systemId":"13de607c-defd-4f84-93cd-7d1c795e9ce2","hostRcpn":"000100072630", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-7a7ad4c3-1841-478d-b463-6859c257fcd8","systemId":"7a7ad4c3-1841-478d-b463-6859c257fcd8","hostRcpn":"0001000733FE", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-2d005a15-b4ef-4f5d-b510-9658196e12d5","systemId":"2d005a15-b4ef-4f5d-b510-9658196e12d5","hostRcpn":"000100072962", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-905084d9-d002-4700-8303-af0ba4acf17e","systemId":"905084d9-d002-4700-8303-af0ba4acf17e","hostRcpn":"0001000731DD", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-51729dca-5b72-4094-bc3c-6416ab1066fc","systemId":"51729dca-5b72-4094-bc3c-6416ab1066fc","hostRcpn":"000100070E12", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-7223b49b-a798-4b62-88e1-4367e0357b67","systemId":"7223b49b-a798-4b62-88e1-4367e0357b67","hostRcpn":"000100071214", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-149b86d6-a257-4021-b9ff-0c42cc01dfd7","systemId":"149b86d6-a257-4021-b9ff-0c42cc01dfd7","hostRcpn":"000100072CFC", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-71cd4370-dc4b-448c-83d9-154c43e5bd6c","systemId":"71cd4370-dc4b-448c-83d9-154c43e5bd6c","hostRcpn":"000100071213", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-fec2752d-b3f6-4396-abe8-69f65e02ac61","systemId":"fec2752d-b3f6-4396-abe8-69f65e02ac61","hostRcpn":"0001000725D4", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-8b3d38f9-0718-4781-bc69-a02e20a2f9d4","systemId":"8b3d38f9-0718-4781-bc69-a02e20a2f9d4","hostRcpn":"00010007321D", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-e693f90f-85fb-42aa-8e7a-fa710e9e2385","systemId":"e693f90f-85fb-42aa-8e7a-fa710e9e2385","hostRcpn":"000100073088", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-20ec4c32-c8ca-47ea-9e74-9947707e2f72","systemId":"20ec4c32-c8ca-47ea-9e74-9947707e2f72","hostRcpn":"000100072F59", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-43fa9621-d75d-4748-ada6-a22ec43bdfc5","systemId":"43fa9621-d75d-4748-ada6-a22ec43bdfc5","hostRcpn":"0001000730A8", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-5789adb5-d374-4342-b6b7-ee7ecb7a8db3","systemId":"5789adb5-d374-4342-b6b7-ee7ecb7a8db3","hostRcpn":"00010007450E", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-bedcb49e-e159-472b-b2b1-592cd1c2f059","systemId":"bedcb49e-e159-472b-b2b1-592cd1c2f059","hostRcpn":"000100073070", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-fa2b5427-e1a9-4905-b048-2584780bb909","systemId":"fa2b5427-e1a9-4905-b048-2584780bb909","hostRcpn":"00010007336C", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-05cab21b-5edc-4098-a01a-ebfe8528244b","systemId":"05cab21b-5edc-4098-a01a-ebfe8528244b","hostRcpn":"000100073109", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-e2ba56af-1828-4e0b-a3e9-9120d7e02799","systemId":"e2ba56af-1828-4e0b-a3e9-9120d7e02799","hostRcpn":"00010007179D", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-8e2a35c7-31a2-4805-a489-971212d9abaa","systemId":"8e2a35c7-31a2-4805-a489-971212d9abaa","hostRcpn":"000100073011", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-59c3046b-0c12-4d73-a05f-02c9d385cab9","systemId":"59c3046b-0c12-4d73-a05f-02c9d385cab9","hostRcpn":"00010007333F", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-1b386779-11e3-48db-a466-d6556645e89c","systemId":"1b386779-11e3-48db-a466-d6556645e89c","hostRcpn":"0001000723DA", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-853c7452-f031-44a2-b18d-25a7dfbccd3c","systemId":"853c7452-f031-44a2-b18d-25a7dfbccd3c","hostRcpn":"000100071166", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-1aed287b-6cd3-40d0-9382-eb35cd650701","systemId":"1aed287b-6cd3-40d0-9382-eb35cd650701","hostRcpn":"0001000732FD", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-beedc3dd-224a-4f77-88d9-2bdb4e282470","systemId":"beedc3dd-224a-4f77-88d9-2bdb4e282470","hostRcpn":"000100072FDE", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-a8b65738-906d-4f41-a537-7b5ac241c5ac","systemId":"a8b65738-906d-4f41-a537-7b5ac241c5ac","hostRcpn":"00010007267E", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-edc8276c-45d0-49d1-89ee-ab8a6da706b5","systemId":"edc8276c-45d0-49d1-89ee-ab8a6da706b5","hostRcpn":"00010007267D", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-596f97dd-c172-40bf-90b6-07250eaf1be7","systemId":"596f97dd-c172-40bf-90b6-07250eaf1be7","hostRcpn":"000100073534", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-7fe54dcf-5181-4cc4-b264-93ac9d5e8629","systemId":"7fe54dcf-5181-4cc4-b264-93ac9d5e8629","hostRcpn":"000100071F6A", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-19b26855-92a4-4746-91ff-2adc15f8f737","systemId":"19b26855-92a4-4746-91ff-2adc15f8f737","hostRcpn":"0001000733A2", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-19f5fddc-b5cb-4eda-aafd-57511f89e609","systemId":"19f5fddc-b5cb-4eda-aafd-57511f89e609","hostRcpn":"0001000725E7", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-2081cce5-0f49-4b5d-9a95-8d5c67396715","systemId":"2081cce5-0f49-4b5d-9a95-8d5c67396715","hostRcpn":"0001000720AF", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-b11d7eda-0435-4724-a1ef-a22e46acfc8d","systemId":"b11d7eda-0435-4724-a1ef-a22e46acfc8d","hostRcpn":"000100071114", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-efcf099e-814e-4f15-a317-3bdb4eb37689","systemId":"efcf099e-814e-4f15-a317-3bdb4eb37689","hostRcpn":"00010007334B", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-079f8685-aefd-4193-bed6-29289957b199","systemId":"079f8685-aefd-4193-bed6-29289957b199","hostRcpn":"000100073091", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-63fde88d-7d3b-45fa-9710-6aec30dcc5cc","systemId":"63fde88d-7d3b-45fa-9710-6aec30dcc5cc","hostRcpn":"000100072CC7", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-9289754b-137b-4d2d-9c34-22e68b4d89ae","systemId":"9289754b-137b-4d2d-9c34-22e68b4d89ae","hostRcpn":"000100073520", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-e8b4222e-75b0-44d0-a0ac-304310c7fa0a","systemId":"e8b4222e-75b0-44d0-a0ac-304310c7fa0a","hostRcpn":"0001000735A6", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-1d751611-22a3-490a-a1cb-7adac2517d94","systemId":"1d751611-22a3-490a-a1cb-7adac2517d94","hostRcpn":"000100073610", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-71c26372-1a4d-4e7c-8f12-64b2251895fc","systemId":"71c26372-1a4d-4e7c-8f12-64b2251895fc","hostRcpn":"0001000732F1", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-b14ac6b1-d334-488e-b402-d61b872716b1","systemId":"b14ac6b1-d334-488e-b402-d61b872716b1","hostRcpn":"0001000742A8", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-721743e2-d972-4eaf-acc6-92a6e8efe447","systemId":"721743e2-d972-4eaf-acc6-92a6e8efe447","hostRcpn":"0001000732DD", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-81222e33-a642-48f5-a113-ae8548d149df","systemId":"81222e33-a642-48f5-a113-ae8548d149df","hostRcpn":"0001000732D1", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-6a044b50-c31d-43e9-81e3-048546cf885b","systemId":"6a044b50-c31d-43e9-81e3-048546cf885b","hostRcpn":"0001000734BD", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-f15dea3c-98cd-4cd2-b0f2-ba64c7588353","systemId":"f15dea3c-98cd-4cd2-b0f2-ba64c7588353","hostRcpn":"00010007358C", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-dc99f677-825a-4590-afca-94515bcae38c","systemId":"dc99f677-825a-4590-afca-94515bcae38c","hostRcpn":"0001000732E8", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-140938c8-5ab4-483a-bfa8-f8da58b829c1","systemId":"140938c8-5ab4-483a-bfa8-f8da58b829c1","hostRcpn":"0001000734B1", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-503f273b-cce5-46a6-b672-48c2de0d3c1b","systemId":"503f273b-cce5-46a6-b672-48c2de0d3c1b","hostRcpn":"0001000732CB", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-226cc720-eed5-470f-94c6-79c0c71638f4","systemId":"226cc720-eed5-470f-94c6-79c0c71638f4","hostRcpn":"0001000732C9", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-9730a325-a780-4b91-b289-14d9f496b5c0","systemId":"9730a325-a780-4b91-b289-14d9f496b5c0","hostRcpn":"000100073643", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-78eef2f7-7510-453f-84ae-e66cb4b94bb6","systemId":"78eef2f7-7510-453f-84ae-e66cb4b94bb6","hostRcpn":"0001000732D2", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-553c6c11-903f-40d8-9894-99b39cdf90f8","systemId":"553c6c11-903f-40d8-9894-99b39cdf90f8","hostRcpn":"0001000732F7", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-5948492d-7539-4113-a122-208a86da6869","systemId":"5948492d-7539-4113-a122-208a86da6869","hostRcpn":"000100073482", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-8309d994-c498-4f82-8efb-a1f34fc8cd0e","systemId":"8309d994-c498-4f82-8efb-a1f34fc8cd0e","hostRcpn":"0001000732E5", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-f2dc022f-3586-4593-9f21-31b4f81ed9be","systemId":"f2dc022f-3586-4593-9f21-31b4f81ed9be","hostRcpn":"0001000732D4", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}',
 '{"siteId":"site-768283cb-85c4-456a-87a3-16604f1069fc","systemId":"768283cb-85c4-456a-87a3-16604f1069fc","hostRcpn":"00010007195F", "installedPVkW":7.5,"expectedSolarGenerationKWh":550,"expectedSolarGenerationLastMonthKWh":550}'

            ] ) as js 
           ) as tuples 
           -- filtering by device shadow hostRcpn to verify js.host_rcpn 
           LEFT JOIN status.device_shadow ds on ds.device_id=tuples.host_rcpn and ds.host_rcpn=tuples.host_rcpn 
        ) as parent 
        -- join on metadata for a timezone 
        LEFT JOIN pwr.system_meta_data md ON md.system_id = parent.system_id  
        -- join on time intervals used for calculations below
        LEFT JOIN LATERAL ( 
           SELECT 
               -- might want to shift by 1 second so we never get 0 
               EXTRACT(EPOCH FROM (NOW() at time zone timezone - DATE_TRUNC('month', NOW() at time zone timezone)) ) / 60 AS minutes_since_start_of_month, 
               EXTRACT(EPOCH FROM (NOW() at time zone timezone - DATE_TRUNC('month', NOW() at time zone timezone)) ) / 60 / 60 / 24 AS days_since_start_of_month, 
               EXTRACT(EPOCH FROM (DATE_TRUNC('month', NOW() at time zone timezone) + '1 MONTH'::INTERVAL) - (DATE_TRUNC('month', NOW() at time zone timezone)) ) / 60 AS minutes_in_current_month, 
               EXTRACT(EPOCH FROM (DATE_TRUNC('month', NOW() at time zone timezone)) - (DATE_TRUNC('month', NOW() at time zone timezone) - '1 MONTH'::INTERVAL) ) / 60 / 60 / 24 AS days_in_last_month, 
               EXTRACT(EPOCH FROM (NOW() at time zone timezone) - (DATE_TRUNC('year', NOW() at time zone timezone))) / 60 / 60 /24 AS days_since_start_of_the_year, 
               EXTRACT(EPOCH FROM (DATE_TRUNC('year', NOW() at time zone timezone)) - (DATE_TRUNC('year', NOW() at time zone timezone) - '1 YEAR'::INTERVAL)) / 60 / 60 / 24 AS days_in_last_year 
        ) DT_CALC on true 
        -- join on battery capabilities 
        LEFT JOIN LATERAL ( 
           SELECT 
               bat.host_rcpn, SUM((np.nameplate::json->>'wh_rtg')::int) as installed_storage, AVG(soh) as soh 
           FROM status.device_shadow bat 
           LEFT JOIN status.nameplate np ON np.device_id = bat.device_id 
           LEFT JOIN LATERAL ( 
               SELECT AVG(bm.soh) as soh, AVG(bm.soc) as soc 
               FROM status.battery_module bm 
               WHERE bm.device_id = bat.device_id AND bm.timestamp_utc > NOW() - INTERVAL '1 day' 
               GROUP BY bm.timestamp_utc 
               ORDER BY bm.timestamp_utc desc 
               LIMIT 1 
           ) as bm on true 
           WHERE bat.host_rcpn = parent.host_rcpn AND bat.device_type = 'BATTERY' 
        AND (np.timestamp_utc = 
                (SELECT max(timestamp_utc)  
                 FROM status.nameplate 
                 WHERE device_id=bat.device_id ) ) 
           GROUP BY host_rcpn 
        ) bat_cap ON true 
        -- join on system energy 
        LEFT JOIN LATERAL ( 
           SELECT 
               soc 
           FROM pwr.system_energy 
           WHERE timestamp_local >= NOW() - INTERVAL '1 day' AND timestamp_local <= NOW() AND system_id = parent.system_id 
           ORDER BY timestamp_local desc 
           LIMIT 1 
        ) as sys_energy ON true 
        -- join on daily table for today 
        LEFT JOIN LATERAL ( 
           SELECT 
              "generation_energy_exported_Wh" as yield_today, 
               "consumption_energy_imported_Wh" as consumptionToday, 
               LEAST(GREATEST(COALESCE("consumption_energy_imported_from_solar_Wh" / NULLIF("generation_energy_exported_Wh", 0), 0) * 100, 0), 100) as self_consumption_today, 
               CASE 
                   WHEN parent.installed_pv > 0 THEN "generation_energy_exported_Wh"/ parent.installed_pv 
                   WHEN parent.installed_pv IS NULL THEN NULL 
                   ELSE 0 
               END as performance_today 
               FROM pwr.system_energy_daily 
               WHERE daily = date_trunc('day', NOW() at time zone timezone) AND system_id = parent.system_id 
               AND daily >= NOW() - INTERVAL '1 day' 
               AND daily <= NOW() + INTERVAL '1 day' 
               ORDER BY daily desc 
               LIMIT 1 
        ) as daily_today ON true 
        -- join on daily table for yesterday 
        LEFT JOIN LATERAL ( 
           SELECT 
               "generation_energy_exported_Wh" as yield_yesterday, 
               "consumption_energy_imported_Wh" as consumption_yesterday, 
               LEAST(GREATEST(COALESCE("consumption_energy_imported_from_solar_Wh" / NULLIF("generation_energy_exported_Wh", 0), 0) * 100, 0), 100) as self_consumption_yesterday, 
               CASE 
                   WHEN parent.installed_pv > 0 THEN "generation_energy_exported_Wh"/ parent.installed_pv 
                   WHEN parent.installed_pv IS NULL THEN NULL 
                   ELSE 0 
               END as performance_yesterday 
               FROM pwr.system_energy_daily 
               WHERE daily = date_trunc('day', NOW() at time zone timezone) - INTERVAL '1 day' AND system_id = parent.system_id 
               AND daily >= NOW() - INTERVAL '2 day' 
               AND daily <= NOW()
               ORDER BY daily desc 
               LIMIT 1 
        ) as daily_yesterday ON true 
        -- join on monthly table this month 
        LEFT JOIN LATERAL ( 
           SELECT 
               "generation_energy_exported_Wh" as yield_this_month, 
               "consumption_energy_imported_Wh" as consumption_this_month, 
               LEAST(GREATEST(COALESCE("consumption_energy_imported_from_solar_Wh" / NULLIF("generation_energy_exported_Wh", 0), 0) * 100, 0), 100) as self_consumption_month, 
               CASE 
                   WHEN parent.installed_pv > 0 THEN "generation_energy_exported_Wh"/ parent.installed_pv / NULLIF(DT_CALC.days_since_start_of_month, 0) 
                   WHEN parent.installed_pv IS NULL THEN NULL 
                   ELSE 0 
               END as avg_performance_this_month, 
               CASE 
                   WHEN parent.expected_solar_generation_kWh > 0 THEN GREATEST("generation_energy_exported_Wh" / 1000 / ( parent.expected_solar_generation_kWh * NULLIF(DT_CALC.minutes_since_start_of_month, 0) / DT_CALC.minutes_in_current_month) * 100, 0)  
                   ELSE 0 
               END as performance_ratio_this_month 
               FROM pwr.system_energy_monthly_materialized 
               WHERE monthly = date_trunc('month', NOW() at time zone timezone) AND system_id = parent.system_id 
               ORDER BY monthly desc 
               limit 1 
        ) monthly_this_month ON true 
        -- join on monthly table last month 
        LEFT JOIN LATERAL ( 
           SELECT 
               "generation_energy_exported_Wh" as yield_last_month, 
               "consumption_energy_imported_Wh" as consumption_last_month, 
               CASE 
                   WHEN parent.installed_pv > 0 THEN "generation_energy_exported_Wh"/ parent.installed_pv / DT_CALC.days_in_last_month 
                   WHEN parent.installed_pv IS NULL THEN NULL 
                   ELSE 0 
               END as avg_performance_last_month, 
               CASE 
                   WHEN parent.expected_solar_generation_last_month_kWh > 0 THEN GREATEST("generation_energy_exported_Wh" / 1000 / ( parent.expected_solar_generation_last_month_kWh) * 100, 0) 
                   ELSE 0 
               END as performance_ratio_last_month 
               FROM pwr.system_energy_monthly_materialized 
               WHERE monthly = date_trunc('month', NOW() at time zone timezone) - INTERVAL '1 month' AND system_id = parent.system_id 
               ORDER BY monthly desc 
               limit 1 
        ) monthly_last_month ON true 
        -- join on yearly table 
        LEFT JOIN LATERAL ( 
           SELECT 
           "generation_energy_exported_Wh" as yield_this_year, 
           "consumption_energy_imported_Wh" as consumption_this_year, 
           LEAST(GREATEST(COALESCE("consumption_energy_imported_from_solar_Wh" / NULLIF("generation_energy_exported_Wh", 0), 0) * 100, 0), 100) as self_consumption_year, 
           CASE 
               WHEN parent.installed_pv > 0 THEN "generation_energy_exported_Wh"/ parent.installed_pv / NULLIF(DT_CALC.days_since_start_of_the_year, 0) 
               WHEN parent.installed_pv IS NULL THEN NULL 
               ELSE 0 
           END as avg_performance_this_year 
           FROM pwr.system_energy_yearly_materialized 
           WHERE yearly = date_trunc('year', NOW() at time zone timezone) AND system_id = parent.system_id 
           ORDER BY yearly desc 
           LIMIT 1 
        ) yearly ON true 
        -- group on site_id from tuples passed in 
        GROUP BY parent.site_id 
        -- sort on 1 to n columns 
        ORDER BY parent.site_id 