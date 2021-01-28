select * from 
pwr.site_energy_hourly_by_system 
where system_id in ('741015cc-a080-4b70-8b76-3c2a6d860e1f')
and hour > now() - INTERVAL '2 day'
-- and hour between '2020-12-15 18:00:00' and '2020-12-15 20:00:00'
order by hour desc


\timing
select distinct on(system_id) hour, system_id, average_soc from pwr.site_energy_hourly_by_system 
where hour > now()- interval '11 hour' 
and system_id = ANY('{d51348a4-dde5-4380-bf2d-95e33f4b28a1,
 7d34346e-cf7e-4bdb-a39f-0e782ab66710,
 210c45b0-b588-40ce-965c-452b07fda348,
 60e4639a-ef4e-47fa-b921-31469a942e27,
 6ee8d769-e638-45e6-bf20-04af8690de9c,
 2e998674-66aa-413c-ab0b-99b1fc4025fb}')
order by system_id, hour desc


\timing
select distinct on(hour, system_id) hour, system_id, average_soc from pwr.site_energy_hourly_by_system 
where hour > now()- interval '11 hour' 
and system_id = ANY('{d51348a4-dde5-4380-bf2d-95e33f4b28a1,
 7d34346e-cf7e-4bdb-a39f-0e782ab66710,
 210c45b0-b588-40ce-965c-452b07fda348,
 60e4639a-ef4e-47fa-b921-31469a942e27,
 6ee8d769-e638-45e6-bf20-04af8690de9c,
 2e998674-66aa-413c-ab0b-99b1fc4025fb}')
order by  hour desc, system_id

