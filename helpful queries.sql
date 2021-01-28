
drop view battery_module_hourly CASCADE

CREATE MATERIALIZED VIEW mv_battery_module_hourly as TABLE battery_module_hourly WITH NO DATA

CREATE TABLE battery_module_hourly_old
AS
SELECT *
  FROM battery_module_hourly

explain analyze
select * from battery_module_hourly
where hour < now()- INTERVAL '10 days'
UNION
select * from battery_module_hourly_old
where hour < now()- INTERVAL '10 days'


insert into battery_module_hourly_old(hour,device_id,SOC,SOH) 
  values( now() - INTERVAL '1 days', 'AAAAA', 99,98)

  SELECT * FROM chunk_relation_size_pretty('status.new');



SELECT create_hypertable('battery_module_hourly_old', 'hour', chunk_time_interval => INTERVAL '7 day', migrate_data => true);


insert into battery_module_hourly_old

\timing
select count(device_id) from (
select device_id, timestamp_utc from status.legacy_status where timestamp_utc=now()-INTERVAL '10 day'
except 
select device_id, timestamp_utc from status.legacy_status_state_change where timestamp_utc=now()-INTERVAL '10 day') as foo

select * from status.legacy_status order by timestamp_utc desc limit 10;


SELECT bpcc.*,
       SCHEMA_NAME,
       TABLE_NAME
FROM _timescaledb_config.bgw_policy_compress_chunks AS bpcc
LEFT JOIN _timescaledb_catalog.hypertable ON (hypertable_id=id)

insert into battery_module(timestamp_utc,timestamp_local,device_id,SN,SOC,SOH) 
  values( now() - INTERVAL '1 days', now() - INTERVAL '1 days', 'AAAAA', 1, 22,23)


refresh materialized view battery_module_hourly

SELECT * FROM chunk_relation_size_pretty('battery_module');

insert into battery_module(timestamp_local, timestamp_utc, device_id, SN, SOC, SOH)
  values(now()-INTERVAL '14 days',now()-INTERVAL '14 days','AAAAA',3,91,77),(now(),now(),'AAAAA',4,66,92)

SELECT drop_chunks(INTERVAL '10 days', 'battery_module',  cascade_to_materialization => FALSE);

ALTER VIEW battery_module_hourly SET (
   timescaledb.ignore_invalidation_older_than = '1 day'
);


SELECT drop_chunks(INTERVAL '2 days', 'battery_module', cascade_to_materializations=>true);


select * from public.site_energy order by timestamp desc limit 10;

delete from
 _timescaledb_config.bgw_policy_compress_chunks 

SELECT add_compress_chunks_policy('status.legacy_status', INTERVAL '1 hour');

select count(device_id) from status.legacy_status;
SELECT * FROM chunk_relation_size_pretty('status.legacy_status');
select now(), hypertable_name, chunk_name, compression_status, uncompressed_total_bytes, compressed_total_bytes from timescaledb_information.compressed_chunk_stats;

SELECT compress_chunk('_timescaledb_internal._hyper_123_61_chunk');

explain analyze select * from status.legacy_status where device_id IN 
('183756414',
 '183756430',
 '183756431',
 '183756433',
 '183756434',
 '18375643') and timestamp_utc > '2020-10-04' and st> 900 order by timestamp_utc desc;

SELECT * FROM indexes_relation_size_pretty ('status.legacy_status');

SELECT * FROM hypertable_relation_size_pretty('status.legacy_status');


select * from status.legacy_status ORDER BY timestamp_utc desc limit 10;

select distinct device_id, host_rcpn, timestamp_utc from status.legacy_status order by timestamp_utc desc limit 40

SELECT * FROM hypertable_approximate_row_count('status.legacy_status');

show table ess;





SELECT max(timestamp_utc) from status.legacy_status

CREATE INDEX on status.legacy_status (host_rcpn, timestamp_utc DESC);


EXPLAIN ANALYZE
\timing
SELECT max(timestamp_utc), device_id, St
FROM status.legacy_status
WHERE device_id = '000100039867' 
GROUP BY device_id, st, timestamp_utc
ORDER BY timestamp_utc DESC
LIMIT 1



select * from status.legacy_status where device_id='000100120E22' order by timestamp_utc desc;

EXPLAIN ANALYZE
\timing

SELECT DISTINCT ON (device_id) device_id, host_rcpn, beacon_rcpn, device_type, timestamp_utc, St
FROM status.legacy_status
--WHERE timestamp_utc > NOW() - INTERVAL '1 day'
WHERE device_id = ANY('{   000100070B7A, 0001000712A0, 000100071B22, 000100071514, 000100071DC5, 000100071F7A, 0001000710E0, 000100070FE5, 000100071F51, 000100071235, 000100070F10, 000100071526, 0001000717C4, 000100070A71, 000100071B19, 00010007252F, 000100070F6A, 000100070AF4, 00010007117E, 0001000716ED, 000100071D78, 00010007160F, 0001000707D7, 00010007196F, 0001000706DC, 000100071566, 000100070738, 000100070644, 000100072451, 0001000715A5, 000100072183, 000100071445, 000100070AD2, 0001000701C2, 00010007286E, 0001000710BA, 0001000721E9, 000100070A0C, 000100072915, 000100070B70, 000100072784, 0001000717CA, 00010007149B, 000100071E2C, 0001000712E3, 000100070B6D, 000100071177, 00010007185D, 00010007045D, 000100071A81, 000100070881, 000100071C02, 0001000723CD, 000100072058, 000100070FC1, 000100072D5B, 0001000728A8, 000100071896, 000100071F81, 00010007105A, 0001000716E7, 000100071664, 000100071465, 000100071C29, 000100071990, 0001000710E6, 000100071980, 000100070746, 0001000710A1, 000100071F3E, 00010007203C, 000100070BCC, 000100071CC0, 000100070650, 0001000724A4, 000100070B21, 000100070663, 0001000704A1, 0001000724A8, 000100071B41, 000100071517, 000100070B18, 000100072264, 000100070557, 000100070731, 000100072E86, 00010007027F, 0001000714BB, 000100070518, 0001000717A8, 000100072738, 000100071D8F, 000100070884, 0001000719EF, 000100073242, 000100072EBC, 0001000716D6, 0001000717B2, 00010007325D, 00010007172D, 00010007041B, 000100072794, 000100071950, 000100071663, 000100070A97, 000100070690, 0001000720AF, 000100071984, 000100071B2A, 000100070BA3, 000100072395, 000100071860, 00010007175D, 00010007162C, 000100072C14, 000100071E54, 000100071D80, 000100071F39, 000100071554, 000100071ED0, 00010007083C, 000100071E8F, 000100070D44, 00010007106E, 000100070FDA, 000100071CEF, 0001000721E6, 000100070FE0, 000100071B0E, 0001000712B0, 000100071AD9, 0001000722AC, 000100071D90, 000100072CE6, 0001000724C3, 000100071DAB, 000100070A89, 00010007205C, 0001000722A2, 000100071D23, 000100071E1C, 0001000719E3, 000100070E6C, 000100072332, 000100071842, 00010007290A, 0001000718E3, 000100070C2A, 000100072161, 000100072590, 000100070A31, 0001000715D3, 00010007147B, 0001000720F2, 000100071BD9, 00010007197A, 000100071708, 000100071255, 0001000723E5, 000100071926, 000100070BD7, 000100070A27, 0001000704E7, 0001000710B6, 000100072820, 0001000727BF, 0001000718D0, 000100070627, 000100071236, 000100071621, 000100070B4A, 0001000717A9, 000100070C91, 000100071643, 000100071769, 000100070F42, 00010007222C, 000100070824, 000100071EC5, 0001000712D2, 000100072111, 000100071ACA, 00010007105F, 000100070EC5, 000100070AF5, 000100071592, 000100070FB8, 000100071571, 0001000728D3, 0001000729A3, 000100070B40, 000100070B59, 00010007140B, 0001000700A1, 00010007174A, 0001000721AB, 000100072043, 000100072C05, 0001000714E0, 00010007054C, 000100071F14, 00010007084A, 000100071E87, 0001000725E5, 000100071942, 000100070797, 0001000717A1, 0001000716BB, 000100071638, 00010007051C, 0001000709F1, 0001000707E6, 000100070BFD, 000100071690, 00010007211A, 000100071A29, 000100071616, 000100070F52, 000100070B33, 000100071F16, 0001000705E8, 000100071F6B, 000100070765, 000100071D27, 0001000718F0, 0001000719AA, 00010007143A, 00010007256C, 000100071A0E, 000100071DBD, 0001000712A9, 000100071421, 000100070748, 00010007250A, 0001000720FE, 000100071F92, 000100071618, 000100070669, 000100072254, 00010007126B, 000100071CD8, 000100072535, 000100071C67, 000100071B3D, 00010007165F, 000100070D5B, 00010007047C, 000100070D54, 000100071A63, 0001000708E0, 000100071EBE, 000100070D0A, 0001000711DF, 00010007147E, 0001000714D9, 00010007224C, 0001000710B7, 000100070C30, 0001000708F0, 000100070D6F, 00010007251E, 00010007170D, 00010007184B, 00010007043B, 000100071507, 000100071B15, 000100071A6B, 000100070C15, 0001000718AE, 00010007169B, 0001000721CD, 000100071DE1, 0001000728C5, 00010007095F, 000100070F44, 000100070C61, 00010007123C, 000100071D79, 000100070FE8, 000100071F44, 000100071067, 0001000718B9, 000100070C29, 00010007291A, 000100072E2B, 00010007183D, 000100070B16, 00010007223A, 000100070A25, 000100070BB7, 00010007148B, 00010007196E, 000100071FFF, 0001000718DC, 000100071527, 00010007156C, 000100070EF0, 000100072648, 00010007192B, 0001000721DF, 000100070C34, 000100071008, 000100070F5E, 000100071A73, 00010007162E, 000100071431, 000100072E3E, 000100071195, 000100071ABB, 000100071433, 0001000718C9, 00010007209C, 0001000718EC, 00010007078F, 000100072802, 000100070A8C, 000100071B67, 00010007159E, 000100071C19, 00010007126D, 000100072562, 0001000715AD, 000100072458, 000100070572, 000100070728, 000100072139, 000100070F8A, 00010007106B, 000100072647, 000100070735, 0001000724D1, 000100071400, 00010007123F, 0001000705B0, 0001000702E0, 000100071420, 000100071D05, 000100071FB3, 0001000707C1, 000100071B4C, 0001000727EA, 0001000712E5, 000100071610, 000100071B58, 00010007242D, 000100071EE4, 000100070EAA, 00010007086B, 000100071AD7, 000100071CA2, 000100071506, 000100070480, 000100073274, 000100071B9D, 0001000708BC, 0001000719A0, 0001000714F2, 00010007151D, 0001000721B3, 0001000726F1, 000100071DFE, 000100070F35, 000100071CD7, 000100070CAD, 000100070664, 000100070AC1, 0001000720E1, 000100070B1F, 000100071595, 000100071899, 000100072900, 00010007187A, 00010007187E, 000100070CAA, 000100070B07, 00010007161E, 000100071AE9, 000100070D2C, 000100072136, 000100070656, 0001000717BD, 0001000719EC, 000100070867, 00010007158C, 000100071939, 0001000712AA, 000100071794, 000100071556, 000100072742, 00010007143F, 000100071A49, 000100071F2D, 000100071823, 000100071BB7, 00010007265B, 00010007023B, 0001000716A6, 000100071F5E, 000100071A67, 00010007205B, 000100071A5F, 00010007215C, 000100070751, 0001000707D2, 000100071AD5, 000100070C2E, 000100070499, 00010007047F, 0001000705BA, 000100072808, 000100070D88, 000100070F15, 000100070EDA, 000100071CB6, 000100071C8D, 000100071BCE, 000100071696, 000100072F6A, 00010007163D, 000100070ADF, 000100070C0E, 00010007140D, 000100071E20, 000100070C7F, 0001000715DA, 0001000712C5, 000100071A7F, 00010007093F, 0001000725C4, 000100072074, 000100070D96, 00010007158E, 000100071F98, 0001000705F6, 0001000706FB, 000100071C5A, 0001000719BB, 000100071723, 000100071A33, 0001000714B0, 00010007071A, 000100070B5F, 000100070B91, 00010007040D, 000100071BF9, 0001000716F0, 000100070378, 000100071A2A, 000100072508, 000100071947, 000100071B4D, 00010007103C, 000100071DA0, 00010007260A, 000100070B04, 00010007180A, 000100070B5C, 0001000705B8, 00010007058D, 000100071DC1, 000100071E11, 000100070D94, 0001000709F3, 0001000714B2, 000100070E69, 0001000728AC, 0001000706E1, 00010007194E, 000100072035, 000100071E74, 000100070C75, 000100072B2B, 00010007200D, 000100070C66, 000100070B5A, 000100071C09, 0001000725A5, 00010007117A, 0001000719D3, 000100071C0B, 0001000706A2, 0001000707B6, 000100071E13, 000100071E7C, 000100071979, 000100071776, 000100071A50, 000100070161, 00010007125A, 000100071978, 0001000719B9, 000100070E22, 0001000707B5, 000100070BC3, 00010007219F, 000100072497, 000100072F8E, 000100071C23, 000100071AE7, 000100071C39, 000100071F59, 000100071BA2, 0001000725D7, 000100070C4C, 000100070291, 000100071D33, 0001000716F2, 000100070855, 000100071D50, 0001000717C0, 000100070442, 000100070623, 0001000721C4, 000100071488, 0001000706E0, 000100071252, 00010007214C, 000100070EA3, 000100070EBC, 000100071B49, 000100071698, 0001000711C1, 000100070444, 000100070C39, 000100071ADB, 000100071738, 00010007021E, 000100070230, 000100070F87, 000100071156, 000100070F27, 000100070B0D, 0001000718D9, 0001000702DE, 000100071E02, 0001000702D6, 0001000703A3, 00010007057D, 000100072448, 000100071E91, 0001000721C2, 00010007204E, 00010007077A, 000100071278, 000100070459, 0001000729EC, 000100071064, 00010007047D, 000100070B8E, 00010007190C, 000100070D09, 000100070F2C, 000100070000, 0001000705F3, 000100071F35, 000100071BAD, 0001000716C4, 0001000712B8, 0001000729D3, 0001000708C6, 0001000715DE, 000100071ABE, 000100071C08, 000100070725, 000100070C23, 0001000707E3, 000100071DAE, 000100071A3F, 0001000724CC, 000100072393, 000100071B96, 000100072830, 000100070CD6, 00010007188C, 0001000728B2, 000100070603, 000100070E94, 000100070643, 0001000714B6, 000100070F9E, 00010007069D, 000100070F88, 0001000712E4, 00010007234F, 000100070B8D, 0001000717EF, 000100071170, 000100071E55, 00010007194C, 000100070853, 000100071ED7, 0001000715B7, 0001000714E3, 000100071F67, 0001000721D4, 000100072D8A, 0001000710FC, 000100070CD5, 000100070C64, 000100070909, 000100070E4E, 000100071E27, 0001000717D9, 0001000714DF, 0001000716DE, 000100071100, 000100072226, 00010007163F, 0001000721FC, 000100070369, 000100071DDE, 000100070545, 000100070B0F, 0001000714ED, 000100071492, 0001000720E5, 00010007285E, 000100072739, 000100071B9C, 000100071203, 000100071B6E, 000100071BFB, 0001000720A0, 000100071B75, 000100072203, 000100070730, 00010007078E, 000100070F95, 000100070FD1, 000100072DDD, 000100070961, 000100070C7A, 000100070BEA, 000100071FB8, 000100072488, 000100071789, 000100072E1D, 000100072621, 0001000708A1, 000100071EF8, 0001000720BF, 000100071790, 000100071E9B, 000100071DCB, 000100071C4D, 0001000706EB, 000100070D30, 000100071161, 000100071BF1, 000100070942, 0001000701E2, 000100072148, 000100071267, 0001000709DB, 000100072023, 000100071E37, 00010007106A, 0001000703ED, 00010007091C, 0001000718EB, 000100071924, 0001000717DD, 000100072024, 000100071F6C, 0001000707B0, 000100073086, 000100071BBD, 000100071D4B, 000100071500, 000100071B7D, 000100070D7E, 000100071E1F, 000100072239, 000100072713, 000100070866, 000100070B79, 0001000722D3, 0001000708AA, 00010007290E, 00010007102C, 00010007208B, 000100070AE7, 000100070321, 0001000706F3, 000100072FA4, 000100071C5E, 0001000718C3, 0001000715FC, 000100070235, 000100071141, 000100070E49, 0001000728FE, 000100072922, 000100070703, 000100070B1E, 000100071B1F, 000100071802, 00010007110D, 000100070DBD, 000100070F4E, 0001000728EF, 0001000728BB, 000100071CED, 0001000704D0, 0001000719E7, 000100070C7D, 000100072079, 000100070AEE, 000100071D0C, 0001000709C4, 000100071C28, 0001000718B2, 000100071557, 000100070DAB, 000100071D6F, 0001000710FF, 000100072222, 00010007153B, 000100070C18, 00010007275B, 0001000720D0, 000100071C8A, 0001000715DD, 000100070CF4, 0001000721B7, 0001000714B3, 00010007042E, 000100071494, 000100071D62, 000100072013, 00010007264A, 000100071422, 000100071E33, 0001000703B2, 0001000717B9, 000100071A6A, 0001000722ED, 000100070938, 00010007274D, 0001000716AA, 00010007162D, 000100071CCE, 000100070AFB, 0001000727FA, 000100071739, 00010007180E, 0001000710BC, 0001000711FB, 000100071863, 000100072EAB, 0001000715F4, 0001000706F6, 0001000714B1, 00010007171E, 00010007148D, 0001000707BD, 0001000708CB, 000100070917, 000100072F40, 000100071E5E, 0001000726E6, 0001000701D5, 000100070B85, 000100070622, 000100071BFA, 000100070AA8, 0001000704AF, 00010007160E, 000100071F71, 000100070655, 000100070AC2, 00010007094D, 000100070372, 00010007101B, 000100071A3E, 00010007321C, 000100071110, 0001000728D9, 000100071DB1, 000100070B6C, 0001000716F6, 000100070F85, 000100070DC4, 000100070D48, 000100071C25, 000100072668, 000100070BF9, 000100070C9F, 000100072098, 000100071CDA, 000100071E70, 0001000724EB, 000100071179, 000100071953, 000100071668, 000100070357, 000100072DF2, 00010007220F, 000100071C0A, 000100070E72, 000100071126, 000100070D69, 000100071DD7, 000100072771, 000100070A98, 000100072339, 000100070300, 000100071B1B, 0001000720CD, 000100072634, 00010007171A, 00010007109E, 000100070F83, 0001000707A8, 000100071051, 0001000707FA, 00010007042A, 000100070B17, 000100071795, 000100071627, 000100071925, 000100071943, 000100071D67, 00010007035B, 000100071005, 0001000727B4, 00010007290B, 00010007159B, 0001000708B5, 000100071D17, 000100070D19, 000100070F45, 000100071C59, 00010007188E, 00010007218D, 000100073292, 000100071C18, 00010007092D, 0001000703A9, 00010007122C, 000100070EFB, 000100071153, 00010007103A, 000100070621, 000100071C88, 000100071F50, 000100070888, 0001000720F3, 000100071DF5, 000100072B8D, 0001000706D8, 000100070366, 00010007197B, 0001000716A3, 000100071584, 0001000715D5, 0001000705BE, 000100071F4E, 0001000702EC, 000100071F3A, 000100070921, 000100070E9F, 0001000718AD, 00010007241F, 000100071486, 000100070E74, 00010007150F, 0001000724C9, 00010007096C, 000100071C5F, 0001000703D6, 0001000723AF, 0001000721B6, 000100070BD2, 0001000708A9, 000100070BC2, 00010007212C, 000100071CF7, 00010007145A, 00010007277C, 00010007259A, 000100070D60, 000100070696, 00010007155B, 000100070F59, 000100072559, 000100070DCC, 000100070D90, 000100072063, 000100071454, 00010007069A, 0001000721E2, 000100071682, 0001000707CF, 000100071254, 0001000718BF, 000100071688, 00010007189C, 000100072869, 000100070661, 000100071FD0, 000100070C09, 000100071180, 0001000701C0, 00010007115B, 000100072918, 0001000719AB, 000100072DDB, 0001000715F7, 000100070D2D}')
--and st>0
ORDER BY device_id, timestamp_utc desc


CREATE INDEX on status.legacy_status (device_type, timestamp_utc DESC);

EXPLAIN ANALYZE
SELECT DISTINCT ON (device_id) device_id, host_rcpn, beacon_rcpn, device_type, day
FROM status.legacy_status_daily
WHERE day > NOW() - INTERVAL '1 day'
and device_type='INVERTER'
ORDER BY device_id, day desc

select count(device_id) from status.legacy_status_daily;

SELECT * FROM indexes_relation_size('status.legacy_status');
DROP INDEX status.legacy_status_device_type_timestamp_utc_idx

select distinct concat(host_rcpn, ',') from (
select host_rcpn, timestamp_utc from status.legacy_status 
 WHERE host_rcpn like '00010007%'
order by timestamp_utc desc limit 5000) as foo;

select * from boo

select * from boo order by st desc

EXPLAIN ANALYZE
select s.device_id, s.host_rcpn, s.beacon_rcpn, s.device_type, s.timestamp_utc, s.St
from (
    select DISTINCT ON (device_id) device_id, timestamp_utc,
    lag(timestamp_utc) over (order by timestamp_utc desc) as lag,
    lag(st) over (order by timestamp_utc) as stlag,
    st
    from status.legacy_status
    WHERE host_rcpn in ('00010007085D', '000100072EAB', '000100071575')
    order by device_id, timestamp_utc desc
) t
join status.legacy_status s ON t.device_id = s.device_id and t.lag = s.timestamp_utc 

select * from status.legacy_status where device_id ='000100034227' order by timestamp_utc desc limit 10;

 and timestamp_utc='2020-11-03 01:19:44'


ORDER BY device_id, array_position(array['00010007085D','000100072EAB', '000100071575'], device_id), timestamp_utc DESC



select DISTINCT ON (device_id) device_id, day
    from status.legacy_status_daily
    where day > now() - INTERVAL ''
    order by device_id, day desc





select * from site_energy limit 10;


SELECT timestamp_utc,
       timestamp_utc - LAG(timestamp_utc) OVER (
                                                ORDER BY timestamp_utc) AS diff
FROM status.legacy_status
WHERE timestamp_utc > now() - INTERVAL '1 day'
  AND device_id='000100001A0A'
ORDER BY timestamp_utc DESC


select * from status.legacy_status_daily where error > 1 limit 40

SELECT timestamp_utc,
       timestamp_utc - LAG(timestamp_utc) OVER (
                                                ORDER BY timestamp_utc) AS diff
FROM status.legacy_status
WHERE timestamp_utc > now() - INTERVAL '1 day'
  AND device_id='000100036033'
ORDER BY timestamp_utc DESC

-- delta row diff 00010007179D Jacob
WITH diff AS (
SELECT st, timestamp_utc,
       timestamp_utc - LAG(timestamp_utc) OVER (ORDER BY timestamp_utc) AS diff,
       LAG(st) OVER (ORDER BY timestamp_utc) AS stdiff
FROM status.legacy_status
WHERE timestamp_utc > now() - INTERVAL '22 day'
AND device_id like '000100039867'
ORDER BY timestamp_utc DESC
)
select *,  timestamp_utc - LAG(timestamp_utc) OVER (ORDER BY timestamp_utc) AS diff2 from diff where st<>stdiff

-----

select  time_bucket(INTERVAL '1 hour', timestamp_utc) AS hour, device_id
I, AVG(I), stddev(I), (AVG(I))/NULLIF(stddev(I),0),
         abs((max(P)-avg(P))/NULLIF(stddev(P),0)) as zscoreMaxP,
 I / (avg(I) over ()) as pct_of_mean,
  ( I - avg( I) over ()) / (stddev( I) over ()) as zscore
from status.legacy_status
WHERE timestamp_utc > now() - INTERVAL '22 day'
AND device_id like '000100039867'
GROUP BY hour, device_id, I
order by hour DESC



select timestamp_utc at time zone 'utc' at time zone 'pst',* from status.legacy_status ORDER BY timestamp_utc desc limit 10;

SELECT DISTINCT ON (device_id) device_id, host_rcpn, st, device_type, timestamp_utc,
LAG(timestamp_utc) OVER (ORDER BY timestamp_utc),
LAG(st, 26) OVER (ORDER BY timestamp_utc)

   FROM status.legacy_status
   WHERE timestamp_utc > now()-INTERVAL '1 day'
   and device_id='000100039867'
   order by device_id, timestamp_utc desc

   select * from status.legacy_status where device_id='000100039867' order by timestamp_utc desc

SELECT max(timestamp_utc) from status.legacy_status


select * from status.legacy_status where device_id = '000100036033' 
and timestamp_utc='2020-10-26 18:09:11'
ORDER BY timestamp_utc desc

WITH inverters AS
  (SELECT host_rcpn
   FROM status.legacy_status
   WHERE device_type='PVLINK'
     AND st=16
     AND timestamp_utc > now() - INTERVAL '5 day'
   GROUP BY host_rcpn),
     pvlinks AS
  ( SELECT DISTINCT ON (device_id) device_id,
                       host_rcpn,
                       st
   FROM status.legacy_status
   WHERE host_rcpn IN
       (SELECT host_rcpn
        FROM inverters)
     AND timestamp_utc>now()-INTERVAL '5 day'
   ORDER BY device_id,
            host_rcpn,
            st,
            timestamp_utc DESC)
SELECT *
FROM pvlinks;




EXPLAIN ANALYZE
\timing
SELECT device_id, St, timestamp_utc
FROM status.legacy_status
WHERE St in(2096, 2080) and device_id like '00010007%'
and timestamp_utc > now() - INTERVAL '3 days'
and device_id IN ('0001000702F5')
GROUP BY device_id, St
ORDER BY device_id, timestamp_utc


EXPLAIN ANALYZE
\timing
SELECT DISTINCT ON (device_id)
device_id, host_rcpn, beacon_rcpn, device_type, timestamp_utc, St
FROM status.legacy_status_state_change
WHERE device_id in ('000100122213','0001001204D2', '000100121088' )
ORDER BY device_id, timestamp_utc DESC;

\timing
SELECT DISTINCT ON (device_id)
device_id, St, timestamp_utc
FROM status.legacy_status
WHERE device_id in ('000100122213','0001001204D2', '000100121088', '000100071575' )
ORDER BY device_id, timestamp_utc DESC;

SELECT DISTINCT ON (device_id)
device_id, St, timestamp_utc
FROM status.legacy_status
WHERE device_id like '00010012%'
and timestamp_utc > now() - INTERVAL '3 day'
and St > 15 and St < 32
ORDER BY device_id, timestamp_utc DESC;

EXPLAIN ANALYZE
\timing
  SELECT DISTINCT ON (device_id)
  device_id, beacon_rcpn, host_rcpn, St, timestamp_utc
  FROM status.legacy_status
  WHERE beacon_rcpn in ('00010012258E' )
  AND timestamp_utc > now() - INTERVAL '3 day'
  ORDER BY device_id, timestamp_utc DESC

\timing
select count(beacon_rcpn) from status.legacy_status where timestamp_utc=now()-INTERVAL '3 day'

\timing
SELECT DISTINCT ON (device_id)
device_id, St, timestamp_utc
FROM status.legacy_status
WHERE host_rcpn in ('000100071742')
ORDER BY device_id, timestamp_utc DESC;




\timing
SELECT DISTINCT ON (device_id)
device_id, host_rcpn, beacon_rcpn, timestamp_utc, St, T, I, V
FROM status.legacy_status
WHERE device_id in
('183757675',
 '183757655',
 '183757671',
 '183757672',
 '183757674',
 '183757675',
 '183757676',
 '183757695',
 '183761186',
 '183761202',
 '183761203',
 '203769350',
 '213617732',
 '223232233',
 '208191940',
 '196555058',
 '193293733',
 '207132146',
 '202795274',
 '214258476',
 '225825332',
 '219868536',
 '215043413',
 '210721074',
 '203168928',
 '200002406',
 '222417102',
 '196427898',
 '208392389',
 '204521304',
 '185872063',
 '227175788',
 '217963967',
 '215111771',
 '184007618',
 '189308225',
 '197805433',
 '210476184',
 '223045538',
 '213061076',
 '212377063',
 '204317075')
ORDER BY device_id, timestamp_utc DESC;

SELECT DISTINCT ON (beacon_rcpn)
beacon_rcpn, host_rcpn, timestamp_utc, St, T, I, V
FROM status.legacy_status
WHERE beacon_rcpn in
('183756454')
ORDER BY beacon_rcpn, timestamp_utc DESC

select beacon_rcpn
from status.legacy_status
limit 10;



\timing
SELECT *
FROM status.legacy_status
WHERE beacon_rcpn='183756454'
ORDER BY timestamp_utc DESC
LIMIT 10;


drop index status.legacy_status_state_change_host_rcpn_timestamp_utc_idx6

EXPLAIN ANALYZE
SELECT DISTINCT ON (device_id)
device_id, beacon_rcpn, host_rcpn, timestamp_utc, St
FROM status.legacy_status
WHERE device_id in
('183757675', '183757655',
 '183757671',
 '183757672',
 '183757674',
 '183757675',
 '183757676',
 '183757695',
 '183761186',
 '183761202',
 '183761203',
 '203769350',
 '213617732',
 '223232233')
ORDER BY device_id, timestamp_utc DESC;

-- ALTER USER streamwriter WITH PASSWORD 'time-is-on-my-side';

\timing
SELECT DISTINCT ON (device_id)
device_id, device_type, timestamp_utc, to_hex(St)
FROM status.legacy_status
WHERE St between 28672 and 32769
and device_id = '000100035768'
and timestamp_utc>now()-INTERVAL '1 week'
ORDER BY device_id, timestamp_utc DESC


select sum(case when St between 28672 and 32769 then 1 else 0 end) as error,
       sum(1) as total_state
from status.legacy_status
where device_id = '000100035768'

  SELECT * FROM chunk_relation_size_pretty('_timescaledb_internal._materialized_hypertable_40');


SELECT add_drop_chunks_policy( 'status.legacy_status_daily', INTERVAL '30 days', cascade_to_materializations => TRUE);

SELECT * FROM timescaledb_information.policy_stats;


drop view status.legacy_status_daily cascade

ALTER VIEW status.legacy_status_daily SET (
   timescaledb.ignore_invalidation_older_than = '1 days'
);


SELECT *
FROM _timescaledb_config.bgw_policy_compress_chunks p
INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = p.hypertable_id);

SELECT drop_chunks(older_than => INTERVAL '7 days',  schema_name => 'status', table_name => 'legacy_status', cascade_to_materializations => FALSE, verbose => true);

explain analyze
select * 
from status.legacy_status_daily 
where error > 1 and device_type='INVERTER'
order by error desc

drop view status.temp cascade;

CREATE VIEW status.temp
   WITH (timescaledb.continuous)
   AS
SELECT 
         time_bucket(INTERVAL '1 day', timestamp_utc) AS day,
         device_id,
         device_type,
         beacon_rcpn,
         host_rcpn,
         sum(case when St between 28672 and 32769 then 1 else 0 end) as error,
         sum(1) as totalStates,
         avg(P) as avgP,
         max(P) as maxP,
         abs((max(P)-avg(P))/NULLIF(stddev(P),0)) as zscoreMaxP,
         abs((min(P)-avg(P))/NULLIF(stddev(P),0)) as zscoreMinP,
         --P / (avg(P) over ()) as pct_of_mean,
         --( P - avg( P) over ()) / (stddev(P) over ()) as zscore,
         stddev(P) as stddevP,         avg(T) as avgT,
         max(T) as maxT
         FROM status.legacy_status
         WHERE device_id ='000100039867'
      GROUP BY day, device_id, device_type, beacon_rcpn, host_rcpn, P;

      select *, 
       --avgP / (avg(avgP) over ()) as pct_of_mean
      ( maxP - avg(avgP) over ()) / (stddevP over ()) as zscore
      from status.temp

      select * from status.temp

  SELECT * FROM chunk_relation_size_pretty('public.site_energy');

  select now(), hypertable_name, chunk_name, compression_status, uncompressed_total_bytes, compressed_total_bytes from timescaledb_information.compressed_chunk_stats;

select * from public.site_energy limit 10

select *, now(), hypertable_name, chunk_name, compression_status, uncompressed_total_bytes, compressed_total_bytes from timescaledb_information.compressed_chunk_stats;


select  *, device_id, beacon_rcpn, host_rcpn from status.legacy_status_daily where device_id='00010003674C'
select  *, device_id, beacon_rcpn, host_rcpn from status.legacy_status where device_id='00010003674C' and date_trunc('day', timestamp_utc) = '2020-10-20'

drop index status.legacy_status_beacon_rcpn_timestamp_utc_idx


select grantee, table_catalog, privilege_type, table_schema, table_name 
from information_schema.table_privileges 
where grantee = 'readonly'
order by grantee, table_schema, table_name;

GRANT SELECT ON public.site_energy TO readonly;

--- AAA
      SELECT time_bucket(INTERVAL '5 min', timestamp_utc) as bucket, *
      from status.legacy_status
      where device_id='215111771'
      ORDER BY timestamp_utc desc
      Limit 10;



select * 
from status.legacy_status
where device_id in (42,48,43)
order by array_position(array[42,48,43], device_id);


select count(device_id) from status.legacy_status;


select last(device_id, timestamp_utc),  st from status.legacy_status where device_id='183756434' group by  st



select * from status.legacy_status where st > 32768 and device_id='183756434';

select * from status.legacy_status
where st in (45073,
49409,
50945,
45313,
50179,
46110,
45057,
49163,
45312,
49162,
45330,
46098,
45315,
45327,
49169,
53262,
47146,
53266,
47139,
57859,
53265,
45329)

limit 100;


CREATE VIEW status.legacy_status_daily
   WITH (timescaledb.continuous)
   AS
      SELECT 
         time_bucket(INTERVAL '1 day', timestamp_utc) AS day,
         device_id, 
         device_type, 
         host_rcpn,
         avg(I) as I,
         avg(V) as V,
         avg(P) as P, 
         last(st, timestamp_utc) as st,
         last(st, timestamp_utc) as st,
         FROM status.legacy_status
      GROUP BY day, device_id, device_type, host_rcpn;

\timing
WITH battery AS
  (SELECT count(device_id),
          device_id, host_rcpn
   FROM status.legacy_status_daily
   WHERE (device_type='BATTERY'
         AND P=0)
     AND DAY > now()-INTERVAL '5 days'
   GROUP BY host_rcpn,
            device_id),
     pvlink AS (
      SELECT count(device_id),
                device_id, host_rcpn
         FROM status.legacy_status_daily
         WHERE (device_type='PVLINK'
                AND st between x'7000'::int and x'7FFF'::int)
           AND DAY > now()-INTERVAL '5 days'
         GROUP BY host_rcpn,
                  device_id
     )



select * from battery b join pvlink p on b.host_rcpn=p.host_rcpn
select x'7210'::int

with pvlink_enabled as (
SELECT count(device_id), host_rcpn
         FROM status.legacy_status_daily
         WHERE (device_type='PVLINK'
                AND st=16)
           AND DAY > now()-INTERVAL '5 days'
         GROUP BY host_rcpn
),
pvlink_disabled as  (
SELECT count(device_id), host_rcpn
         FROM status.legacy_status_daily
         WHERE (device_type='PVLINK'
                AND st!=16)
           AND DAY > now()-INTERVAL '5 days'
         GROUP BY host_rcpn
)

select * from pvlink_enabled e join pvlink_disabled d on e.host_rcpn=d.host_rcpn
order by e.host_rcpn, d.host_rcpn



select count(device_id) from status.legacy_status_daily 


ALTER VIEW status.legacy_status_daily SET (
    timescaledb.materialized_only = true,
    timescaledb.refresh_lag = '12 hours',
    timescaledb.ignore_invalidation_older_than = '29 days'
);

SELECT drop_chunks(older_than => INTERVAL '30 days',  schema_name => 'status', table_name => 'legacy_status', cascade_to_materializations => FALSE, verbose => true);


REFRESH MATERIALIZED VIEW status.legacy_status_daily;

SELECT max(timestamp_utc) from status.legacy_status



      explain analyze 
      select * from status.legacy_status_daily where device_id='183757676'order by day desc limit 10;

        SELECT * FROM chunk_relation_size_pretty('status.legacy_status');




select * from status.legacy_status order by timestamp_utc desc limit 10;


select count(device_id) from status.legacy_status_daily

SELECT * FROM timescaledb_information.policy_stats;

select * from public.site_energy where location_id='b4e4cc55-959f-4103-a9c1-839d44cb688d' 
order by timestamp desc 
limit 10000;



select * from status.legacy_status where device_id='183781523' order by timestamp_utc desc;

CREATE VIEW status.battery_SOC
   WITH (timescaledb.continuous, timescaledb.ignore_invalidation_older_than = '2 days')
   AS
SELECT 
         time_bucket(INTERVAL '1 day', timestamp_utc) AS day,
         device_id,
         device_type,
         beacon_rcpn,
         host_rcpn,
         avg(O5) as avgSoC,
         min(O5) as minSoC,
         max(O5) as macSoC,
         sum(O5) as sumSoC
         FROM status.legacy_status
         WHERE device_type = 'BATTERY'
      GROUP BY day, device_id, device_type, beacon_rcpn, host_rcpn
      


DROP FUNCTION IF EXISTS status.device_map_daily;
CREATE OR REPLACE FUNCTION status.device_map_daily (inverters text[])
RETURNS TABLE (
  rcpn text,
  today timestamp,
  yieldToday integer,
  yesterday timestamp,
  yesterdayYield integer,
  avg_current numeric
) AS $$
BEGIN
  RETURN QUERY

    SELECT DISTINCT ON (device_id) device_id, day, last_st,
    lag(day) over (order by day), 
    lag(last_st) over (order by day),
    avg_I
    FROM status.legacy_status_daily
    WHERE device_id = ANY (inverters)
    --AND day > NOW() - INTERVAL '12 hour'
    ORDER BY device_id, day desc;
END;
$$ LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS status.device_map2;
CREATE OR REPLACE FUNCTION status.device_map2 (inverters text[])
RETURNS SETOF record AS $$
BEGIN

SELECT s.device_id, s.host_rcpn, s.beacon_rcpn, s.timestamp_utc, s.st, s.device_type
    FROM status.device_shadow as s
    WHERE s.host_rcpn = ANY (inverters)
    ORDER BY s.device_id, s.timestamp_utc desc;

END;
$$ LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS status.device_map;
CREATE OR REPLACE FUNCTION status.device_map (inverters text[])
RETURNS TABLE (
  device_id text,
  host_rcpn text,
  beacon_rcpn text,
  timestamp_utc timestamp,
  st integer,
  device_type text
) AS $$
BEGIN
  RETURN QUERY

   
SELECT s.device_id, s.host_rcpn, s.beacon_rcpn, s.timestamp_utc, s.st, s.device_type
    FROM status.device_shadow as s
    WHERE s.host_rcpn = ANY (inverters) and s.host_rcpn!=''
    ORDER BY s.device_id, s.timestamp_utc desc;

END;
$$ LANGUAGE plpgsql;


SELECT DISTINCT ON (device_id) device_id, timestamp_utc, st, device_type
    FROM status.device_shadow
    WHERE device_id = ANY (inverters)
    ORDER BY device_id, timestamp_utc desc;





DROP FUNCTION IF EXISTS status.device_map_by_inverter;
CREATE OR REPLACE FUNCTION status.device_map_by_inverter (inverters text[])
RETURNS TABLE (
  rcpn text,
  today timestamp,
  last_state integer
) AS $$
BEGIN
  RETURN QUERY

    SELECT DISTINCT ON (device_id) device_id, timestamp_utc, st
    FROM status.legacy_status
    WHERE host_rcpn = ANY (inverters)
    --AND timestamp_utc > NOW() - INTERVAL '6 hour'
    ORDER BY device_id, timestamp_utc desc;
END;
$$ LANGUAGE plpgsql;





CREATE VIEW status.legacy_status_daily2
   WITH (timescaledb.continuous,
         timescaledb.ignore_invalidation_older_than = '29 days')
   AS
      SELECT 
         time_bucket(INTERVAL '1 day', timestamp_utc) AS day,
         device_id,
         device_type,
         beacon_rcpn,
         host_rcpn,
         sum(case when (St between 28672 and 32769) then 1 else 0 end) as error,
         sum(1) as totalStates,
         avg(P) as avgP,
         max(P) as maxP,
         avg(T) as avgT,
         max(T) as maxT
         FROM status.legacy_status
      GROUP BY day, device_id, device_type, beacon_rcpn, host_rcpn

REFRESH MATERIALIZED VIEW status.legacy_status_daily;

