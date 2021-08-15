SELECT device_id,
       device_type,
       TIMESTAMP_utc,
       to_hex(st) as st
FROM   status.legacy_status_state_change --_timescaledb_internal._hyper_185_4737_chunk
WHERE device_id = '000100035C98' 
--WHERE device_id='000100070B70'
-- WHERE st between x'7000'::int and x'7FFF'::int
--and timestamp_utc between '2021-06-24' and '2021-07-08'
--and timestamp_utc >= now() - INTERVAL '1 days'
--and st between 0 and x'FFFF'::int
order by timestamp_utc desc
limit 100;


SELECT device_id,
       max(TIMESTAMP_utc),
       lag(TIMESTAMP_utc) OVER (order by timestamp_utc desc) as lag
FROM   status.legacy_status_state_change --_timescaledb_internal._hyper_185_4737_chunk
WHERE device_id = '000100035C98' 
GROUP BY device_id, timestamp_utc
order by timestamp_utc desc
limit 2




  



show all
select * from status.legacy_status_state_change where timestamp_utc between '2021-07-07' and '2021-07-08' limit 11;

\COPY (select * from status.legacy_status_state_change where timestamp_utc between '2021-06-17' and '2021-06-24') TO '/tmp/2021-07-08-15.csv' DELIMITER ',';


timescaledb-parallel-copy --connection "postgres://tsdbadmin:pwdeaj1f3rv5ntvc@localhost:12949/defaultdb" --schema status --table legacy_status_state_change_new --file _timescaledb.internal._hyper_185_4737_chunk.csv --workers 3 --reporting-period 30s

  select show_chunks('status.legacy_status_state_change_good')
  -- select compress_chunk('_timescaledb_internal._hyper_265_4761_chunk');
  -- select decompress_chunk('_timescaledb_internal._hyper_265_4761_chunk')


select state_text, * from status.legacy_status_state_change l
left join status.rcp_state r ON r.state_code = (l.st & x'FFF0'::int)
where device_id='0001000311EA'
and timestamp_utc between '2021-04-01 00:00:00+00' and  '2021-04-08 00:00:00+00'
order by timestamp_utc desc
limit 111;

select state_text, * from status.legacy_status_state_change l
left join status.rcp_state r ON r.state_code = (l.st & x'FFF0'::int)
where device_id='0001000311EA'
and timestamp_utc between '2021-04-09 00:00:00+00' and  '2021-04-11 00:00:00+00'
order by timestamp_utc desc
limit 111;

select state_text, * from status.legacy_status_state_change l
left join status.rcp_state r ON r.state_code = (l.st & x'FFF0'::int)
where device_id='000100035C98'
and timestamp_utc between '2021-04-01 00:00:00+00' and  '2021-04-08 00:00:00+00'
order by timestamp_utc desc
limit 111;

select state_text, * from status.legacy_status_state_change l
left join status.rcp_state r ON r.state_code = (l.st & x'FFF0'::int)
where device_id='000100035C98'
and timestamp_utc between '2021-07-01 00:00:00+00' and  '2021-07-07 00:00:00+00'
order by timestamp_utc desc
limit 111;
 ***********************************

SELECT device_id,
       device_type,
       TIMESTAMP_utc,
       to_hex(st) as st
FROM   _timescaledb_internal._hyper_185_4737_chunk
WHERE  device_id='000100070B70'
--and st between x'7000'::int and x'7FFF'::int
--and timestamp_utc between '2021-07-07' and '2021-07-08'
order by timestamp_utc desc
limit 100;

select alter_job(1256, next_start=>now())

\d+ status.legacy_status_state_change_old

\d+ status.legacy_status
\d+ status.legacy_status_state_change

select max(timestamp_utc) from status.legacy_status_state_change_good

 select drop_chunks('status.legacy_status_state_change_old',  newer_than => '2021-07-01')

/********************* RENAME *************************************/
--ALTER TABLE status.legacy_status_state_change RENAME TO legacy_status_state_change_old;
--ALTER TABLE status.legacy_status_state_change_good RENAME TO legacy_status_state_change;

--ALTER TABLE status.legacy_status RENAME TO legacy_status_old;
--ALTER TABLE status.legacy_status_good RENAME TO legacy_status;
/********************* RENAME *************************************/

select * from status.legacy_status_state_change ORDER BY timestamp_utc DESC limit 10;
select * from status.legacy_status ORDER BY timestamp_utc DESC limit 10;

/**********************  INSERT ***********************/
insert into status.legacy_status_state_change (select * from status.legacy_status_state_change_old where timestamp_utc >= '2021-06-24 00:00:00' and timestamp_utc <'2021-07-01 00:00:00') ON CONFLICT DO NOTHING;
/**********************  INSERT ***********************/

select device_id, count(*) from status.legacy_status_state_change_old
where device_id='000100036FBB' and timestamp_utc between '2021-04-01' and '2021-04-02'
GROUP BY device_id
limit 11

select r.state_text, * from status.legacy_status_state_change_old l 
left join status.rcp_state r ON r.state_code = (l.st & x'FFF0'::int)
where device_id='000100036FBB' and timestamp_utc between '2021-04-01' and '2021-04-08'
order by timestamp_utc desc

select * from status.legacy_status where device_id='000100082334' ORDER BY timestamp_utc desc limit 11;
select * from status.legacy_status_state_change where device_id='000100082334' ORDER BY timestamp_utc desc limit 11;


_hyper_185_4757_chunk



ALTER INDEX status.legacy_status_state_change_new_timestamp_utc_idx RENAME TO legacy_status_state_change_good_timestamp_utc_idx

select now()
CREATE TABLE status.legacy_status_state_change_new AS SELECT * FROM status.legacy_status_state_change where timestamp_utc > '2021-07-08 15:20';
a
select count(*) from status.legacy_status_state_change_new where timestamp_utc between '2021-07-01' and '2021-07-08'


VACUUM FULL status.device_shadow


SELECT format('%I.%I', c.schema_name, c.table_name) 
FROM _timescaledb_catalog.hypertable h inner join _timescaledb_catalog.hypertable c ON h.compressed_hypertable_id = c.id 
WHERE h.table_name = 'legacy_status' and h.schema_name = 'status';

CREATE UNIQUE INDEX on status.legacy_status_state_change_good (device_id, timestamp_utc DESC)

SELECT remove_compression_policy('status.legacy_status_state_change_good');
ALTER TABLE status.legacy_status_state_change_good SET (timescaledb.compress = FALSE);
CREATE UNIQUE INDEX on status.legacy_status_state_change_good (device_id, timestamp_utc DESC);


CALL migrate_data('status.legacy_status_state_change', 'status.legacy_status_state_change_new', older_than => INTERVAL '6 months');



select (*) from status.legacy_status_state_change where timestamp_utc between '2020-12-17 00:00:00+00' and '2020-12-24 00:00:00+00');

select count(*) from status.legacy_status_state_change where timestamp_utc between '2020-12-17 00:00:00+00' and '2020-12-24 00:00:00+00');


  (select * from show_chunks('status.legacy_status_state_change', older_than => INTERVAL '1 day'));

select * from show_chunk(chunk regclass)


\d+ _timescaledb_internal._hyper_258_4752_chunk

delete from status.legacy_status_state_change_new where device_id='0001000007CD' and timestamp_utc='2021-07-01 16:24:46'

select * from _timescaledb_internal._hyper_185_3881_chunk

ALTER TABLE  status.legacy_status_state_change_new
  SET (timescaledb.compress,
       timescaledb.compress_orderby = 'timestamp_utc DESC, st',
       timescaledb.compress_segmentby = 'host_rcpn, device_id, beacon_rcpn, device_type');

SELECT add_compression_policy('status.legacy_status_state_change_new', INTERVAL '7 day');

select count(*) from status.legacy_status_state_change where
timestamp_utc between '2020-12-17 00:00:00+00' and '2020-12-24 00:00:00+00'

select * from status.device_shadow where device_id='00010000001F'

-- device, state, timestamp index

select timestamp_utc, 
st, to_hex(st), state_text from status.legacy_status_state_change l 
LEFT JOIN status.rcp_state r ON l.st & x'FFF0'::int = r.state_code
where device_id='000100035AE4' 
order by timestamp_utc desc limit 50;