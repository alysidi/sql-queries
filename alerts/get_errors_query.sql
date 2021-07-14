WITH state_changes AS 
      ( 
                 SELECT     p.*
                 FROM (VALUES ('0001000823F0','00010007110F'),('000100083518', '000100072FA2') ) AS parent(device_id, host_rcpn) -- pass in tuples from chunks
                 --FROM (VALUES {} ) AS parent(device_id, host_rcpn) -- pass in tuples from chunks
                 CROSS JOIN lateral 
                            ( 
                                     SELECT   device_id, 
                                              host_rcpn, 
                                              timestamp_utc, 
                                              st
                                     FROM     status.legacy_status_state_change lssc 
                                     WHERE    device_id = parent.device_id --and host_rcpn = parent.host_rcpn
                                     AND      timestamp_utc >= NOW() - INTERVAL '12 hours'
                    
                             ) p 
                 
        ),

        bad_states AS (
                        select device_id, to_hex(st) as state, count(st) as state_count
                        from state_changes
                        where st BETWEEN x'0000'::int AND x'FFFF'::int 
                        group by device_id, st
          )


      SELECT * FROM  (
        SELECT   device_id,
                 host_rcpn, 
                 timestamp_utc as last_heard,
                 to_hex(st) as last_state
          
        FROM     status.device_shadow 
        WHERE device_id in (select distinct device_id from bad_states)
        GROUP BY device_id, host_rcpn, st, last_heard
      ) button
      
       CROSS JOIN LATERAL (

                      select json_build_object('total', count(t.*), 'data', json_agg(to_json(t))) as num
                      from (
                       select state, state_count
                        from bad_states
                        where device_id=button.device_id
                      ) t
                  ) x
       CROSS JOIN LATERAL (

                      select json_build_object( 'history', json_agg(to_json(t))) as history
                      from (
                        select st as state,
                        lag(st) OVER (partition BY device_id ORDER BY timestamp_utc DESC) AS next_state,
                        timestamp_utc,
                        lag(timestamp_utc) OVER (partition BY device_id ORDER BY timestamp_utc DESC) AS next_state_timestamp
                        from status.legacy_status_state_change
                        where device_id=button.device_id
                        and timestamp_utc > now() - INTERVAL '30 days'
                        order by timestamp_utc desc
                        LIMIT 200
                      ) t
                  ) z
                